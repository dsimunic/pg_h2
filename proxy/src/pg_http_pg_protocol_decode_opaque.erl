%% Mostly from the pgsql_protocol module in https://github.com/semiocast/pgsql
-module(pg_http_pg_protocol_decode_opaque).

-include("pg_http_pg_protocol_opaque.hrl").

-export([decode_message/2]).

%%====================================================================
%% Constants
%%====================================================================
-define(PROTOCOL_VERSION_MAJOR, <<3:16/integer>>).
-define(PROTOCOL_VERSION_MINOR, <<0:16/integer>>).

-define(POSTGRESQL_GD_EPOCH, 730485). % ?_value(calendar:date_to_gregorian_days({2000,1,1}))).
-define(POSTGRESQL_GS_EPOCH, 63113904000). % ?_value(calendar:datetime_to_gregorian_seconds({{2000,1,1}, {0,0,0}}))).

-define(POSTGRES_EPOC_JDATE, 2451545).
-define(POSTGRES_EPOC_USECS, 946684800000000).

-define(MINS_PER_HOUR, 60).
-define(SECS_PER_MINUTE, 60).

-define(SECS_PER_DAY, 86400.0).

-define(USECS_PER_DAY, 86400000000).
-define(USECS_PER_HOUR, 3600000000).
-define(USECS_PER_MINUTE, 60000000).
-define(USECS_PER_SEC, 1000000).

%%====================================================================
%% Public API
%%====================================================================

%%--------------------------------------------------------------------
%% @doc Decode a message.
%%
-spec decode_message(byte(), iodata())
                     -> {ok, pgsql_backend_message()} | {error, any()}.
decode_message($R,[_Head, Payload])            -> decode_authentication_message(Payload);
decode_message($K,[_Head, Payload])            -> decode_backend_key_data_message(Payload);

decode_message($2,_Payload)                    -> {ok, #bind_complete{}};
decode_message($3,[_Head, Payload])            -> decode_close_complete_message(Payload);
decode_message($C,[_Head, Payload])            -> decode_command_complete_message(Payload);

decode_message($d,[_Head, Payload])            -> decode_copy_data_message(Payload);
decode_message($c,[_Head, Payload])            -> decode_copy_done_message(Payload);
decode_message($G,[_Head, Payload])            -> decode_copy_in_response_message(Payload);
decode_message($H,[_Head, Payload])            -> decode_copy_out_response_message(Payload);
decode_message($W,[_Head, Payload])            -> decode_copy_both_response_message(Payload);
decode_message($D,Payload)                     -> {ok, #data_row{payload=Payload}};
decode_message($I,[_Head, Payload])            -> decode_empty_query_response_message(Payload);
decode_message($E,[_Head, Payload])            -> decode_error_response_message(Payload);
decode_message($V,[_Head, Payload])            -> decode_function_call_response_message(Payload);
decode_message($n,[_Head, Payload])            -> decode_no_data_message(Payload);
decode_message($N,[_Head, Payload])            -> decode_notice_response_message(Payload);
decode_message($A,[_Head, Payload])            -> decode_notification_response_message(Payload);
decode_message($t,[_Head, Payload])            -> decode_parameter_description_message(Payload);
decode_message($S,[_Head, Payload])            -> decode_parameter_status_message(Payload);
decode_message($1,[_Head, Payload])            -> decode_parse_complete_message(Payload);
decode_message($s,[_Head, Payload])            -> decode_portal_suspended_message(Payload);
decode_message($Z,[_Head, Payload])            -> decode_ready_for_query_message(Payload);
decode_message($T,Payload)                     -> {ok, #row_description{payload = Payload}};
decode_message(Code, _) ->
    {error, {unknown_message_type, Code}}.

decode_authentication_message(<<0:32/integer>>) ->
    {ok, #authentication_ok{}};
decode_authentication_message(<<2:32/integer>>) ->
    {ok, #authentication_kerberos_v5{}};
decode_authentication_message(<<3:32/integer>>) ->
    {ok, #authentication_cleartext_password{}};
decode_authentication_message(<<5:32/integer, Salt:4/binary>>) ->
    {ok, #authentication_md5_password{salt = Salt}};
decode_authentication_message(<<6:32/integer>>) ->
    {ok, #authentication_scm_credential{}};
decode_authentication_message(<<7:32/integer>>) ->
    {ok, #authentication_gss{}};
decode_authentication_message(<<9:32/integer>>) ->
    {ok, #authentication_sspi{}};
decode_authentication_message(<<8:32/integer, Rest/binary>>) ->
    {ok, #authentication_gss_continue{data = Rest}};
decode_authentication_message(Payload) ->
    {error, {unknown_message, authentication, Payload}}.

decode_backend_key_data_message(<<ProcID:32/integer, Secret:32/integer>>) ->
    {ok, #backend_key_data{procid = ProcID, secret = Secret}};
decode_backend_key_data_message(Payload) ->
    {error, {unknown_message, backend_key_data, Payload}}.


decode_close_complete_message([$3]) -> {ok, #close_complete{}};
decode_close_complete_message([$3,Payload]) ->
    {error, {unknown_message, close_complete, Payload}}.

decode_command_complete_message(Payload) ->
    case decode_string(Payload) of
        {ok, String, <<>>} -> {ok, #command_complete{command_tag = String}};
        _ -> {error, {unknown_message, command_complete, Payload}}
    end.

decode_copy_data_message(Payload) -> {ok, #copy_data{data = Payload}}.

decode_copy_done_message(<<>>) -> {ok, #copy_done{}};
decode_copy_done_message(Payload) ->
    {error, {unknown_message, copy_done, Payload}}.

decode_copy_in_response_message(Payload) ->
    case decode_copy_response_message(Payload) of
        {ok, {OverallFormat, N, ColumnFormats}} ->
            {ok, #copy_in_response{format=OverallFormat,
                                   columns=N,
                                   column_formats=ColumnFormats}};
        {error, _} ->
            {error, {unknow_message, copy_in_response, Payload}}
    end.

decode_copy_out_response_message(Payload) ->
    case decode_copy_response_message(Payload) of
        {ok, {OverallFormat, N, ColumnFormats}} ->
            {ok, #copy_out_response{format=OverallFormat,
                                    columns=N,
                                    column_formats=ColumnFormats}};
        {error, _} ->
            {error, {unknow_message, copy_out_response, Payload}}
    end.

decode_copy_both_response_message(Payload) ->
    case decode_copy_response_message(Payload) of
        {ok, {OverallFormat, N, ColumnFormats}} ->
            {ok, #copy_both_response{format=OverallFormat,
                                     columns=N,
                                     column_formats=ColumnFormats}};
        {error, _} ->
            {error, {unknow_message, copy_both_response, Payload}}
    end.

decode_empty_query_response_message(<<>>) -> {ok, #empty_query_response{}};
decode_empty_query_response_message(Payload) ->
    {error, {unknown_message, empty_query_response, Payload}}.

decode_error_response_message(Payload) ->
    case decode_error_and_notice_message_fields(Payload) of
        {ok, Fields} -> {ok, #error_response{fields = Fields}};
        {error, _} -> {error, {unknown_message, error_response, Payload}}
    end.

decode_function_call_response_message(<<-1:32/signed-integer>>) ->
    {ok, #function_call_response{value = null}};
decode_function_call_response_message(<<Len:32/integer, Value:Len/binary>>) ->
    {ok, #function_call_response{value = Value}};
decode_function_call_response_message(Payload) ->
    {error, {unknown_message, function_call_response, Payload}}.

decode_no_data_message(<<>>) -> {ok, #no_data{}};
decode_no_data_message(Payload) ->
    {error, {unknown_message, no_data, Payload}}.

decode_notice_response_message(Payload) ->
    case decode_error_and_notice_message_fields(Payload) of
        {ok, Fields} -> {ok, #notice_response{fields = Fields}};
        {error, _} -> {error, {unknown_message, notice_response, Payload}}
    end.

decode_notification_response_message(<<ProcID:32/integer, Rest0/binary>> = Payload) ->
    case decode_string(Rest0) of
        {ok, Channel, Rest1} ->
            case decode_string(Rest1) of
                {ok, PayloadStr, <<>>} ->
                    {ok, #notification_response{procid=ProcID,
                                                channel=Channel,
                                                payload=PayloadStr}};
                {error, _} ->
                    {error, {unknown_message, notification_response, Payload}}
            end;
        {error, _} ->
            {error, {unknown_message, notification_response, Payload}}
    end;
decode_notification_response_message(Payload) ->
    {error, {unknown_message, notification_response, Payload}}.

decode_parameter_description_message(<<Count:16/integer, Rest/binary>> = Payload) ->
    ParameterDataTypes = decode_parameter_data_types(Rest),
    if
        Count =:= length(ParameterDataTypes) ->
            {ok, #parameter_description{count=Count,
                                        data_types=ParameterDataTypes}};
        true ->
            {error, {unknown_message, parameter_description, Payload}}
    end;
decode_parameter_description_message(Payload) ->
    {error, {unknown_message, parameter_description, Payload}}.

decode_parameter_status_message(Payload) ->
    case decode_string(Payload) of
        {ok, Name, Rest0} ->
            case decode_string(Rest0) of
                {ok, Value, <<>>} ->
                    {ok, #parameter_status{name=Name,
                                           value=Value}};
                {error, _} ->
                    {error, {unknown_message, parameter_status, Payload}}
            end;
        {error, _} ->
            {error, {unknown_message, parameter_status, Payload}}
    end.

decode_parse_complete_message(<<>>) -> {ok, #parse_complete{}};
decode_parse_complete_message(Payload) ->
    {error, {unknown_message, parse_complete, Payload}}.

decode_portal_suspended_message(<<>>) -> {ok, #portal_suspended{}};
decode_portal_suspended_message(Payload) ->
    {error, {unknown_message, portal_suspended, Payload}}.

decode_ready_for_query_message(<<$I>>) -> {ok, #ready_for_query{transaction_status=idle}};
decode_ready_for_query_message(<<$T>>) -> {ok, #ready_for_query{transaction_status=transaction}};
decode_ready_for_query_message(<<$E>>) -> {ok, #ready_for_query{transaction_status=error}};
decode_ready_for_query_message(Payload) ->
    {error, {unknown_message, ready_for_query, Payload}}.

%%% Helper functions.

decode_copy_response_message(<<Format:8/integer, N:16/integer, Rest/binary>>) when Format =:= 0
                                                                                   orelse Format =:= 1 ->
    {ok, OverallFormat} = decode_format_code(Format),
    if
        byte_size(Rest) =:= N * 2 ->
            case decode_format_codes(Rest) of
                {ok, ColumnFormats} ->
                    {ok, {OverallFormat, N, ColumnFormats}};
                {error, _} -> {error, column_formats}
            end;
        true ->
            {error, column_formats_size}
    end;
decode_copy_response_message(Payload) ->
    {error, {unknown_message, copy_response, Payload}}.

decode_error_and_notice_message_fields(Binary) ->
    decode_error_and_notice_message_fields0(Binary, []).

decode_error_and_notice_message_fields0(<<0>>, Acc) ->
    {ok, maps:from_list(Acc)};

decode_error_and_notice_message_fields0(<<FieldType, Rest0/binary>>, Acc) ->
    case decode_string(Rest0) of
        {ok, FieldString, Rest1} ->
            FieldTypeSym = decode_error_and_mention_field_type(FieldType),
            Field = {FieldTypeSym, FieldString},
            NewAcc = [Field | Acc],
            decode_error_and_notice_message_fields0(Rest1, NewAcc);
        {error, _} = Error -> Error
    end;
decode_error_and_notice_message_fields0(Bin, _Acc) -> {error, {badarg, Bin}}.

-spec decode_error_and_mention_field_type(byte()) -> pgsql_error:pgsql_error_and_mention_field_type().
decode_error_and_mention_field_type($S) -> severity;
decode_error_and_mention_field_type($C) -> code;
decode_error_and_mention_field_type($M) -> message;
decode_error_and_mention_field_type($D) -> detail;
decode_error_and_mention_field_type($H) -> hint;
decode_error_and_mention_field_type($P) -> position;
decode_error_and_mention_field_type($p) -> internal_position;
decode_error_and_mention_field_type($q) -> internal_query;
decode_error_and_mention_field_type($W) -> where;
decode_error_and_mention_field_type($s) -> schema;
decode_error_and_mention_field_type($t) -> table;
decode_error_and_mention_field_type($c) -> column;
decode_error_and_mention_field_type($d) -> data_type;
decode_error_and_mention_field_type($n) -> constraint;
decode_error_and_mention_field_type($F) -> file;
decode_error_and_mention_field_type($L) -> line;
decode_error_and_mention_field_type($R) -> routine;
decode_error_and_mention_field_type(Other) -> {unknown, Other}.

decode_parameter_data_types(Binary) ->
    decode_parameter_data_types0(Binary, []).

decode_parameter_data_types0(<<>>, Acc) -> lists:reverse(Acc);
decode_parameter_data_types0(<<Oid:32/integer, Tail/binary>>, Acc) ->
    decode_parameter_data_types0(Tail, [Oid | Acc]).

-spec decode_format_code(integer()) -> {ok, pgsql_format()} | {error, any()}.
decode_format_code(0) -> {ok, text};
decode_format_code(1) -> {ok, binary};
decode_format_code(_Other) -> {error, unknown_format_code}.

-spec decode_format_codes(binary()) -> {ok, [pgsql_format()]} | {error, any()}.
decode_format_codes(Binary) ->
    decode_format_codes0(Binary, []).

decode_format_codes0(<<FormatCode:16/integer, Tail/binary>>, Acc) ->
    case decode_format_code(FormatCode) of
        {ok, Format} ->
            decode_format_codes0(Tail, [Format | Acc]);
        {error, _} = Error -> Error
    end;
decode_format_codes0(<<>>, Acc) -> {ok, lists:reverse(Acc)}.

-spec decode_string(binary()) -> {ok, binary(), binary()} | {error, not_null_terminated}.
decode_string(Binary) ->
    case binary:match(Binary, <<0>>) of
        nomatch -> {error, not_null_terminated};
        {Position, 1} ->
            {String, <<0, Rest/binary>>} = split_binary(Binary, Position),
            {ok, String, Rest}
    end.
