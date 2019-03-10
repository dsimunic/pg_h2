%% Mostly from the pgsql_protocol module in https://github.com/semiocast/pgsql
-module(pg_http_pg_protocol_encode).

-include("pg_http_pg_protocol.hrl").

-export([startup_message/1,
         ssl_request_message/0,
         password_message/1,
         query_message/1,
         parse_message/3,
         bind_message/6,
         describe_message/2,
         execute_message/2,
         sync_message/0,
         flush_message/0,
         cancel_message/2,
         copy_data_message/1,
         copy_done/0,
         copy_fail/1,

         bind_requires_statement_description/1]).

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
%% @doc Encode the startup message.
%%
-spec startup_message([{iodata(), iodata()}]) -> iolist().
startup_message(Parameters) ->
    EncodedParams = [[Key, 0, Value, 0] || {Key, Value} <- Parameters],
    Packet = [?PROTOCOL_VERSION_MAJOR, ?PROTOCOL_VERSION_MINOR, EncodedParams, 0],
    Size = iolist_size(Packet) + 4,
    [<<Size:32/integer>>, Packet].

%%--------------------------------------------------------------------
%% @doc Encode the ssl request message.
%%
-spec ssl_request_message() -> binary().
ssl_request_message() ->
    <<8:32/integer, 1234:16/integer, 5679:16/integer>>.

%%--------------------------------------------------------------------
%% @doc Encode a password.
%%
-spec password_message(iodata()) -> iolist().
password_message(Password) ->
    string_message($p, Password).

%%--------------------------------------------------------------------
%% @doc Encode a query.
%%
-spec query_message(iodata()) -> iolist().
query_message(Query) ->
    string_message($Q, Query).

%%--------------------------------------------------------------------
%% @doc Encode a data segment of a COPY operation
%%
-spec copy_data_message(iodata()) -> iolist().
copy_data_message(Message) ->
    MessageLen = iolist_size(Message) + 4,
    [<<$d, MessageLen:32/integer>>, Message].

%%--------------------------------------------------------------------
%% @doc Encode the end of a COPY operation
%%
-spec copy_done() -> binary().
copy_done() ->
    <<$c, 4:32/integer>>.

%%--------------------------------------------------------------------
%% @doc Encode the cancellation of a COPY operation with the given
%%      failure message
%%
-spec copy_fail(iodata()) -> iolist().
copy_fail(ErrorMessage) ->
    string_message($f, ErrorMessage).

%%--------------------------------------------------------------------
%% @doc Encode a parse message.
%%
-spec parse_message(iodata(), iodata(), [oid()]) -> iolist().
parse_message(PreparedStatementName, Query, DataTypes) ->
    DataTypesBin = [<<DataTypeOid:32/integer>> || DataTypeOid <- DataTypes],
    DataTypesCount = length(DataTypes),
    Packet = [PreparedStatementName, <<0>>, Query, <<0>>,
              <<DataTypesCount:16/integer>>, DataTypesBin],
    PacketLen = iolist_size(Packet) + 4,
    [<<$P, PacketLen:32/integer>>, Packet].

%%--------------------------------------------------------------------
%% @doc Encode a bind message.
%%
-spec bind_message(iodata(), iodata(), [any()], [oid()], atom(), boolean()) -> iolist().
bind_message(PortalName, StatementName, Parameters, ParametersDataTypes, OIDMap, IntegerDateTimes) ->
    ParametersCount = length(Parameters),
    ParametersCountBin = <<ParametersCount:16/integer>>,
    ParametersWithTypes = case ParametersDataTypes of
                              [] -> [{Parameter, undefined} || Parameter <- Parameters];
                              _ -> lists:zip(Parameters, ParametersDataTypes)
                          end,
    ParametersValues = [parameter(Parameter, Type, OIDMap, IntegerDateTimes)
                        || {Parameter, Type} <- ParametersWithTypes],
    ParametersFormatsBin = [ParametersCountBin | [<<1:16/integer>> || _ <- ParametersValues]],
    Results = <<1:16/integer, 1:16/integer>>,   % We want all results in binary format.
    Packet = [PortalName, 0, StatementName, 0, ParametersFormatsBin,
              ParametersCountBin, ParametersValues, Results],
    PacketLen = iolist_size(Packet) + 4,
    [<<$B, PacketLen:32/integer>> | Packet].

numeric('NaN', _, _) ->
    <<0:16/unsigned, 0:16, 16#C000:16/unsigned, 0:16/unsigned>>;
numeric(Float, _Weight, Scale) ->
    Sign = case Float > 0 of
               true -> 16#0000;
               false -> 16#4000
           end,
    IntegerPart = trunc(Float),
    DecimalPart = trunc((Float - IntegerPart) * math:pow(10, Scale)),

    Weight = length(integer_to_list(IntegerPart)),
    Scale1 = length(integer_to_list(DecimalPart)),

    Digits = [I - 48 || I <- integer_to_list(IntegerPart)] ++
        [I - 48 || I <- integer_to_list(DecimalPart)],
    Bin = [<<I:16/unsigned-integer>> || I <- Digits],
    Len = length(Digits),
    [<<Len:16/unsigned, Weight:16/signed, Sign:16/unsigned, Scale1:16/unsigned>> | Bin].

%%--------------------------------------------------------------------
%% @doc Encode a parameter.
%% All parameters are currently encoded in text format except binaries that are
%% encoded as binaries.
%%
-spec parameter(any(), oid() | undefined, atom(), boolean()) -> iodata().
parameter(null, _Type, _OIDMap, _IntegerDateTimes) ->
    <<-1:32/integer>>;
parameter({Numeric, Weight, Scale}, ?NUMERICOID, _OIDMap, _IntegerDateTimes) ->
    D = numeric(Numeric, Weight, Scale),
    [<<(iolist_size(D)):32>> | D];
parameter(Numeric, ?NUMERICOID, OIDMap, IntegerDateTimes) ->
    parameter({Numeric, 8, 5}, ?NUMERICOID, OIDMap, IntegerDateTimes);
parameter(Float, ?FLOAT8OID, _OIDMap, _IntegerDateTimes) ->
    <<8:32/integer, Float:1/big-float-unit:64>>;
parameter(Integer, ?INT2OID, _OIDMap, _IntegerDateTimes) when is_integer(Integer) ->
    <<2:32/integer, Integer:16>>;
parameter(Integer, ?INT4OID, _OIDMap, _IntegerDateTimes) when is_integer(Integer) ->
    <<4:32/integer, Integer:32>>;
parameter(UUID, ?UUIDOID, _OIDMap, _IntegerDateTimes) ->
    uuid(UUID);
parameter(Binary, ?TEXTOID, _OIDMap, _IntegerDateTimes) when is_binary(Binary) ->
    Text = unicode:characters_to_binary(Binary, utf8),
    Size = byte_size(Text),
    <<Size:32/integer, Text/binary>>;
parameter({array, []}, ?JSONBOID, _OIDMap, _IntegerDateTimes) ->
    Binary = <<"{}">>,
    Size = byte_size(Binary),
    <<(Size+1):32/integer, ?JSONB_VERSION_1:8, Binary/binary>>;
parameter({array, List}, Type, OIDMap, IntegerDateTimes) ->
    array(List, Type, OIDMap, IntegerDateTimes);
parameter(Binary, ?JSONBOID, _OIDMap, _IntegerDateTimes) when is_binary(Binary) ->
    Size = byte_size(Binary),
    <<(Size+1):32/integer, ?JSONB_VERSION_1:8, Binary/binary>>;
parameter({jsonb, Binary}, ?JSONBOID, _OIDMap, _IntegerDateTimes) ->
    Size = byte_size(Binary),
    <<(Size+1):32/integer, ?JSONB_VERSION_1:8, Binary/binary>>;
parameter(Binary, ?JSONBOID, _OIDMap, _IntegerDateTimes) ->
    Size = byte_size(Binary),
    <<(Size+1):32/integer, ?JSONB_VERSION_1:8, Binary/binary>>;
parameter({json, Binary}, _Type, _OIDMap, _IntegerDateTimes) ->
    Size = byte_size(Binary),
    <<Size:32/integer, Binary/binary>>;
parameter(Binary, ?JSONOID, _OIDMap, _IntegerDateTimes) ->
    Size = byte_size(Binary),
    <<Size:32/integer, Binary/binary>>;
parameter({jsonb, Binary}, _Type, _OIDMap, _IntegerDateTimes) ->
    Size = byte_size(Binary),
    <<(Size+1):32/integer, ?JSONB_VERSION_1:8, Binary/binary>>;

parameter({interval, {T, D, M}}, _, _OIDMap, true) ->
    <<16:32/integer, (time(T, true)):64, D:32, M:32>>;
parameter({T, D, M}, ?INTERVALOID, _OIDMap, true) ->
    <<16:32/integer, (time(T, true)):64, D:32, M:32>>;
parameter({T, D, M}, ?INTERVALOID, _OIDMap, false) ->
    <<16:32/integer, (time(T, false)):1/big-float-unit:64, D:32, M:32>>;

parameter(Binary, _Type, _OIDMap, _IntegerDateTimes) when is_binary(Binary) ->
    Size = byte_size(Binary),
    <<Size:32/integer, Binary/binary>>;
parameter(Float, _Type, _OIDMap, _IntegerDateTimes) when is_float(Float) ->
    <<4:32/integer, Float:1/big-float-unit:32>>;
parameter(Integer, ?INT8OID, _OIDMap, _IntegerDateTimes) ->
    <<8:32/integer, Integer:64>>;
parameter(Integer, _Type, _OIDMap, _IntegerDateTimes) when is_integer(Integer) ->
    <<4:32/integer, Integer:32>>;

parameter(true, _Type, _OIDMap, _IntegerDateTimes) ->
    <<1:32/integer, 1:1/big-signed-unit:8>>;
parameter(false, _Type, _OIDMap, _IntegerDateTimes) ->
    <<1:32/integer, 0:1/big-signed-unit:8>>;

parameter(#{x := X, y := Y}, ?POINTOID, _OIDMap, _IntegerDateTimes) ->
    <<16:32/integer, X:1/big-float-unit:64, Y:1/big-float-unit:64>>;
parameter(#{long := X, lat := Y}, ?POINTOID, _OIDMap, _IntegerDateTimes) ->
    <<16:32/integer, X:1/big-float-unit:64, Y:1/big-float-unit:64>>;
parameter({point, {X, Y}}, ?POINTOID, _OIDMap, _IntegerDateTimes) ->
    <<16:32/integer, X:1/big-float-unit:64, Y:1/big-float-unit:64>>;
parameter({X, Y}, ?POINTOID, _OIDMap, _IntegerDateTimes) ->
    <<16:32/integer, X:1/big-float-unit:64, Y:1/big-float-unit:64>>;

parameter(T={{_, _, _}, {_, _, _}}, ?TIMESTAMPOID, _OIDMap, true) ->
    <<8:32/integer, (timestamp(T, true)):64>>;
parameter(T={{_, _, _}, {_, _, _}}, ?TIMESTAMPOID, _OIDMap, false) ->
    <<8:32/integer, (timestamp(T, false)):1/big-float-unit:64>>;

parameter(Time, ?TIMEOID, _OIDMap, true) ->
    <<8:32/integer, (time(Time, true)):64>>;
parameter(Time, ?TIMEOID, _OIDMap, false) ->
    <<8:32/integer, (time(Time, false)):1/big-float-unit:64>>;

parameter(Date, ?DATEOID, _OIDMap, _IntegerDateTimes) ->
    <<4:32/integer, (date(Date) - ?POSTGRES_EPOC_JDATE):32>>;

parameter({Time, Tz}, ?TIMETZOID, _OIDMap, true) ->
    <<12:32/integer, (time(Time, true)):64, Tz:32>>;
parameter({Time, Tz}, ?TIMETZOID, _OIDMap, false) ->
    <<12:32/integer, (time(Time, false)):1/big-float-unit:64, Tz:32>>;

parameter(Timestamp, ?TIMESTAMPTZOID, _OIDMap, true) ->
    <<(timestamp(Timestamp, true)):64>>;
parameter(Timestamp, ?TIMESTAMPTZOID, _OIDMap, false) ->
    <<(timestamp(Timestamp, false)):1/big-float-unit:64>>;

parameter(Binary, _, _OIDMap, _IntegerDateTimes) when is_binary(Binary)
                                                             ; is_list(Binary)->
    Text = unicode:characters_to_binary(Binary, utf8),
    Size = byte_size(Text),
    <<Size:32/integer, Text/binary>>;
parameter(Value, _Type, _OIDMap, _IntegerDateTimes) ->
    throw({badarg, Value}).

timestamp({Date, Time}, true) ->
    D = date(Date) - ?POSTGRES_EPOC_JDATE,
    D * ?USECS_PER_DAY + time(Time, true);
timestamp({Date, Time}, false) ->
    D = date(Date) - ?POSTGRES_EPOC_JDATE,
    D * ?SECS_PER_DAY + time(Time, false).

date({Y, M, D}) ->
    M2 = case M > 2 of
        true ->
            M + 1;
        false ->
            M + 13
    end,
    Y2 = case M > 2 of
        true ->
            Y + 4800;
        false ->
            Y + 4799
    end,
    C = Y2 div 100,
    J1 = Y2 * 365 - 32167,
    J2 = J1 + (Y2 div 4 - C + C div 4),
    J2 + 7834 * M2 div 256 + D.

time(0, _) ->
    0;
time({H, M, S}, true) ->
    US = trunc(round(S * ?USECS_PER_SEC)),
    ((H * ?MINS_PER_HOUR + M) * ?SECS_PER_MINUTE) * ?USECS_PER_SEC + US;
time({H, M, S}, false) ->
    ((H * ?MINS_PER_HOUR + M) * ?SECS_PER_MINUTE) + S.

array(Elements, ArrayType, OIDMap, IntegerDateTimes) ->
    ElementType = array_type_to_element_type(ArrayType, OIDMap),
    ArrayElements = array_elements(Elements, ElementType, OIDMap, IntegerDateTimes, []),
    array_binary(ArrayElements, ElementType).

uuid(<<>>) ->
    <<-1:32/integer>>;
uuid(null) ->
    <<-1:32/integer>>;
uuid(<<U:128>>) ->
    <<16:1/big-signed-unit:32, U:128>>;
uuid(U) when is_integer(U) ->
    <<16:1/big-signed-unit:32, U:128>>;
uuid(U) when is_binary(U) ->
    uuid(binary_to_list(U));
uuid(U) ->
    Hex = [H || H <- U, H =/= $-],
    {ok, [Int], _} = io_lib:fread("~16u", Hex),
    <<16:1/big-signed-unit:32, Int:128>>.

array_type_to_element_type(undefined, _OIDMap) -> undefined;
array_type_to_element_type(?CIDRARRAYOID, _OIDMap) -> ?CIDROID;
array_type_to_element_type(?UUIDARRAYOID, _OIDMap) -> ?UUIDOID;
array_type_to_element_type(?JSONBOID, _OIDMap) -> ?JSONBOID;
array_type_to_element_type(?JSONOID, _OIDMap) -> ?JSONOID;
array_type_to_element_type(?BOOLARRAYOID, _OIDMap) -> ?BOOLOID;
array_type_to_element_type(?BYTEAARRAYOID, _OIDMap) -> ?BYTEAOID;
array_type_to_element_type(?CHARARRAYOID, _OIDMap) -> ?CHAROID;
array_type_to_element_type(?NAMEARRAYOID, _OIDMap) -> ?NAMEOID;
array_type_to_element_type(?INT2ARRAYOID, _OIDMap) -> ?INT2OID;
array_type_to_element_type(?INT2VECTORARRAYOID, _OIDMap) -> ?INT2VECTOROID;
array_type_to_element_type(?INT4ARRAYOID, _OIDMap) -> ?INT4OID;
array_type_to_element_type(?REGPROCARRAYOID, _OIDMap) -> ?REGPROCOID;
array_type_to_element_type(?TEXTARRAYOID, _OIDMap) -> ?TEXTOID;
array_type_to_element_type(?TIDARRAYOID, _OIDMap) -> ?TIDOID;
array_type_to_element_type(?XIDARRAYOID, _OIDMap) -> ?XIDOID;
array_type_to_element_type(?CIDARRAYOID, _OIDMap) -> ?CIDOID;
array_type_to_element_type(?OIDVECTORARRAYOID, _OIDMap) -> ?OIDVECTOROID;
array_type_to_element_type(?BPCHARARRAYOID, _OIDMap) -> ?BPCHAROID;
array_type_to_element_type(?VARCHARARRAYOID, _OIDMap) -> ?VARCHAROID;
array_type_to_element_type(?INT8ARRAYOID, _OIDMap) -> ?INT8OID;
array_type_to_element_type(?POINTARRAYOID, _OIDMap) -> ?POINTOID;
array_type_to_element_type(?LSEGARRAYOID, _OIDMap) -> ?LSEGOID;
array_type_to_element_type(?PATHARRAYOID, _OIDMap) -> ?PATHOID;
array_type_to_element_type(?BOXARRAYOID, _OIDMap) -> ?BOXOID;
array_type_to_element_type(?FLOAT4ARRAYOID, _OIDMap) -> ?FLOAT4OID;
array_type_to_element_type(?FLOAT8ARRAYOID, _OIDMap) -> ?FLOAT8OID;
array_type_to_element_type(?ABSTIMEARRAYOID, _OIDMap) -> ?ABSTIMEOID;
array_type_to_element_type(?RELTIMEARRAYOID, _OIDMap) -> ?RELTIMEOID;
array_type_to_element_type(?TINTERVALARRAYOID, _OIDMap) -> ?TINTERVALOID;
array_type_to_element_type(?POLYGONARRAYOID, _OIDMap) -> ?POLYGONOID;
array_type_to_element_type(?OIDARRAYOID, _OIDMap) -> ?OIDOID;
array_type_to_element_type(?ACLITEMARRAYOID, _OIDMap) -> ?ACLITEMOID;
array_type_to_element_type(?MACADDRARRAYOID, _OIDMap) -> ?MACADDROID;
array_type_to_element_type(?INETARRAYOID, _OIDMap) -> ?INETOID;
array_type_to_element_type(?CSTRINGARRAYOID, _OIDMap) -> ?CSTRINGOID;
array_type_to_element_type(TypeOID, OIDMap) ->
    Type = decode_oid(TypeOID, OIDMap),
    if not is_atom(Type) -> undefined;
        true ->
            case atom_to_list(Type) of
                [$_ | ContentType] -> % Array
                    OIDContentType = type_to_oid(list_to_atom(ContentType), OIDMap),
                    OIDContentType;
                _ -> undefined
            end
    end.

array_elements([{array, SubArray} | Tail], ElementType, OIDMap, IntegerDateTimes, Acc) ->
    SubArrayElements = array_elements(SubArray, ElementType, OIDMap, IntegerDateTimes, []),
    array_elements(Tail, ElementType, OIDMap, IntegerDateTimes, [{array, SubArrayElements} | Acc]);
array_elements([null | Tail], ElementType, OIDMap, IntegerDateTimes, Acc) ->
    array_elements(Tail, ElementType, OIDMap, IntegerDateTimes, [null | Acc]);
array_elements([Element | Tail], ElementType, OIDMap, IntegerDateTimes, Acc) ->
    Encoded = parameter(Element, ElementType, OIDMap, IntegerDateTimes),
    array_elements(Tail, ElementType, OIDMap, IntegerDateTimes, [Encoded | Acc]);
array_elements([], _ElementType, _OIDMap, _IntegerDateTimes, Acc) ->
    lists:reverse(Acc).

array_binary(ArrayElements, ElementTypeOID) ->
    {HasNulls, Rows} = array_binary_row(ArrayElements, false, []),
    Dims = get_array_dims(ArrayElements),
    Header = array_binary_header(Dims, HasNulls, ElementTypeOID),
    Encoded = [Header, Rows],
    Size = iolist_size(Encoded),
    [<<Size:32/integer>>, Encoded].

array_binary_row([null | Tail], _HasNull, Acc) ->
    array_binary_row(Tail, true, [<<-1:32/integer>> | Acc]);
array_binary_row([<<_BinarySize:32/integer, _BinaryVal/binary>> = Binary | Tail], HasNull, Acc) ->
    array_binary_row(Tail, HasNull, [Binary | Acc]);
array_binary_row([{array, Elements} | Tail], HasNull, Acc) ->
    {NewHasNull, Row} = array_binary_row(Elements, HasNull, []),
    array_binary_row(Tail, NewHasNull, [Row | Acc]);
array_binary_row([], HasNull, AccRow) ->
    {HasNull, lists:reverse(AccRow)}.

get_array_dims([{array, SubElements} | _] = Row) ->
    Dims0 = get_array_dims(SubElements),
    Dim = length(Row),
    [Dim | Dims0];
get_array_dims(Row) ->
    Dim = length(Row),
    [Dim].

array_binary_header(Dims, HasNulls, ElementTypeOID) ->
    NDims = length(Dims),
    Flags = if
        HasNulls -> 1;
        true -> 0
    end,
    EncodedDimensions = [<<Dim:32/integer, 1:32/integer>> || Dim <- Dims],
    [<<
        NDims:32/integer,
        Flags:32/integer,
        ElementTypeOID:32/integer
    >>,
    EncodedDimensions].

%%--------------------------------------------------------------------
%% @doc Determine if we need the statement description with these parameters.
%% We currently only require statement descriptions if we have arrays of
%% binaries.
-spec bind_requires_statement_description([any()]) -> boolean().
bind_requires_statement_description([]) -> false;
bind_requires_statement_description([{array, [{array, SubArrayElems} | SubArrayT]} | Tail]) ->
    bind_requires_statement_description([{array, SubArrayElems}, {array, SubArrayT} | Tail]);
bind_requires_statement_description([{array, [ArrayElem | _]} | _]) when is_binary(ArrayElem) -> true;
bind_requires_statement_description([{array, [null | ArrayElemsT]} | Tail]) ->
    bind_requires_statement_description([{array, ArrayElemsT} | Tail]);
bind_requires_statement_description([{array, []} | Tail]) ->
    bind_requires_statement_description(Tail);
bind_requires_statement_description([_OtherParam | Tail]) ->
    bind_requires_statement_description(Tail).

%%--------------------------------------------------------------------
%% @doc Encode a describe message.
%%
-spec describe_message(portal | statement, iodata()) -> iolist().
describe_message(PortalOrStatement, Name) ->
    MessageLen = iolist_size(Name) + 6,
    WhatByte = case PortalOrStatement of
        portal -> $P;
        statement -> $S
    end,
    [<<$D, MessageLen:32/integer, WhatByte>>, Name, <<0>>].

%%--------------------------------------------------------------------
%% @doc Encode an execute message.
%%
-spec execute_message(iodata(), non_neg_integer()) -> iolist().
execute_message(PortalName, MaxRows) ->
    MessageLen = iolist_size(PortalName) + 9,
    [<<$E, MessageLen:32/integer>>, PortalName, <<0, MaxRows:32/integer>>].

%%--------------------------------------------------------------------
%% @doc Encode a sync message.
%%
-spec sync_message() -> binary().
sync_message() ->
    <<$S, 4:32/integer>>.

%%--------------------------------------------------------------------
%% @doc Encode a flush message.
%%
-spec flush_message() -> binary().
flush_message() ->
    <<$H, 4:32/integer>>.

%%--------------------------------------------------------------------
%% @doc Encode a flush message.
%%
-spec cancel_message(integer(), integer()) -> binary().
cancel_message(ProcID, Secret) ->
    <<16:32/integer, 80877102:32/integer, ProcID:32/integer, Secret:32/integer>>.

%%--------------------------------------------------------------------
%% @doc Encode a string message.
%%
-spec string_message(byte(), iodata()) -> iolist().
string_message(Identifier, String) ->
    MessageLen = iolist_size(String) + 5,
    [<<Identifier, MessageLen:32/integer>>, String, <<0>>].

type_to_oid(Type, Pool) ->
    case ets:match_object(Pool, {'_', Type}) of
        [{OIDType, _}] ->
            OIDType;
        [] ->
            undefined
    end.

decode_oid(Oid, Pool) ->
    case ets:lookup(Pool, Oid) of
        [{_, OIDName}] -> OIDName;
        [] -> Oid
    end.
