-module(pg_http_cowboy_handler).
-behavior(cowboy_handler).

-export([init/2, info/3]).


init(Req=#{method := <<"QUERY">>} , State) ->
    % process_flag(trap_exit, true),

    {ok, Query, _} = cowboy_req:read_body(Req),
    % _Ignore = get(pg_http_transaction_connection),
    % Here we want to send in the cowboy stream, so that we can
    % write binary output directly without interpreting the body
    Req1 = pg_http_http_replies:set_query_response_headers(Req),
    DbResponse = query(Query, [], Req1, #{}),

    {ok, DbResponse, State};

init(Req=#{method := <<"OPTIONS">>}, State) -> { ok, pg_http_http_replies:no_content(Req), State};

init(Req,State) -> {ok, pg_http_http_replies:invalid_request(Req), State }.


info({'EXIT', _, _}, Req, State) ->
    {ok, pg_http_http_replies:invalid_request(Req), State};

info(_Msg, Req, State) ->
    {ok, Req, State, hibernate}.

-include("pg_http_pg_protocol_opaque.hrl").


%%--------------------------------------------------------------------
%% @doc Set up tracing span if this is a new quer, and then call
%% extended_query/5.
%%
% -spec query(iodata(), list(), options()) -> result().
query(Query, Params, HttpReq, Options) ->
    case get(pg_http_transaction_connection) of
        undefined ->
            Pool = maps:get(pool, Options, default),
            PoolOptions = maps:get(pool_options, Options, []),
            case pg_http:checkout(Pool, PoolOptions) of
                {ok, Ref, Conn=#conn{trace=TraceDefault}} ->
                    DoTrace = maps:get(trace, Options, TraceDefault),
                    {SpanCtx, ParentCtx} = pg_http:maybe_start_span(DoTrace,
                                                            <<"pg_http:query/3">>,
                                                            #{attributes => #{<<"query">> => Query}}),
                    try
                        extended_query(Conn, Query, Params, HttpReq, #{queue_time => undefined})
                    after
                        pg_http:maybe_finish_span(DoTrace, SpanCtx, ParentCtx),
                        pg_http:checkin(Ref, Conn)
                    end;
                {error, _}=E ->
                    E
            end;
        Conn=#conn{pool=Pool} ->
            %% verify we aren't trying to run a query against another pool from a transaction
            case maps:get(pool, Options, Pool) of
                P when P =:= Pool ->
                    extended_query(Conn, Query, Params, HttpReq, #{queue_time => undefined});
                P ->
                    error({in_other_pool_transaction, P})
            end
    end.

%%----------------------------------------------------------------------
%% @doc Set up the latency measure and call extended_query/6 to do the
%% actual work
%%
extended_query(Socket=#conn{pool=Pool}, Query, Parameters, HttpReq, Timings) ->
    Start = erlang:monotonic_time(),
    Result = extended_query_2(Socket, Query, Parameters, HttpReq),
    Latency = erlang:monotonic_time() - Start,
    telemetry:execute([pgo, query], Latency, Timings#{pool => Pool,
                                                      query => Query,
                                                      query_time => Latency,
                                                      result => Result}),
    Result.


%%----------------------------------------------------------------------
%% @doc Run extended query
%%
extended_query_2(Conn=#conn{socket=Socket, pool=Pool}, Query, Parameters, HttpReq) ->
    put(query, Query),
    IntegerDateTimes = true,
    ParseMessage = pg_http_pg_protocol:encode_parse_message("", Query, []),
    % We ask for a description of parameters only if required.
    PacketT = case catch(pg_http_db_query_cache:lookup(Pool, Query)) of
       DataTypes when is_list(DataTypes) ->
            case encode_bind_describe_execute(Parameters, DataTypes, Pool, IntegerDateTimes) of
                {ok, BindExecute} ->
                    {ok, [ParseMessage, BindExecute], parse_complete};
                {error, _} = Error ->
                    Error
            end;
        not_found ->
            DescribeStatementMessage = pg_http_pg_protocol:encode_describe_message(statement, ""),
            FlushMessage = pg_http_pg_protocol:encode_flush_message(),
            LoopState0 = {parse_complete_with_params, Parameters},
            {ok, [ParseMessage, DescribeStatementMessage, FlushMessage], LoopState0}

    end,
    flush_until_ready_for_query(ok, Conn),
    case PacketT of
        {ok, SinglePacket, LoopState} ->
            case gen_tcp:send(Socket, SinglePacket) of
                ok ->
                    receive_loop(LoopState,
                                 HttpReq,
                                 Conn);
                {error, _} = SendSinglePacketError ->
                    SendSinglePacketError
            end;
        {error, _} ->
            PacketT
    end.

-spec encode_bind_describe_execute([any()], [oid()], atom(), boolean()) -> {ok, iodata()} | {error, any()}.
encode_bind_describe_execute(Parameters, ParameterDataTypes, Pool, IntegerDateTimes) ->
    DescribeMessage = pg_http_pg_protocol:encode_describe_message(portal, ""),
    ExecuteMessage = pg_http_pg_protocol:encode_execute_message("", 0),
    SyncOrFlushMessage = pg_http_pg_protocol:encode_sync_message(),
    try
        BindMessage = pg_http_pg_protocol:encode_bind_message("", "", Parameters, ParameterDataTypes,
                                                       Pool, IntegerDateTimes),
        SinglePacket = [BindMessage, DescribeMessage, ExecuteMessage, SyncOrFlushMessage],
        {ok, SinglePacket}
    catch throw:Exception ->
            {error, Exception};
          _:Exception ->
            {error, Exception}
    end.

%% requires_statement_description(_Parameters) ->
%%     true. %pg_http_pg_protocol:bind_requires_statement_description(Parameters).

% -spec receive_loop(extended_query_loop_state(), pgo:decode_fun(), list(), list(), pgo:conn()) -> pgo:result().
receive_loop(LoopState, HttpReq, Conn=#conn{socket=Socket}) ->
    case receive_message(Socket) of
        {ok, Message} ->
            receive_loop0(Message, LoopState, HttpReq, Conn);
        {error, _} = ReceiveError ->
            ReceiveError
    end.
%% TODO: Handle Close/CloseComplete
%% > Close
%% < CloseComplete
%% > Sync

receive_loop0(#parameter_status{name=_Name, value=_Value}, LoopState, HttpReq, Conn) ->
    %% State1 = handle_parameter(Name, Value, Conn),
    receive_loop(LoopState, HttpReq, Conn);
receive_loop0(#parse_complete{}, parse_complete, HttpReq, Conn) ->
    receive_loop(bind_complete, HttpReq, Conn);

%% Path where we ask the backend about what it expects.
%% We ignore row descriptions sent before bind as the format codes are null.
receive_loop0(#parse_complete{}, {parse_complete_with_params, Parameters}, HttpReq, Conn) ->
    receive_loop({parameter_description_with_params, Parameters}, HttpReq, Conn);

receive_loop0(#parameter_description{data_types=ParameterDataTypes}, {parameter_description_with_params, Parameters}, HttpReq, Conn=#conn{socket=Socket, pool=Pool}) ->
    pg_http_db_query_cache:insert(Pool, get(query), ParameterDataTypes),
    % oob_update_oid_map_if_required(Conn, ParameterDataTypes),
    PacketT = encode_bind_describe_execute(Parameters, ParameterDataTypes, Pool, true),
    case PacketT of
        {ok, SinglePacket} ->
            case gen_tcp:send(Socket, SinglePacket) of
                ok ->
                    receive_loop(pre_bind_row_description, HttpReq, Conn);
                {error, _} = SendError ->
                    SendError
            end;
        {error, _} = Error ->
            case gen_tcp:send(Socket, pg_http_pg_protocol:encode_sync_message()) of
                ok -> flush_until_ready_for_query(Error, Conn);
                {error, _} = SendSyncPacketError -> SendSyncPacketError
            end
    end;
receive_loop0(#row_description{}, pre_bind_row_description, HttpReq, Conn) ->
    receive_loop(bind_complete, HttpReq, Conn);
receive_loop0(#no_data{}, pre_bind_row_description, HttpReq, Conn) ->
    receive_loop(bind_complete, HttpReq, Conn);

%% Common paths after bind.
receive_loop0(#bind_complete{}, bind_complete, HttpReq, Conn) ->
    receive_loop(row_description, HttpReq, Conn);

receive_loop0(#no_data{}, row_description, HttpReq, Conn) ->
    receive_loop(no_data, HttpReq, Conn);
receive_loop0(#row_description{payload = Payload}, row_description, HttpReq, Conn) ->
    % oob_update_oid_map_from_fields_if_required(Conn, Fields),
    HttpReq0 = cowboy_req:stream_reply(200, #{
        <<"content-type">> => <<"text/plain">>
    }, HttpReq),
    HttpReq1 = cowboy_req:stream_body(list_to_binary(Payload), nofin, HttpReq0),
    receive_loop({rows}, HttpReq1, Conn);

receive_loop0(#data_row{payload = Payload}, {rows} = LoopState, HttpReq, Conn) ->
    HttpReq1 = cowboy_req:stream_body(list_to_binary(Payload), nofin, HttpReq),
    receive_loop(LoopState, HttpReq1, Conn);

receive_loop0(#command_complete{command_tag = _Tag}, _LoopState, HttpReq, Conn) ->
    HttpReq1 = cowboy_req:stream_body(<<>>, fin, HttpReq),
    receive_loop(result, HttpReq1, Conn);
%% receive_loop0(#portal_suspended{}, LoopState, HttpReq, Conn={_,S}) ->
%%     ExecuteMessage = pg_http_pg_protocol:encode_execute_message("", 0),
%%     FlushMessage = pg_http_pg_protocol:encode_flush_message(),
%%     SinglePacket = [ExecuteMessage, FlushMessage],
%%     case gen_tcp:send(S, SinglePacket) of
%%         ok -> receive_loop(LoopState, HttpReq, Conn);
%%         {error, _} = SendSinglePacketError ->
%%             SendSinglePacketError
%%     end;
receive_loop0(#ready_for_query{}, _LoopState, HttpReq, __Socket) ->
    HttpReq;

receive_loop0(#error_response{fields = Fields}, LoopState, _HttpReq, Conn=#conn{socket=Socket}) ->
    Error = {error, {pgsql_error, Fields}},
    % We already sent a Sync except when we sent a Flush :-)
    % - when we asked for the statement description
    % - when MaxRowsStep > 0
    NeedSync = case LoopState of
                   {parse_complete_with_params, _Args} -> true;
                   {parameter_description_with_params, _Parameters} -> true;
                   _ -> false
               end,
    case NeedSync of
        true ->
            case gen_tcp:send(Socket, pg_http_pg_protocol:encode_sync_message()) of
                ok -> flush_until_ready_for_query(Error, Conn);
                {error, _} = SendSyncPacketError -> SendSyncPacketError
            end;
        false ->
            flush_until_ready_for_query(Error, Conn)
    end;
receive_loop0(#ready_for_query{} = Message, _LoopState, _HttpReq, _Conn) ->
    Result = {error, {unexpected_message, Message}},
    Result;
receive_loop0(Message, _LoopState, _HttpReq, Conn=#conn{socket=Socket}) ->
    gen_tcp:send(Socket, pg_http_pg_protocol:encode_sync_message()),
    Error = {error, {unexpected_message, Message}},
    flush_until_ready_for_query(Error, Conn).

flush_until_ready_for_query(Result, Conn=#conn{socket=Socket}) ->
    case receive_message(Socket) of
        {ok, #parameter_status{name = _Name, value = _Value}} ->
            flush_until_ready_for_query(Result, Conn);
        {ok, #ready_for_query{}} ->
            Result;
        {ok, _OtherMessage} ->
            flush_until_ready_for_query(Result, Conn);
        {error, _} = ReceiveError ->
            ReceiveError
    end.


%%--------------------------------------------------------------------
%% @doc Receive a single packet (in passive mode). Notifications and
%% notices are broadcast to subscribers.
%%

-define(MESSAGE_HEADER_SIZE, 5).

receive_message(Socket) ->
    Result0 = case gen_tcp:recv(Socket, ?MESSAGE_HEADER_SIZE) of
                  {ok, <<Code:8/integer, Size:32/integer>> = Head} ->
                      PayloadLength = Size - 4,
                      case PayloadLength of
                          0 ->
                              pg_http_pg_protocol_decode_opaque:decode_message(Code, [Head, <<>>]);
                          _ ->
                              case gen_tcp:recv(Socket, PayloadLength) of
                                  {ok, Rest} ->
                                      pg_http_pg_protocol_decode_opaque:decode_message(Code, [Head, Rest]);
                                  {error, _} = ErrorRecvPacket ->
                                      ErrorRecvPacket
                              end
                      end;
                  {error, _} = ErrorRecvPacketHeader ->
                      ErrorRecvPacketHeader
              end,
    case Result0 of
        {ok, #notification_response{} = _Notification} ->
            receive_message(Socket);
        {ok, #notice_response{} = _Notice} ->
            receive_message(Socket);
        _ ->
            Result0
    end.
