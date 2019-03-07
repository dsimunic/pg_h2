%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <t@crashfast.com>
%%%
%%% @doc Postgres driver and pool for Erlang.
%%%
%%% This module provides functions for interacting with a pool and
%%  querying the database.
%%% @end
%%% ---------------------------------------------------------------------------
-module(pg_http).

-export([start_pool/2,
         query/1,
         query/2,
         query/3,
         transaction/1,
         transaction/2,
         transaction/3,
         with_conn/2,
         checkout/1,
         checkout/2,
         checkin/2,
         break/1]).

-include("pg_http_internal.hrl").

-export_type([result/0,
              error/0,
              pool_config/0,
              decode_fun/0]).

-type result() :: #{command := atom(),
                    num_rows := integer(),
                    rows := list()} | {error, error()} | {error, any()}.

-type error() :: {pg_http_error, #{error_field() => binary()}}.

-type pool() :: atom().

-type row() :: list() | map().
-type fields() :: [#row_description_field{}].
-type decode_fun() :: fun((row(), fields()) -> row()) | undefined.

-type decode_option() :: return_rows_as_maps | {return_rows_as_maps, boolean()} |
                         column_name_as_atom | {column_name_as_atom, boolean()} |
                         {decode_fun, decode_fun()}.

-type pool_option() :: queue | {queue, boolean()}.
-type options() :: #{pool => atom(),
                     trace => boolean(),
                     queue => boolean(),
                     decode_opts => [decode_option()]}.

-type pool_config() :: #{host => string(),
                         port => integer(),
                         user => string(),
                         password => string(),
                         database => string(),

                         %% pool specific settings
                         pool_size => integer(),
                         queue_target => integer(),
                         queue_interval => integer(),
                         idle_interval => integer(),

                         %% defaults for options used at query time
                         queue => boolean(),
                         trace => boolean(),
                         decode_opts => [decode_option()]}.

%% @doc Starts connection pool as a child of pg_http_sup.
-spec start_pool(pool(), pool_config()) -> {ok, pid()}.
start_pool(Name, PoolConfig) ->
    pg_http_sup:start_child(Name, PoolConfig).

%% @equiv query(Query, [], #{})
-spec query(iodata()) -> result().
query(Query) ->
    query(Query, [], #{}).

%% @equiv query(Query, Params, #{})
-spec query(iodata(), list()) -> result().
query(Query, Params) ->
    query(Query, Params, #{}).

%% @doc Executes an extended query either on a Pool or a provided connection.
-spec query(iodata(), list(), options()) -> result().
query(Query, Params, Options) ->
    DecodeOptions = maps:get(decode_opts, Options, []),
    case get(pg_http_transaction_connection) of
        undefined ->
            Pool = maps:get(pool, Options, default),
            PoolOptions = maps:get(pool_options, Options, []),
            case checkout(Pool, PoolOptions) of
                {ok, Ref, Conn=#conn{trace=TraceDefault, decode_opts=DefaultDecodeOpts}} ->
                    DoTrace = maps:get(trace, Options, TraceDefault),
                    {SpanCtx, ParentCtx} = maybe_start_span(DoTrace,
                                                            <<"pgo:query/3">>,
                                                            #{attributes => #{<<"query">> => Query}}),
                    try
                        pg_http_handler:extended_query(Conn, Query, Params,
                                                   DecodeOptions ++ DefaultDecodeOpts,
                                                   #{queue_time => undefined})
                    after
                        maybe_finish_span(DoTrace, SpanCtx, ParentCtx),
                        checkin(Ref, Conn)
                    end;
                {error, _}=E ->
                    E
            end;
        Conn=#conn{pool=Pool, decode_opts=DefaultDecodeOpts} ->
            %% verify we aren't trying to run a query against another pool from a transaction
            case maps:get(pool, Options, Pool) of
                P when P =:= Pool ->
                    pg_http_handler:extended_query(Conn, Query, Params,
                                               DecodeOptions ++ DefaultDecodeOpts,
                                               #{queue_time => undefined});
                P ->
                    error({in_other_pool_transaction, P})
            end
    end.

%% @equiv transaction(default, Fun, [])
-spec transaction(fun(() -> any())) -> any() | {error, any()}.
transaction(Fun) ->
    transaction(default, Fun, #{}).

%% @equiv transaction(default, Fun, Options)
-spec transaction(fun(() -> any()), options()) -> any() | {error, any()}.
transaction(Fun, Options) when is_function(Fun) ->
    transaction(Fun, Options).

%% @doc Runs a function, passing it a connection, in a SQL transaction.
-spec transaction(pool(), fun(() -> any()), options()) -> any() | {error, any()}.
transaction(Pool, Fun, Options) ->
    case get(pg_http_transaction_connection) of
        undefined ->
            PoolOptions = maps:get(pool_options, Options, []),
            case checkout(Pool, PoolOptions) of
                {ok, Ref, Conn=#conn{trace=TraceDefault}} ->
                    DoTrace = maps:get(trace, Options, TraceDefault),
                    {SpanCtx, ParentCtx} = maybe_start_span(DoTrace,
                                                            <<"pgo:transaction/2">>,
                                                            #{}),
                    try
                        #{command := 'begin'} = pg_http_handler:extended_query(Conn, "BEGIN", [],
                                                                           #{queue_time => undefined}),
                        put(pg_http_transaction_connection, Conn),
                        Result = Fun(),
                        #{command := commit} = pg_http_handler:extended_query(Conn, "COMMIT", [],
                                                                          #{queue_time => undefined}),
                        Result
                    catch
                        ?WITH_STACKTRACE(T, R, S)
                        pg_http_handler:extended_query(Conn, "ROLLBACK", [], #{queue_time => undefined}),
                        erlang:raise(T, R, S)
                    after
                        maybe_finish_span(DoTrace, SpanCtx, ParentCtx),
                        checkin(Ref, Conn),
                        erase(pg_http_transaction_connection)
                    end;
                {error, _}=E ->
                    E
            end;
        _Conn ->
            %% already in a transaction
            Fun()
    end.


maybe_start_span(false, _, _) ->
    {undefined, undefined};
maybe_start_span(true, Name, Attributes) ->
    CurrentSpanCtx = ocp:current_span_ctx(),
    NewSpanCtx = oc_trace:start_span(Name, ocp:current_span_ctx(), Attributes),
    ocp:with_span_ctx(NewSpanCtx),
    {NewSpanCtx, CurrentSpanCtx}.

maybe_finish_span(false, _, _) ->
    undefined;
maybe_finish_span(true, SpanCtx, ParentCtx) ->
    oc_trace:finish_span(SpanCtx),
    ocp:with_span_ctx(ParentCtx).

with_conn(Conn, Fun) ->
    case get(pg_http_transaction_connection) of
        undefined ->
            put(pg_http_transaction_connection, Conn),
            try Fun()
            after
                erase(pg_http_transaction_connection)
            end;
        OldConn ->
            try Fun()
            after
                put(pg_http_transaction_connection, OldConn)
            end
    end.

%% @doc Returns a connection from the pool.
-spec checkout(atom()) -> {ok, pg_http_pool:pool_ref(), pg_http_pool:conn()} | {error, any()}.
checkout(Pool) ->
    pg_http_pool:checkout(Pool, []).

-spec checkout(atom(), [pool_option()]) -> {ok, pg_http_pool:pool_ref(), pg_http_pool:conn()} | {error, any()}.
checkout(Pool, Options) ->
    pg_http_pool:checkout(Pool, Options).

%% @doc Return a checked out connection to its pool
-spec checkin(pg_http_pool:pool_ref(), pg_http_pool:conn()) -> ok.
checkin(Ref, Conn) ->
    pg_http_pool:checkin(Ref, Conn, []).

%% @doc Disconnects the socket held by this reference.
-spec break(pg_http_pool:conn()) -> ok.
break(Conn) ->
    pg_http_connection:break(Conn).
