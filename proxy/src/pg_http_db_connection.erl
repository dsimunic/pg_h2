-module(pg_http_db_connection).

-behaviour(gen_statem).

-export([start_link/5,
         ping/3,
         stop/3,
         disconnect/4,
         pool_name/1,
         reload_types/1,
         break/1,
         break/2]).

-export([init/1,
         callback_mode/0,
         connected/3,
         disconnected/3,
         enqueued/3,
         dequeued/3,
         terminate/3]).

-include("pg_http_pg_protocol.hrl").

-record(data, {monitor :: reference() | undefined,
               ref :: reference() | undefined,
               queue :: reference(),
               conn :: #conn{} | undefined,
               pool_config :: pgo:pool_config(),
               holder :: ets:tid() | undefined,
               broker :: atom(),
               pool :: pid(),
               sup :: pid(),
               backoff :: backoff:backoff()}).

start_link(QueueTid, PoolPid, PoolName, Sup, PoolConfig) ->
    gen_statem:start_link(?MODULE, {QueueTid, PoolPid, PoolName, Sup, PoolConfig}, []).

pool_name({_Pid, Holder}) ->
    [PoolName] = ets:lookup(Holder, pool_name),
    PoolName.

-spec ping(pid(), ets:tid(), pg_http_db_pool:conn()) -> ok.
ping(Pid, Holder, _Conn) ->
    gen_statem:cast(Pid, {ping, Holder}).

-spec stop(pid(), ets:tid(), pg_http_db_pool:conn()) -> ok.
stop(Pid, Holder, _Conn) ->
    gen_statem:cast(Pid, {stop, Holder}).

-spec disconnect(pid(), ets:tid(), {error, atom()}, pg_pool:conn()) -> ok.
disconnect(Pid, Holder, _Err, _Conn) ->
    %% maybe log Err?
    gen_statem:cast(Pid, {disconnect, Holder}).

-spec reload_types(pg_http_db_pool:conn()) -> ok.
reload_types(#conn{owner=Pid}) ->
    gen_statem:call(Pid, reload_types).

-spec break(pg_http_db_pool:conn(), pg_http_db_pool:ref()) -> ok.
break(#conn{owner=Pid}, {_Pool, _Ref, _Deadline, Holder}) ->
    gen_statem:cast(Pid, {break, Holder}).

-spec break(pg_http_db_pool:conn()) -> ok.
break(#conn{owner=Pid}) ->
    gen_statem:cast(Pid, break).

init({QueueTid, Pool, PoolName, Sup, PoolConfig}) ->
    erlang:process_flag(trap_exit, true),
    B = backoff:init(1000, 10000),
    {ok, disconnected, #data{backoff=B,
                             pool=Pool,
                             broker=PoolName,
                             queue=QueueTid,
                             sup=Sup,
                             pool_config=PoolConfig},
     {next_event, internal, connect}}.

callback_mode() ->
    state_functions.

disconnected(EventType, _, Data=#data{broker=Broker,
                                      backoff=B,
                                      queue=QueueTid,
                                      pool=Pool,
                                      pool_config=PoolConfig}) when EventType =:= internal
                                                                    ; EventType =:= timeout
                                                                    ; EventType =:= state_timeout ->
    try pg_http_db_handler:open(Broker, PoolConfig) of
        {ok, Conn} ->
            Holder = pg_http_db_pool:update(Pool, QueueTid, ?MODULE, Conn),
            {_, B1} = backoff:succeed(B),
            {next_state, enqueued, Data#data{conn=Conn,
                                             holder=Holder,
                                             backoff=B1}};
        _Error ->
            {Backoff, B1} = backoff:fail(B),
            {next_state, disconnected, Data#data{broker=Broker,
                                                 backoff=B1},
             [{state_timeout, Backoff, connect}]}
    catch
        throw:_Reason ->
            {Backoff, B1} = backoff:fail(B),
            {next_state, disconnected, Data#data{broker=Broker,
                                                 backoff=B1},
             [{state_timeout, Backoff, connect}]}
    end;
disconnected(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

connected(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

enqueued(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

dequeued(EventType, EventContent, Data) ->
    handle_event(EventType, EventContent, Data).

handle_event(cast, {ping, Holder}, Data=#data{pool=Pool,
                                              holder=Holder,
                                              queue=QueueTid,
                                              conn=Conn}) ->
    %% TODO: do ping
    NewHolder = pg_http_db_pool:update(Pool, QueueTid, ?MODULE, Conn),
    {keep_state, Data#data{holder=NewHolder}};
handle_event(cast, {stop, Holder}, Data=#data{holder=Holder,
                                              conn=Conn}) ->
    pg_http_db_handler:close(Conn),
    {stop, Data#data{conn=undefined,
                     holder=undefined}};
handle_event(cast, {disconnect, Holder}, Data=#data{holder=Holder}) ->
    close_and_reopen(Data);
handle_event(cast, break, Data) ->
    close_and_reopen(Data);
handle_event(cast, {break, Holder}, Data=#data{holder=Holder}) ->
    close_and_reopen(Data);
handle_event({call, From}, reload_types, #data{sup=Sup}) ->
    TypeServer = pg_http_db_pool_sup:whereis_child(Sup, type_server),
    pg_http_db_type_server:reload(TypeServer),
    {keep_state_and_data, [{reply, From, ok}]};
handle_event(info, {'EXIT', Socket, _Reason}, Data=#data{conn=Socket}) ->
    %% socket died, go to disconnected state
    {next_state, disconnected, Data#data{conn=undefined},
     [{next_event, internal, connect}]}.

%% @private
terminate(_Reason, _, #data{conn=undefined}) ->
    ok;
terminate(_Reason, _, #data{conn=Conn}) ->
    pg_http_db_handler:close(Conn),
    ok.

%%

close_and_reopen(Data=#data{conn=Conn}) ->
    pg_http_db_handler:close(Conn),
    {next_state, disconnected, Data#data{conn=undefined,
                                         holder=undefined},
     [{next_event, internal, connect}]}.
