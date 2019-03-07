-module(pg_http_sup).
-behavior(supervisor).

-export([start_link/0,
         start_child/2]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_child(Name, PoolConfig) ->
    supervisor:start_child(?SERVER, [Name, PoolConfig]).

%% Child :: #{id => Id, start => {M, F, A}}
%% Optional keys are restart, shutdown, type, modules.
%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 5,
                 period => 10},
    ChildSpec = #{id => pg_http_pool,
                  start => {pg_http_pool, start_link, []},
                  shutdown => 1000},
    {ok, {SupFlags, [ChildSpec]}}.
