-module(prx_sup).
-behavior(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% Child :: #{id => Id, start => {M, F, A}}
%% Optional keys are restart, shutdown, type, modules.
init([]) ->
    Child = [],
    {ok, { {one_for_all, 0, 1}, Child} }.
