%%%-------------------------------------------------------------------
%% @doc prx public API
%% @end
%%%-------------------------------------------------------------------

-module(prx_app).
-behavior(application).

%% Application callback
-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, SupPid} = prx_sup:start_link(),
    Dispatch = cowboy_router:compile([ {'_', [{"/", pg_handler, []}]} ]),
    {ok, _} = cowboy:start_clear(my_http_listener, [{port, 8080}], #{env => #{dispatch => Dispatch}}),
    {ok, SupPid, []}.


stop(_State) ->
	ok.