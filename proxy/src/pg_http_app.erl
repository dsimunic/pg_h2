%%%-------------------------------------------------------------------
%% @doc pg_http public API
%% @end
%%%-------------------------------------------------------------------

-module(pg_http_app).
-behavior(application).

%% Application callback
-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    pg_http_query_cache:start_link(),
    Pools = application:get_env(pg_http, pools, []),

    {ok, SupPid} = pg_http_sup:start_link(),
    [{ok, _} = pg_http_sup:start_child(Name, PoolConfig) || {Name, PoolConfig} <- Pools],

    Dispatch = cowboy_router:compile([ {'_', [{"/", pg_handler, []}]} ]),
    {ok, _} = cowboy:start_clear(my_http_listener, [{port, 8080}], #{env => #{dispatch => Dispatch}}),
    {ok, _} = cowboy:start_tls(https, [ {port, 8443},
                                        {cacertfile, os:getenv("CA_CERT_PATH") },
                                        {certfile, os:getenv("CERT_PATH") },
                                        {keyfile, os:getenv("CERT_KEY_PATH") } ]
                              , #{env => #{dispatch => Dispatch}}
                              ),
    {ok, SupPid, []}.


stop(_State) ->
	ok.
