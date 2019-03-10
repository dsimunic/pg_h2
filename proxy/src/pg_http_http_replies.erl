-module(pg_http_http_replies).

-export([cors_headers/0
        ,no_content/1
        ,invalid_request/1
        ,invalid_request/2
        ,validate_db_headers/4
        ,set_query_response_headers/1
        ]).

cors_headers() ->
        #{ <<"access-control-allow-methods">> => <<"GET, OPTIONS, QUERY">>
         , <<"access-control-allow-origin">> => <<"*">>
        , <<"access-control-allow-headers">> => <<"content-type,database,dbhost,dbport,password,user">>
        }.

set_query_response_headers(R) ->
    Hdrs = maps:merge(#{ <<"content-type">> => <<"postgresql/binary">> }, cors_headers()),
    lists:foldl(fun({N, V}, R1) ->
                        cowboy_req:set_resp_header(N, V, R1)
                  end, R, maps:to_list(Hdrs)).


validate_db_headers(D, U, P, H) ->
    case lists:filter(fun({_,HDR}) -> string:length(HDR) =:= 0 end, [D,U,P,H]) of
        Invalid when length(Invalid) > 0 -> { invalid, Invalid };
        _ -> ok
    end.


no_content(R) ->
    cowboy_req:reply(204,
        maps:merge(#{ <<"content-type">> => <<"text/plain">> }, cors_headers()),
        <<"No content.">>,
        R).

invalid_request(R) ->
    invalid_request(R, <<"Invalid request.">>).

invalid_request(R, Msg) ->
    cowboy_req:reply(400,
        maps:merge(#{ <<"content-type">> => <<"text/plain">> }, cors_headers()),
        Msg,
        R).
%     Db =     cowboy_req:header(<<"database">>, Req0, <<>>),
%     DbUser = cowboy_req:header(<<"user">>, Req0, <<>>),
%     DbPass = cowboy_req:header(<<"password">>, Req0, <<>>),
%     DbHost = cowboy_req:header(<<"dbhost">>, Req0, <<"localhost">>),
%     DbPort = cowboy_req:header(<<"dbport">>, Req0, <<"5432">>),
%     _Accept = cowboy_req:header(<<"accept">>, Req0),
%     {ok, Body, _} = cowboy_req:read_body(Req0),

%     case validate_db_headers({database, Db}, {user, DbUser}, {password, DbPass}, {host, DbHost}) of
%         { invalid, Invalid } ->
%             { ok, invalid_request(Req0, lists:flatten(io_lib:format("Missing pg-specific headers: ~p", [lists:map(fun({H,_}) -> H end, Invalid)]))), State};
