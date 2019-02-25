-module(pg_handler).
-behavior(cowboy_handler).

-export([init/2]).




init(Req0=#{method := <<"QUERY">>,version := 'HTTP/2'} , State) ->
    Db =     cowboy_req:header(<<"database">>, Req0, <<>>),
    DbUser = cowboy_req:header(<<"user">>, Req0, <<>>),
    DbPass = cowboy_req:header(<<"password">>, Req0, <<>>),
    DbHost = cowboy_req:header(<<"dbhost">>, Req0, <<"localhost">>),
    DbPort = cowboy_req:header(<<"dbport">>, Req0, <<"5432">>),

    case validate_db_headers({database, Db}, {user, DbUser}, {password, DbPass}, {host, DbHost}) of
        { invalid, Invalid } ->
            { ok, invalid_request(Req0, lists:flatten(io_lib:format("Missing pg-specific headers: ~p", [lists:map(fun({H,_}) -> H end, Invalid)]))), State};

        _ ->
            try epgsql:connect(binary_to_list(DbHost), DbUser, DbPass, #{ database => Db, timeout => 1000, port => list_to_integer(binary_to_list(DbPort))}) of
                {error, Reason} ->
                    Rep = invalid_request(Req0, io_lib:format("Connection failed ~p~n",[Reason])),
                    {ok, Rep, State};

                {ok,C} ->
                    io:format("db pid ~p~n",[C]),
                    Qry = epgsql:squery(C, "select CURRENT_TIMESTAMP"),

                    io:format("Qry DB return ~p~n",[Qry]),
                    {_,_,[{C1}]}= Qry,

                    ok = epgsql:close(C),

                    Rep = cowboy_req:reply(200,
                        #{<<"content-type">> => <<"text/plain">>},
                        C1,
                        Req0),
                    {ok, Rep, State};
                _ -> {ok, invalid_request(Req0, "Epgsql threw an exception."), State}
            catch
                exit:_ -> {ok, invalid_request(Req0, "Epgsql threw an exception."), State};
                _:_ -> {ok, invalid_request(Req0, "Epgsql threw an exception."), State}
            end
    end;


init(Req0,State) ->
    {ok, invalid_request(Req0), State }.


validate_db_headers(D, U, P, H) ->
    case lists:filter(fun({_,HDR}) -> string:length(HDR) =:= 0 end, [D,U,P,H]) of
        Invalid when length(Invalid) > 0 -> { invalid, Invalid };
        _ -> ok
    end.


invalid_request(R) ->
    cowboy_req:reply(400,
        #{ <<"content-type">> => <<"text/plain">> },
        <<"Invalid request you made.">>,
        R).


invalid_request(R, Msg) ->
    cowboy_req:reply(400,
        #{ <<"content-type">> => <<"text/plain">> },
        Msg,
        R).