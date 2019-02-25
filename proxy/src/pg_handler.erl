-module(pg_handler).
-behavior(cowboy_handler).

-export([init/2, info/3]).


init(Req0=#{method := <<"QUERY">>,version := 'HTTP/2'} , State) ->
    process_flag(trap_exit, true),
    Db =     cowboy_req:header(<<"database">>, Req0, <<>>),
    DbUser = cowboy_req:header(<<"user">>, Req0, <<>>),
    DbPass = cowboy_req:header(<<"password">>, Req0, <<>>),
    DbHost = cowboy_req:header(<<"dbhost">>, Req0, <<"localhost">>),
    DbPort = cowboy_req:header(<<"dbport">>, Req0, <<"5432">>),
    Accept = cowboy_req:header(<<"accept">>, Req0),

    case validate_db_headers({database, Db}, {user, DbUser}, {password, DbPass}, {host, DbHost}) of
        { invalid, Invalid } ->
            { ok, invalid_request(Req0, lists:flatten(io_lib:format("Missing pg-specific headers: ~p", [lists:map(fun({H,_}) -> H end, Invalid)]))), State};

        _ ->
            try epgsql:connect(binary_to_list(DbHost), DbUser, DbPass, #{ database => Db, timeout => 1000, port => list_to_integer(binary_to_list(DbPort))}) of
                {error, Reason} ->
                    Rep = invalid_request(Req0, io_lib:format("Connection failed ~p~n",[Reason])),
                    {ok, Rep, State};

                {ok,C} ->
                    case epgsql_sock:sync_command(C, epgsql_cmd_squery_raw, "select CURRENT_TIMESTAMP") of
                        {ok, Hdr, Body} ->
                            Rep = cowboy_req:reply(200,
                                #{<<"content-type">> => <<"text/plain">>},
                                begin
                                    R = lists:flatten(io_lib:format("~p~p", [Hdr, Body])),
                                    case Accept of
                                        <<"text/hex">> -> io_lib:format("~p~n", [to_hex(list_to_binary(R))]);
                                        _ -> R
                                    end
                                end,
                                Req0);
                        {error, Err } ->
                            Rep = invalid_request(Req0, io_lib:format("~p~n", [Err]))
                    end,
                    ok = epgsql:close(C),

                    {ok, Rep, State}

            catch
                exit:_ -> {ok, invalid_request(Req0, "Epgsql exited."), State};
                _:_ -> {ok, invalid_request(Req0, "Epgsql threw an exception."), State}
            end
    end;


init(Req0,State) ->
    {ok, invalid_request(Req0), State }.


info({'EXIT', Pid, Reason}, Req, State) ->
    io:fwrite("Pid ~p Closed. Reason ~p", [Pid, Reason]),
    {ok, invalid_request(Req), State};

info(_Msg, Req, State) ->
    {ok, Req, State, hibernate}.


validate_db_headers(D, U, P, H) ->
    case lists:filter(fun({_,HDR}) -> string:length(HDR) =:= 0 end, [D,U,P,H]) of
        Invalid when length(Invalid) > 0 -> { invalid, Invalid };
        _ -> ok
    end.


invalid_request(R) ->
    cowboy_req:reply(400,
        #{ <<"content-type">> => <<"text/plain">> },
        <<"Invalid request.">>,
        R).


invalid_request(R, Msg) ->
    cowboy_req:reply(400,
        #{ <<"content-type">> => <<"text/plain">> },
        Msg,
        R).

to_hex(Bin) when is_binary(Bin) ->
    << <<(hex(H)),(hex(L))>> || <<H:4,L:4>> <= Bin >>.

hex(C) when C < 10 -> $0 + C;
hex(C) -> $a + C - 10.