%% Squery may contain many semicolon-separated queries
%% > Query
%% < (RowDescription?
%% <  DataRow*
%% <  CommandComplete)+
%% < ReadyForQuery
%% ---
%% > Query when len(strip(Query)) == 0
%% < EmptyQueryResponse
%% < ReadyForQuery
-module(pg_http_handle_query).
%%-behavior(pg_http_verb).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-type response_single() ::
        {ok, Count :: non_neg_integer(), Cols :: [epgsql:column()], RawCols :: [binary()], Rows :: [tuple()]}
      | {ok, Count :: non_neg_integer()}
      | {ok, Cols :: [epgsql:column()], RawCols :: [binary()], Rows :: [tuple()]}
      | {error, epgsql:query_error()}.
-type response() :: response_single() | [response_single()].

-include("protocol.hrl").

-record(squery,
        {query :: iodata(),
         columns = [],
         raw_columns :: binary(),
         decoder}).

init(Sql) ->
    #squery{query = Sql}.

execute(Sock, #squery{query = Q} = State) ->
    epgsql_sock:send(Sock, ?SIMPLEQUERY, [Q, 0]),
    {ok, Sock, State}.

handle_message(?ROW_DESCRIPTION, <<Count:?int16, Bin/binary>> = RawCols, Sock, State) ->
    Codec = epgsql_sock:get_codec(Sock),
    Columns = epgsql_wire:decode_columns(Count, Bin, Codec),
    Decoder = epgsql_wire:build_decoder(Columns, Codec),
    epgsql_sock:notify(Sock, {columns, Columns}),
    {noaction, Sock, State#squery{columns = Columns,
                                  raw_columns = [?ROW_DESCRIPTION| binary_to_list(RawCols)],
                                  decoder = Decoder}};
handle_message(?DATA_ROW, <<_Count:?int16, _Bin/binary>> = RawRow, Sock, St) ->
    Row = {[?DATA_ROW | binary_to_list(RawRow)]},
    {add_row, Row, Sock, St};
handle_message(?COMMAND_COMPLETE, Bin, Sock, #squery{columns = Cols, raw_columns = RawCols} = St) ->
    Complete = epgsql_wire:decode_complete(Bin),
    Rows = epgsql_sock:get_rows(Sock),
    Result = case Complete of
                 {_, Count} when Cols == [] ->
                     {ok, Count};
                 {_, Count} ->
                     {ok, Count, Cols, RawCols, Rows};
                 _ ->
                     {ok, Cols, RawCols, Rows}
             end,
    {add_result, Result, {complete, Complete}, Sock, St};
handle_message(?EMPTY_QUERY, _, Sock, St) ->
    {add_result, {ok, [], []}, {complete, empty}, Sock, St};
handle_message(?READY_FOR_QUERY, _Status, Sock, _State) ->
    %% We return single result if there is only one or list of results if
    %% there are more than one
    Result = case epgsql_sock:get_results(Sock) of
                 [Res] -> Res;
                 Res -> Res
             end,
    {finish, Result, done, Sock};
handle_message(?ERROR, Error, Sock, St) ->
    Result = {error, Error},
    {add_result, Result, Result, Sock, St};
handle_message(_, _, _, _) ->
    unknown.
