-module(qry_parser_eqc).

-include_lib("eqc/include/eqc.hrl").
-compile(export_all).

-define(P, dql).

non_empty_string() ->
    ?SUCHTHAT(L, list(choose($a, $z)), length(L) >= 2).

diff_strings() ->
    ?SUCHTHAT({S1, S2}, {non_empty_string(), non_empty_string()}, S1 =/= S2).

non_empty_binary() ->
    ?LET(L, non_empty_string(), list_to_binary(L)).

pos_int() ->
    ?SUCHTHAT(N, int(), N > 0).

time_unit() ->
    oneof([ms, s, m, h, d, w]).

time_type() ->
    {time, pos_int(), time_unit()}.

aggr_range() ->
    oneof([time_type(), pos_int()]).

non_empty_list(T) ->
    ?SUCHTHAT(L, list(T), L /= []).


qry_tree() ->
    oneof([
           {select,
            non_empty_list(?SIZED(Size, qry_tree(Size))),
            oneof([
                   {last, 60},
                   {between, 60, 10},
                   {before, 60, 10},
                   {'after', 60, 10}
                  ]),
            time_type()}
           ]).


percentile() ->
    ?SUCHTHAT(N, real(), N > 0 andalso N =< 1).

qry_tree(Size) ->
    ?LAZY(oneof(
            [
             oneof([
                    {get, bm()},
                    {mget, sum, glob_bm()},
                    {mget, avg, glob_bm()}
                   ]) || Size == 0] ++
                [?LETSHRINK(
                    [Q], [qry_tree(Size - 1)],
                    oneof(
                      [
                       {aggr, derivate, Q},
                       {aggr, percentile, Q, percentile(), aggr_range()},
                       {aggr, min, Q, aggr_range()},
                       {aggr, max, Q, aggr_range()},
                       {aggr, sum, Q, aggr_range()},
                       {aggr, avg, Q, aggr_range()},
                       {math, multiply, Q, pos_int()},
                       {math, divide, Q, pos_int()}
                      ])) || Size > 0]
           )).

bucket() ->
    non_empty_binary().

metric() ->
    non_empty_list(non_empty_binary()).

glob_metric() ->
    non_empty_list(oneof([non_empty_binary(),'*'])).

glob_bm() ->
    {bucket(), glob_metric()}.

bm() ->
    {bucket(), metric()}.

glob() ->
    ?LET({S, G, M}, ?SIZED(Size, glob(Size)),
         {list_to_binary(string:join(S, ".")),
          list_to_binary(string:join(G, ".")),
          M}).

glob(Size) ->
    ?LAZY(oneof([?LET(S, non_empty_string(), {[S], [S], true}) || Size == 0] ++
                    [add(Size) || Size > 0])).

add(Size) ->
    frequency(
      [{1, diff_element(Size)},
       {10, matching_element(Size)},
       {10, glob_element(Size)}]).

matching_element(Size) ->
    ?LAZY(
       ?LET(
          {L, G, M}, glob(Size-1),
          ?LET(S, non_empty_string(),
               {[S | L], [S | G], M}))).

diff_element(Size) ->
    ?LAZY(
       ?LET(
          {L, G, _M}, glob(Size-1),
          ?LET({S1, S2}, diff_strings(),
               {[S1 | L], [S2 | G], false}))).

glob_element(Size) ->
    ?LAZY(
       ?LET(
          {L, G, M}, glob(Size-1),
          {[non_empty_string() | L], ["*" | G], M})).

prop_qery_parse_unparse() ->
    ?FORALL(T, qry_tree(),
            begin
                Unparsed = ?P:unparse(T),
                {ok, ReParsed} = ?P:parse(Unparsed),
                ?WHENFAIL(
                   io:format(user, "   ~p~n-> ~p~n-> ~p~n",
                             [T, Unparsed, ReParsed]),
                   T == ReParsed)
            end).

prop_glob_match() ->
    ?FORALL({S, G, M}, glob(),
            begin
                M ==  ([S] == ?P:glob_match(G, [S]))
            end).
