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

rel_time() ->
    oneof([
           pos_int(),
           now,
           {ago, pos_int()}
          ]).


qry_tree() ->
    {select,
     non_empty_list(?SIZED(Size, qry_tree(Size))),
     oneof([
            {last, pos_int()},
            {between, rel_time(), pos_int()},
            {before, rel_time(), pos_int()},
            {'after', pos_int(), pos_int()}
           ]),
     time_type()}.

comb_fun() ->
    oneof([sum, avg]).
aggr_fun() ->
    oneof([sum, min, max, avg]).

percentile() ->
    ?SUCHTHAT(N, real(), N > 0 andalso N =< 1).

comb_tree(Size) ->
    ?LETSHRINK(
       [QL, QR], [qry_tree(Size - 1), qry_tree(Size - 1)],
       {combine, comb_fun(), [QL, QR]}).
aggr_tree(Size) ->
    ?LETSHRINK(
       [Q], [qry_tree(Size - 1)],
       oneof(
         [
          {combine, comb_fun(), [Q]},
          {aggr, derivate, Q},
          {aggr, percentile, Q, percentile(), aggr_range()},
          {aggr, aggr_fun(), Q, aggr_range()},
          {math, multiply, Q, pos_int()},
          {math, divide, Q, pos_int()}
         ])).

qry_tree(0) ->
    oneof([
           {get, bm()},
           {sget, glob_bm()}
          ]);

qry_tree(Size) ->
    ?LAZY(frequency(
            [{1, comb_tree(Size)},
             {10, aggr_tree(Size)}]
           )).

bucket() ->
    non_empty_binary().

metric() ->
    non_empty_list(non_empty_binary()).

glob_metric() ->
    ?SUCHTHAT(L,
              non_empty_list(oneof([non_empty_binary(),'*'])),
              lists:member('*', L)).

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
                case ?P:parse(Unparsed) of
                    {ok, ReParsed} ->
                        ?WHENFAIL(
                           io:format(user, "   ~p~n-> ~p~n-> ~p~n",
                                     [T, Unparsed, ReParsed]),
                           T == ReParsed);
                    {error, E} ->
                        io:format(user, "   ~p~n-> ~p~n-> ~p~n",
                                  [T, Unparsed, E]),
                        false
                end
            end).

prop_glob_match() ->
    ?FORALL({S, G, M}, glob(),
            begin
                M ==  ([S] == ?P:glob_match(G, [S]))
            end).
