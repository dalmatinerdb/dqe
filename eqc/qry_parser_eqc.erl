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
           {ago, time_type()}
          ]).

hfun() ->
    oneof([min, max, avg, mean, median, stddev]).


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
          {aggr, aggr_fun(), Q, aggr_range()},
          {math, multiply, Q, pos_int()},
          {math, divide, Q, pos_int()}
         ])).

significant_figures() ->
    choose(1,5).

hist_tree(Size) ->
    ?LETSHRINK(
       [Q], [qry_tree(Size - 1)],
       ?LET(H, {histogram, pos_int(), significant_figures(), Q, aggr_range()},

            oneof([
                   {hfun, hfun(), H},
                   {hfun, percentile, H, percentile()}
                   ]))).

qry_tree(S) when S < 1->
    oneof([
           {get, bm()},
           {sget, glob_bm()},
           {lookup, lookup()}
          ]);

qry_tree(Size) ->
    ?LAZY(frequency(
            [{1, comb_tree(Size)},
             {1, hist_tree(Size)},
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

lookup() ->
    ?SIZED(S, lookup(S)).

lookup(0) ->
    {in, bucket(), metric()};
lookup(S) ->
    {in, bucket(), metric(), where_clause(S)}.

tag() ->
    frequency(
      [{10, {tag, non_empty_binary(), non_empty_binary()}},
       {1,  {tag, <<>>, non_empty_binary()}}]).

where_clause(S) when S =< 1 ->
    {'=', tag(), non_empty_binary()};
where_clause(S) ->
    ?LAZY(?LET(N, choose(0, S - 1), where_clause_choice(N, S))).

where_clause_choice(N, S) ->
    oneof([{'and', where_clause(N), where_clause(S - N)},
           {'or', where_clause(N), where_clause(S - N)}]).

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

prop_prepare() ->
    ?FORALL(T, qry_tree(),
            begin
                Unparsed = ?P:unparse(T),
                case ?P:prepare(Unparsed) of
                    {ok, _} ->
                        true;
                    {error, E} ->
                        io:format(user, "   ~p~n-> ~p~n-> ~p~n",
                                  [T, Unparsed, E]),
                        false
                end
            end).

prop_dflow_prepare() ->
    ?SETUP(fun mock/0,
           ?FORALL(T, qry_tree(),
                   begin
                       Unparsed = ?P:unparse(T),
                       case dqe:prepare(Unparsed) of
                           {ok, _} ->
                               true;
                           {error, E} ->
                               io:format(user, "   ~p~n-> ~p~n-> ~p~n",
                                         [T, Unparsed, E]),
                               false
                       end
                   end)).

prop_glob_match() ->
    ?FORALL({S, G, M}, glob(),
            begin
                M ==  ([S] == ?P:glob_match(G, [S]))
            end).

mock() ->
    application:ensure_all_started(dqe_connection),
    meck:new(dqe, [passthrough]),
    meck:expect(dqe, glob_match,
                fun(_Glob, Metrics) ->
                        {ok, Metrics}
                end),
    meck:new(ddb_connection),
    meck:expect(ddb_connection, list,
                fun (_) ->
                        {ok, [dproto:metric_from_list([<<"a">>])]}
                end),
    meck:expect(ddb_connection, list,
                fun (_, Prefix) ->
                        P1 = dproto:metric_to_list(Prefix),
                        {ok, [dproto:metric_from_list(P1 ++ [<<"a">>])]}
                end),

    fun unmock/0.

unmock() ->
    meck:unload(ddb_connection),
    meck:unload(dqe),
    ok.
