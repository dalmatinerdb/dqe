-module(dqe_helper).

-include_lib("eqc/include/eqc.hrl").

-export([glob/0, where_clause/1, lookup/0, tag/0, bm/0, glob_bm/0,
         glob_metric/0, metric/0, bucket/0, qry_tree/1, maybe_named/1,
         maybe_shifted/1, named_element/0, dvar/0, pvar/0, sget_f/0, get_f/0,
         comb/1, comb/2, aggr1/1, aggr2/1, arith/1, aggr_tree/1, hist_tree/1,
         comb_tree/1, percentile/0, select_stmt/0, aliases/0, rel_time/0]).

str() ->
    list(choose($a, $z)).

str_bin() ->
    ?LET(S, list(choose($a, $z)), list_to_binary(S)).

non_empty_string() ->
    ?SUCHTHAT(L, str(), length(L) >= 2).

diff_strings() ->
    ?SUCHTHAT({S1, S2}, {non_empty_string(), non_empty_string()}, S1 =/= S2).

non_empty_binary() ->
    ?LET(L, non_empty_string(), list_to_binary(L)).

pos_int() ->
    ?SUCHTHAT(N, int(), N > 0).

pos_real() ->
    ?SUCHTHAT(R, real(), R > 0).

time_unit() ->
    oneof([ms, s, m, h, d, w]).

time_type() ->
    #{op => time,
      return => time,
      signature => [integer,time_unit],
      args => [pos_int(), time_unit()]}.

aggr_range() ->
    time_type().

non_empty_list(T) ->
    ?SUCHTHAT(L, list(T), L /= []).

rel_time() ->
    oneof([
           pos_int(),
           now,
           #{op => ago, args => [time_type()]}
          ]).

hfun() ->
    oneof([min, max, avg, mean, median, stddev]).


aliases() ->
    [].

select_stmt() ->
    {select,
     non_empty_list(?SIZED(Size, maybe_named(Size))),
     aliases(),
     oneof([
            #{op => last, args => [pos_int()]},
            #{op => between, args => [rel_time(), pos_int()]},
            #{op => before, args => [rel_time(), pos_int()]},
            #{op => 'after', args => [pos_int(), pos_int()]}
           ]),
    limit()}.

limit() ->
    oneof(
      [undefined,
       {oneof([top, bottom]), pos_int(), aggr2_fun()}]).

percentile() ->
    ?SUCHTHAT(N, real(), N > 0 andalso N =< 1).

comb_tree(Size) ->
    ?LETSHRINK(
       [QL, QR], [qry_tree(Size - 1), qry_tree(Size - 1)],
       comb(QL, QR)).
aggr_tree(Size) ->
    ?LETSHRINK(
       [Q], [qry_tree(Size - 1)],
       oneof(
         [
          aggr1(Q),
          aggr2(Q),
          comb(Q),
          arith(Q)
         ])).

arith_fun() ->
    oneof([<<"add">>, <<"divide">>, <<"mul">>, <<"sub">>]).
arith_const() ->
    oneof([pos_int(), pos_real()]).
arith(Q) ->
    #{op => fcall,
      args => #{name => arith_fun(),
                inputs => [Q, arith_const()]}}.

aggr2_fun() ->
    oneof([<<"avg">>, <<"sum">>, <<"min">>, <<"max">>]).
aggr2(Q) ->
    #{op => fcall,
      args => #{name => aggr2_fun(),
                inputs => [Q, aggr_range()]}}.
aggr1_fun() ->
    oneof([<<"derivate">>]).
aggr1(Q) ->
    #{op => fcall,
      args => #{name => aggr1_fun(),
                inputs => [Q]}}.
comb_fun() ->
    oneof([<<"sum">>]).
comb(Q) ->
    #{op => fcall,
      args => #{name => comb_fun(),
                inputs => [Q]}}.
comb(QL, QR) ->
    #{op => fcall,
      args => #{name => comb_fun(),
                inputs => [QL, QR]}}.


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

get_f() ->
    #{
      op        => get,
      args      => bm(),
      signature => [integer, integer, integer, metric, bucket],
      return    => metric
    }.

sget_f() ->
    #{
      op        => sget,
      args      => glob_bm(),
      signature => [integer, integer, integer, glob, bucket],
      return    => metric
    }.

dvar() ->
    {dvar, {str_bin(), non_empty_binary()}}.

pvar() ->
    {pvar, choose(1, 1)}.

named_element() ->
    oneof([
           non_empty_binary(),
           %%dvar(),
           pvar()
           ]).
named() ->
    ?SUCHTHAT(L, list(named_element()), L =/= []).

maybe_named(S) ->
    oneof([
           qry_tree(S),
           #{
              op   => named,
              args => [named(), qry_tree(S)],
              return => undefined
            }
          ]).

maybe_shifted(S) ->
    oneof([
           qry_tree(S),
           #{
              op   => timeshift,
              args => [time_type(), qry_tree(S)]
            }
          ]).

qry_tree(S) when S < 1->
    oneof([
           get_f(),
           sget_f(),
           lookup()
          ]);

qry_tree(Size) ->
    ?LAZY(frequency(
            [
             %%{1, comb_tree(Size)},
             %%{1, hist_tree(Size)},
             {10, aggr_tree(Size)}]
           )).

bucket() ->
    non_empty_binary().

metric() ->
    non_empty_list(non_empty_binary()).

lqry_metric() ->
    oneof([metric(), undefined]).

glob_metric() ->
    ?SUCHTHAT(L,
              non_empty_list(oneof([non_empty_binary(),'*'])),
              lists:member('*', L)).

glob_bm() ->
    [bucket(), glob_metric()].

bm() ->
    [bucket(), metric()].

lookup() ->
    #{op   => lookup,
      return => metric,
      args => ?SIZED(S, lookup(S))}.

lookup(0) ->
    [bucket(), lqry_metric()];
lookup(S) ->
    [bucket(), lqry_metric(), where_clause(S)].

tag() ->
    frequency(
      [{10, {tag, non_empty_binary(), non_empty_binary()}},
       {1,  {tag, <<>>, non_empty_binary()}}]).

where_clause(S) when S =< 1 ->
    oneof([{'=', tag(), non_empty_binary()},
           {'!=', tag(), non_empty_binary()}]);
where_clause(S) ->
    ?LAZY(?LET(N, choose(0, S - 1), where_clause_choice(N, S))).

where_clause_choice(N, S) ->
    oneof([{'and', where_clause(N), where_clause(S - N)}
           %%,{'or', where_clause(N), where_clause(S - N)}
          ]).

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
