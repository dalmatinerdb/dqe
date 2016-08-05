-module(qry_parser_eqc).

-include_lib("eqc/include/eqc.hrl").
-compile(export_all).

-define(P, dql).

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
           ])}.

aggr_fun() ->
    oneof([sum, min, max, avg]).

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
          comb(Q)
%%          {math, multiply, Q, pos_int()},
%%          {math, divide, Q, pos_int()}
         ])).


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
    [bucket(), metric()];
lookup(S) ->
    [bucket(), metric(), where_clause(S)].

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

prop_query_parse_unparse() ->
    ?FORALL(T, select_stmt(),
            begin
                Unparsed = dql_unparse:unparse(T),
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
    ?SETUP(fun mock/0,
           ?FORALL(T, select_stmt(),
                   begin
                       Unparsed = dql_unparse:unparse(T),
                       case ?P:prepare(Unparsed) of
                           {ok, _, _} ->
                               true;
                           {error, E} ->
                               io:format(user, "   ~p~n-> ~p~n-> ~p~n",
                                         [T, Unparsed, E]),
                               false
                       end
                   end)).

prop_dflow_prepare() ->
    ?SETUP(fun mock/0,
           ?FORALL(T, select_stmt(),
                   begin
                       Unparsed = dql_unparse:unparse(T),
                       case dqe:prepare(Unparsed) of
                           {ok, _, _} ->
                               true;
                           {error, E} ->
                               io:format(user, "   ~p~n-> ~p~n-> ~p~n",
                                         [T, Unparsed, E]),
                               false
                       end
                   end)).

mock() ->
    application:ensure_all_started(dqe_connection),
    meck:new(ddb_connection),
    meck:expect(ddb_connection, list,
                fun (_) ->
                        M = [<<"a">>, <<"b">>, <<"c">>],
                        {ok, [dproto:metric_from_list(M)]}
                end),
    meck:expect(ddb_connection, resolution,
                fun (_) ->
                        {ok, 1000}
                end),
    meck:expect(ddb_connection, list,
                fun (_, Prefix) ->
                        P1 = dproto:metric_to_list(Prefix),
                        {ok, [dproto:metric_from_list(P1 ++ [<<"a">>])]}
                end),
    ensure_dqe_fun(),
    meck:new(dqe_idx, [passthrough]),
    meck:expect(dqe_idx, lookup,
                fun (_, _) ->
                        {ok, [{<<"a">>, <<"a">>, [<<"a">>]}]}
                end),

    fun unmock/0.

unmock() ->
    meck:unload(ddb_connection),
    meck:unload(dqe_idx),
    ok.

ensure_dqe_fun() ->
    try
        dqe_fun:init(),
        dqe:init()
    catch
        _:_ ->
            ok
    end,
    fun() ->
            ok
    end.
