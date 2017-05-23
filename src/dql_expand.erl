%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@project-fifo.net>
%%% @copyright (C) 2016, Heinz Nikolaus Gies
%%% @doc
%%% Expand query parts
%%% @end
%%% Created :  2 Aug 2016 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(dql_expand).

-export([expand/4]).

expand(Qs, Start, End, Opts) ->
    lists:flatten([expand_grouped(Q, Start, End, [], Opts) || Q <- Qs]).

expand_grouped({calc, Chain, #{op := group_by, args := [L, G, Fun]}},
               Start, End, Groupings, Opts) ->
    Groupings1 = Groupings ++ G,
    R = expand_grouped(L, Start, End, Groupings1, Opts),
    R1 = combine_groupings(R, G, Fun),
    [{calc, Chain, E} || E <- R1];

expand_grouped(Q = #{op := events}, Start, End, _, _Opts) ->
    [Q#{ranges => [{Start, End, default}]}];
expand_grouped(Q = #{op := named, args := [L, M, S]},
               Start, End, Groupings, Opts)
  when is_list(L) ->
    MGs = [N || {_, {dvar, N}} <- M],
    Gs = [N || {dvar, N} <- L],
    [Q#{args => [L, M, S1]} ||
        S1 <- expand_grouped(S, Start, End, Gs ++ MGs ++  Groupings, Opts)];

expand_grouped(Q = #{op := named, args := [N, M, S]},
               Start, End, Groupings, Opts) ->
    MGs = [D || {_, {dvar, D}} <- M],
    [Q#{args => [N, M, S1]} ||
        S1 <- expand_grouped(S, Start, End, MGs ++ Groupings, Opts)];

expand_grouped(Q = #{op := timeshift, args := [T, S]},
               Start, End, Groupings, Opts) ->
    [Q#{args => [T, S1]} ||
        S1 <- expand_grouped(S, Start, End, Groupings, Opts)];

expand_grouped({calc, Fs, Q}, Start, End, Groupings, Opts) ->
    [{calc, Fs, Q1} || Q1 <- expand_grouped(Q, Start, End, Groupings, Opts)];

expand_grouped({combine, F, Qs}, Start, End, Groupings, Opts) ->
    [{combine, F, lists:flatten([expand_grouped(Q, Start, End, Groupings, Opts)
                                 || Q <- Qs])}];

expand_grouped(Q = #{op := get}, Start, End, _Groupings, _Opts) ->
    [Q#{ranges => [{Start, End, default}]}];

expand_grouped(Q = #{op := lookup,
                     args := [Collection, Metric, Where]},
               Start, End, [], Opts) ->
    {ok, BMs} = dqe_idx:lookup({in, Collection, Metric, Where},
                               Start, End, Opts),
    expand_lookup(Q, BMs, []);

expand_grouped(Q = #{op := lookup,
                     args := [Collection, Metric, Where]},
               Start, End, Groupings, Opts) ->
    Groupings1 = lists:usort(Groupings),
    {ok, BMs} = dqe_idx:lookup({in, Collection, Metric, Where},
                               Start, End, Groupings1, Opts),
    expand_lookup(Q, BMs, Groupings1);

expand_grouped(Q = #{op := lookup,
                     args := [Collection, Metric]}, Start, End, [], Opts) ->
    {ok, BMs} = dqe_idx:lookup({in, Collection, Metric},
                               Start, End, Opts),
    expand_lookup(Q, BMs, []);

expand_grouped(Q = #{op := lookup,
                     args := [Collection, Metric]},
               Start, End, Groupings, Opts) ->
    Groupings1 = lists:usort(Groupings),
    {ok, BMs} = dqe_idx:lookup({in, Collection, Metric},
                               Start, End, Groupings1, Opts),
    expand_lookup(Q, BMs, Groupings1);

expand_grouped(Q = #{op := sget,
                     args := [Bucket, Glob]}, Start, End, _Groupings, _Opts) ->
    %% Glob is in an extra list since expand is build to use one or more
    %% globs
    {ok, {_Bucket, Ms}} = dqe_idx:expand(Bucket, [Glob]),
    Q1 = Q#{op := get},
    [Q1#{args => [Bucket, Key],
         ranges => [{Start, End, default}]} || Key <- Ms].

expand_lookup(Q, BMs, []) ->
    Q1 = Q#{op := get},
    [Q1#{args => [Bucket, Key],
         ranges => Ranges} || {Bucket, Key, Ranges} <- BMs];

expand_lookup(Q, BMs, Groupings) ->
    Q1 = Q#{op := get},
    [Q1#{args => [Bucket, Key], groupings => lists:zip(Groupings, GVs),
         ranges => Ranges}
     || {{Bucket, Key, Ranges}, GVs} <- BMs].

combine_groupings(Rs, Groupings, Fun) ->
    Rs1 = [append_values(R, Groupings) || R <- Rs],
    Rs2 = lists:foldl(fun ({K, E}, Acc) ->
                              orddict:append(K, {calc, [], E}, Acc)
                      end, orddict:new(), Rs1),
    [{combine, Fun, E} || {_, E} <- Rs2].

append_values(E = #{
                groupings := Values
               }, Gs) ->
    S = orddict:from_list(Values),
    {[orddict:fetch(G, S) || G <- Gs], E}.
