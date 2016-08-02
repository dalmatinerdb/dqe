%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@project-fifo.net>
%%% @copyright (C) 2016, Heinz Nikolaus Gies
%%% @doc
%%% Expand query parts
%%% @end
%%% Created :  2 Aug 2016 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(dql_expand).

-export([expand/1]).

expand(Qs) ->
    [expand_grouped(Q, []) || Q <- Qs].

expand_grouped(Q = #{op := named, args := [L, S]}, Groupings) when is_list(L) ->
    Gs = [N || {dvar, N} <- L],
    [Q#{args => [L, S1]} || S1 <- expand_grouped(S, Gs ++  Groupings)];

expand_grouped(Q = #{op := named, args := [N, S]}, Groupings) ->
    [Q#{args => [N, S1]} || S1 <- expand_grouped(S, Groupings)];

expand_grouped(Q = #{op := timeshift, args := [T, S]}, Groupings) ->
    [Q#{args => [T, S1]} || S1 <- expand_grouped(S, Groupings)];

expand_grouped({calc, Fs, Q}, Groupings) ->
    [{calc, Fs, Q1} || Q1 <- expand_grouped(Q, Groupings)];

expand_grouped({combine, F, Qs}, Groupings) ->
    [{combine, F, lists:flatten([expand_grouped(Q, Groupings) || Q <- Qs])}];

expand_grouped(Q = #{op := get}, _Groupings) ->
    [Q];

expand_grouped(Q = #{op := lookup,
             args := [Collection, Metric, Where]}, []) ->
    {ok, BMs} = dqe_idx:lookup({in, Collection, Metric, Where}),
    Q1 = Q#{op := get},
    [Q1#{args => [Bucket, Key]} || {Bucket, Key} <- BMs];

expand_grouped(Q = #{op := lookup,
             args := [Collection, Metric, Where]}, Groupings) ->
    {ok, BMs} = dqe_idx:lookup({in, Collection, Metric, Where}, Groupings),
    Q1 = Q#{op := get},
    [Q1#{args => [Bucket, Key], groupings => lists:zip(Groupings, GVs)}
     || {Bucket, Key, GVs} <- BMs];

expand_grouped(Q = #{op := lookup,
             args := [Collection, Metric]}, []) ->
    {ok, BMs} = dqe_idx:lookup({in, Collection, Metric}),
    Q1 = Q#{op := get},
    [Q1#{args => [Bucket, Key]} || {Bucket, Key} <- BMs];

expand_grouped(Q = #{op := lookup,
             args := [Collection, Metric]}, Groupings) ->
    {ok, BMs} = dqe_idx:lookup({in, Collection, Metric}, Groupings),
    Q1 = Q#{op := get},
    [Q1#{args => [Bucket, Key], groupings => lists:zip(Groupings, GVs)}
     || {Bucket, Key, GVs} <- BMs];

expand_grouped(Q = #{op := sget,
             args := [Bucket, Glob]}, _Groupings) ->
    %% Glob is in an extra list since expand is build to use one or more
    %% globs
    {ok, {_Bucket, Ms}} = dqe_idx:expand(Bucket, [Glob]),
    Q1 = Q#{op := get},
    [Q1#{args => [Bucket, Key]} || Key <- Ms].
