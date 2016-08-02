%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@project-fifo.net>
%%% @copyright (C) 2016, Heinz Nikolaus Gies
%%% @doc
%%% Function resolver logic
%%% @end
%%% Created :  2 Aug 2016 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(dql_resolver).
-export([resolve/1]).

resolve(Qs) ->
    resolve_functions(Qs, []).

%%--------------------------------------------------------------------
%% @private
%% @doc Resolves functions based on their parameter types
%% @end
%%--------------------------------------------------------------------
resolve_functions(#{op := named, args := [N, Q], return := undefined}) ->
    case resolve_functions(Q) of
        {ok, R = #{return := T}} ->
            {ok, #{
               op => named,
               args => [N, R],
               signature => [string, T],
               return => T
              }
            };
        E ->
            E
    end;

resolve_functions(#{op := timeshift, args := [S, Q]}) ->
    case resolve_functions(Q) of
        {ok, R = #{return := T}} ->
            {ok, #{
               op => timeshift,
               args => [S, R],
               signature => [integer, T],
               return => T
              }
            };
        E ->
            E
    end;

resolve_functions(#{op := fcall, args := #{name   := Function,
                                           inputs := Args}}) ->
    case resolve_functions(Args, []) of
        {ok, Args1} ->
            %% Determine the type of each argument
            Types = [T || #{return := T} <- Args1],
            %% Decide wather an argument is a constant (passed to init)
            %% or a nested function
            Args2 = [{C, is_constant(T)} || C = #{return := T} <- Args1],
            Constants = [C || {C, true} <- Args2],
            Inputs = [C || {C, false} <- Args2],
            %% Lookup if we know a function with the given types
            case dqe_fun:lookup(Function, Types) of
                %% If we find a function that does not take a
                %% list of sub functions we know this is a normal
                %%aggregate.
                {ok,{{_, _, none}, ReturnType, FunMod}} ->
                    FArgs = #{name      => Function,
                              orig_args => Args,
                              mod       => FunMod,
                              inputs    => Inputs,
                              constants => Constants},
                    {ok, #{
                       op => fcall,
                       args => FArgs,
                       signature => Types,
                       return => ReturnType
                      }};
                %% If we find one that takes a list of sub functions
                %% we know it is a combinator function.
                {ok,{{_, _, _}, ReturnType, FunMod}} ->
                    FArgs = #{name      => Function,
                              orig_args => Args,
                              mod       => FunMod,
                              inputs    => Inputs,
                              constants => Constants},
                    {ok, #{
                       op => combine,
                       args => FArgs,
                       signature => Types,
                       return => ReturnType
                      }};
                {error, not_found} ->
                    {error, {not_found, Function, Types}}
            end;
        E ->
            E
    end;
%% Thse are constatns, we don't need to resolve any further.
resolve_functions(N) when is_integer(N) ->
    {ok, #{op => integer, args => [N], return => integer}};
resolve_functions(N) when is_float(N) ->
    {ok, #{op => float, args => [N], return => float}};
resolve_functions(#{} = R) ->
    {ok, R}.

resolve_functions([], Acc) ->
    {ok, lists:reverse(Acc)};
resolve_functions([A | R], Acc) ->
    case resolve_functions(A) of
        {ok, T} ->
            resolve_functions(R, [T | Acc]);
        E ->
            E
    end.


%%--------------------------------------------------------------------
%% @doc Determines if a type is a constant or sub function.
%% @end
%%--------------------------------------------------------------------
is_constant(metric) -> false;
is_constant(metric_list) -> false;
is_constant(histogram) -> false;
is_constant(histogram_list) -> false;
is_constant(_) -> true.
