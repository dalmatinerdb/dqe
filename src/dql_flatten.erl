%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@project-fifo.net>
%%% @copyright (C) 2016, Heinz Nikolaus Gies
%%% @doc
%%% Flattens a query AST into sub parts.
%%% @end
%%% Created :  2 Aug 2016 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(dql_flatten).

-export([flatten/1]).

flatten(Qs) ->
    [flatten_(Q) || Q <- Qs].

-spec flatten_(dql:dqe_fun() | dql:get_stmt()) ->
                      #{op => named, args => [binary() | dql:flat_stmt()]}.

flatten_(#{op := named, args := [undefined, M, Child]}) ->
    N = dql_unparse:unparse(Child),
    io:format("N: ~p~n", [N]),
    C = flatten_(Child, []),
    R = get_type(C),
    #{
       op => named,
       args => [N, M, C],
       signature => [string, R],
       return => R
     };
flatten_(#{op := named, args := [N, M, Child]}) ->
    C = flatten_(Child, []),
    R = get_type(C),
    #{
       op => named,
       args => [N, M, C],
       signature => [string, R],
       return => R
     };

flatten_(Child = #{return := R}) ->
    N = dql_unparse:unparse(Child),
    C = flatten_(Child, []),
    #{
       op => named,
       args => [N, [], C],
       signature => [string, R],
       return => R
     }.

-spec flatten_(dql:statement(), [dql:dqe_fun()]) ->
                     dql:flat_stmt().
flatten_(#{op := timeshift, args := [Time, Child]}, Chain) ->
    C = flatten_(Child, Chain),
    #{op => timeshift, args => [Time, C],
      return => get_type(C)};

%% flatten_(#{op := timeshift, args := [_Time, Child]}, []) ->
flatten_(GroupBy = #{op := group_by}, Chain) ->
    {calc, Chain, GroupBy};

flatten_(F = #{op   := fcall,
               args := Args = #{inputs := [Child]}}, Chain) ->
    Args1 = maps:remove(inputs, Args),
    Args2 = maps:remove(orig_args, Args1),
    flatten_(Child, [F#{args => Args2} | Chain]);

flatten_(F = #{op   := combine,
               args := Args = #{inputs := Children}}, Chain) ->
    Args1 = maps:remove(orig_args, Args),
    Args2 = maps:remove(inputs, Args1),
    Children1 = [flatten_(C, []) || C <- Children],
    Comb = {combine, F#{args => Args2}, Children1},
    {calc, Chain, Comb};


flatten_(O = #{op := events}, Chain) ->
    {calc, Chain, O};

flatten_(Get = #{op := get},
         Chain) ->
    {calc, Chain, Get};

flatten_(Get = #{op := lookup},
        Chain) ->
    {calc, Chain, Get};

flatten_(Get = #{op := sget},
        Chain) ->
    {calc, Chain, Get}.

get_type(#{return := R}) ->
    R;
get_type({calc, [], C}) ->
    get_type(C);
get_type({calc, L, _}) when is_list(L) ->
    get_type(lists:last(L));
get_type({combine, F, _}) ->
    get_type(F).
