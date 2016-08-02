%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@project-fifo.net>
%%% @copyright (C) 2016, Heinz Nikolaus Gies
%%% @doc
%%% Naming related logic
%%% @end
%%% Created :  2 Aug 2016 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(dql_naming).

-export([update/1]).

%%--------------------------------------------------------------------
%% @doc Updates names for query parts.
%% @end
%%--------------------------------------------------------------------

update(Qs) ->
    [update_name(Q) || Q <- Qs].

%%%===================================================================
%%% Internal functions
%%%===================================================================

update_name({named, L, C}) when is_list(L)->
    {Path, Gs} = extract_path_and_groupings(C),
    Name = [update_name_element(N, Path, Gs) || N <- L],
    {named, dql_unparse:unparse_metric(Name), C};

update_name({named, _N, _C} = Q) when is_binary(_N) ->
    Q;

update_name({named, _N, _C} = Q) ->
    io:format("Unkown named: ~p~n", [Q]),
    Q.

update_name_element({dvar, N}, _Path, Gs) ->
    {_, Name} = lists:keyfind(N, 1, Gs),
    Name;
update_name_element({pvar, N}, Path, _Gs) ->
    lists:nth(N, Path);
update_name_element(N, _, _) ->
    N.

extract_path_and_groupings(G = #{op := get, args := [_,_,_,_,Path]})
  when is_list(Path) ->
    {Path, extract_groupings(G)};
extract_path_and_groupings(G = #{op := get, args := [_,_,_,_,Path]})
  when is_binary(Path) ->
    {dproto:metric_to_list(Path), extract_groupings(G)};
extract_path_and_groupings({calc, _, G}) ->
    extract_path_and_groupings(G);

%% If we find a combine we take the values of its first element
%% for grouings all elements will have the same anyway and for
%% pvars there is no 'right' answer so picking the first is
%% as good as picking any other.
extract_path_and_groupings({combine, _, [G | _]}) ->
    extract_path_and_groupings(G).

extract_groupings(#{groupings := Gs}) ->
    Gs;
extract_groupings(_) ->
    [].
