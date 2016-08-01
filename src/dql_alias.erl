-module(dql_alias).

-export([expand/2]).

%%--------------------------------------------------------------------
%% @doc Expand aliases in all query parts by replacing them with 
%% actual selectors
%% @end
%%--------------------------------------------------------------------
-spec expand([dql:statement()], [term()]) ->
                    {error, term()} |
                    {ok, [dql:statement()]}.
expand(Qs, Aliases) ->
    AliasesF =
        lists:foldl(fun({alias, Alias, Res}, AAcc) ->
                            gb_trees:enter(Alias, Res, AAcc)
                    end, gb_trees:empty(), Aliases),
    dqe_lib:pdebug('parse', "Aliases resolved.", []),
    resolve(Qs, AliasesF).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc Resolves the aliases.
%% @end
%%--------------------------------------------------------------------
-spec resolve([dql:statement()], gb_trees:tree()) ->
                        {error, term()} |
                        {ok, [dql:statement()]}.
resolve(Qs, Aliases) ->
    {QQ, _AliasesQ} =
        lists:foldl(fun(Q, {QAcc, AAcc}) ->
                            {Q1, A1} = resolve_statement(Q, AAcc),
                            {[Q1 | QAcc], A1}
                    end, {[], Aliases}, Qs),
    dqe_lib:pdebug('parse', "Preprocessor done.", []),
    {ok, lists:reverse(QQ)}.

%%--------------------------------------------------------------------
%% @private
%% @doc Resolves alias in a statement
%% @end
%%--------------------------------------------------------------------
-spec resolve_statement(dql:statement(), gb_trees:tree()) -> 
                               {error, term()} |
                               {term(), gb_trees:tree()}.
resolve_statement(O = #{op  := fcall,
                        args := Args = #{inputs := Input}},
                  Aliases) ->
    {Input1, A1} =
        lists:foldl(fun (Q, {QAcc, AAcc}) ->
                            {Qx, Ax} = resolve_statement(Q, AAcc),
                            {[Qx | QAcc], Ax}
                    end, {[], Aliases}, Input),
    Input2 = lists:reverse(Input1),
    {O#{args => Args#{inputs => Input2}}, A1};
resolve_statement(O = #{op := named, args := [N, Q]}, Aliases) ->
    {Q1, A1} = resolve_statement(Q, Aliases),
    {O#{args => [N, Q1]}, A1};
resolve_statement(#{op := var, args := [V]}, Aliases) ->
    {value, G} = gb_trees:lookup(V, Aliases),
    {G, Aliases};
resolve_statement(O = #{}, Aliases) ->
    {O, Aliases};
resolve_statement(N, A) when is_number(N)->
    {N, A}.
