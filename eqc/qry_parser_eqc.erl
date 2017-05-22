-module(qry_parser_eqc).

-include_lib("eqc/include/eqc.hrl").

-import(dqe_helper, [select_stmt/0]).

-compile(export_all).

-define(P, dql).

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
                       case ?P:prepare(Unparsed, []) of
                           {ok, _, _, _} ->
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
                       case dqe:prepare(Unparsed, []) of
                           {ok, _, _, _} ->
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
                fun (_Q, Start, End, _Opts, _G) ->
                        {ok, [{{<<"a">>, <<"a">>, [{Start, End, default}]},
                               [<<"a">>]}]}
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
