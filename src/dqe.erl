%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc This is the DalmatinerDB query engine, it is run as part of
%%% the DalmatinerDB frontend but can be embedded in other applications
%%% just as well.
%%%
%%% @end
%%% Created : 14 Dec 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(dqe).

-export([prepare/1, run/1, run/2, error_string/1, init/0]).

-type query_reply() :: [{Name :: binary(),
                         Data :: binary(),
                         Resolution :: pos_integer()}].

-type query_error() :: {'error', 'no_results' |
                        'significant_figures' |
                        'resolution_conflict' |
                        'timeout' |
                        binary() |
                        {not_found, binary(), [atom()]} |
                        {'not_found',{binary(), binary()}}}.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Initializes the query engine by loading the internal functions
%% @end
%%--------------------------------------------------------------------

init() ->
    AggrFuns = [
                dqe_max,
                dqe_min,
                dqe_avg_aggr,
                dqe_sum_aggr,
                dqe_derivate,
                dqe_confidence
               ],
    ArithFuns = [
                 dqe_add_arith,
                 dqe_sub_arith,
                 dqe_divide_arith,
                 dqe_mul_arith
                ],
    CombFuns = [
                dqe_avg_comb,
                dqe_sum_comb,
                dqe_diff_comb,
                dqe_product_comb,
                dqe_quotient_comb,
                dqe_max_comb
               ],
    HistFuns = [
                dqe_hist,
                dqe_hist_max,
                dqe_hist_min,
                dqe_hist_mean,
                dqe_hist_median,
                dqe_hist_percentile,
                dqe_hist_stddev
               ],
    AllFuns = AggrFuns ++ ArithFuns ++ CombFuns ++ HistFuns,
    [dqe_fun:reg(F) || F <- AllFuns].

%%--------------------------------------------------------------------
%% @doc Translates an error into a readable string.
%% @end
%%--------------------------------------------------------------------

-spec error_string(query_error()) -> string().
error_string({error, {not_found, {var, Name}}}) ->
    ["Variable '", Name, "' referenced but not defined!"];

error_string({error, {not_found, {glob, Glob}}}) ->
    ["No series matches ", dqe_lib:glob_to_string(Glob), "!"];

error_string({error, no_results}) ->
    "No results were returned for the query.";

error_string({error, resolution_conflict}) ->
    "Combination functions can't have mix resolutions as children.";

error_string({error, A}) when is_atom(A) ->
    atom_to_list(A);

error_string({error, B}) when is_binary(B) ->
    binary_to_list(B).

%%--------------------------------------------------------------------
%% @doc Same as {@link run/2} with the timeout set to <em>infinity</em>
%%
%% @end
%%--------------------------------------------------------------------

-spec run(Query :: string()) ->
                 {'ok', pos_integer(), query_reply()} |
                 query_error().
run(Query) ->
    run(Query, infinity).

%%--------------------------------------------------------------------
%% @doc Runs a query and returns the results or exits with a timeout.
%%
%% This call includes all optimisations made by dflow, as well as some
%% query planning done in the {@link prepare/1} function.
%%
%% @end
%%--------------------------------------------------------------------

-spec run(Query :: string(), Timeout :: pos_integer() | infinity) ->
                 {error, _} |
                 {ok, Start::pos_integer(), query_reply()}.

run(Query, Timeout) ->
    put(start, erlang:system_time()),
    case prepare(Query) of
        {ok, {0, 0, _Parts}, _Start} ->
            dqe_lib:pdebug('query', "prepare found no metrics.", []),
            {error, no_results};
        {ok, {Total, Unique, Parts}, Start} ->
            dqe_lib:pdebug('query', "preperation done.", []),
            WaitRef = make_ref(),
            Funnel = {dqe_funnel, [Parts]},
            Sender = {dflow_send, [self(), WaitRef, Funnel]},
            %% We only optimize the flow when there are at least 10% duplicate
            %% gets, or in other words if less then 90% of the requests are
            %% unique
            FlowOpts = case Unique / Total of
                           UniquePercentage when UniquePercentage > 0.9 ->
                               [terminate_when_done];
                           _ ->
                               [optimize, terminate_when_done]
                       end,
            {ok, _Ref, Flow} = dflow:build(Sender, FlowOpts),
            dqe_lib:pdebug('query', "flow generated.", []),
            dflow:start(Flow, run),
            case dflow_send:recv(WaitRef, Timeout) of
                {ok, [{error, no_results}]} ->
                    dqe_lib:pdebug('query', "Query has no results.", []),
                    {error, no_results};
                {ok, [Result]} ->
                    dqe_lib:pdebug('query', "Query complete.", []),
                    %% Result1 = [Element || {points, Element} <- Result],
                    {ok, Start, Result};
                {ok, []} ->
                    dqe_lib:pdebug('query', "Query has no results.", []),
                    {error, no_results};
                E ->
                    dqe_lib:pdebug('query', "Query error: ~p", [E]),
                    E
            end;
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @doc Prepares query exeuction, this can be used of the query is
%% to be executed asynchronously instead of using {@link run/2}.
%%
%% @end
%%--------------------------------------------------------------------

-spec prepare(Query :: string()) ->
                     {ok, {Total  :: pos_integer(),
                           Unique :: pos_integer(),
                           Count  :: pos_integer(),
                           DFlows :: [dflow:step()]},
                        Start :: pos_integer()} |
                     {error, _}.

prepare(Query) ->
    case dql:prepare(Query) of
        {ok, Parts, Start} ->
            dqe_lib:pdebug('prepare', "Parsing done.", []),
            {Total, Unique} = count_parts(Parts),
            dqe_lib:pdebug('prepare', "Counting parts ~p total and ~p unique.",
                           [Total, Unique]),
            {ok, Parts1} = add_collect(Parts, []),
            dqe_lib:pdebug('prepare', "Naming applied.", []),
            {ok, {Total, Unique, Parts1}, Start};
        E ->
            io:format("E: ~p~n", [E]),
            E
    end.

%%%===================================================================
%%% Interal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc Wrap the query in a collect dflow, where get operations become
%% `dqe_get' flows and function applications become either
%% `dqe_fun_list_flow' or `dqe_fun_flow' flows.
%% @end
%%--------------------------------------------------------------------
-spec add_collect([dql:query_stmt()], [dflow:step()]) -> {ok, [dflow:step()]}.
add_collect([{named, Name, Q} | R], Acc) ->
    {ok, Resolution, Translated} = translate(Q),
    Q1 = {dqe_collect, [Name, Resolution, Translated]},
    add_collect(R, [Q1 | Acc]);

add_collect([], Acc) ->
    {ok, lists:reverse(Acc)}.

%%--------------------------------------------------------------------
%% @private
%% @doc Counts how many total and unique get's are performed, this is
%% used to determine of we want to optimize the query or not.
%% @end
%%--------------------------------------------------------------------
-spec count_parts([dql:query_stmt()]) ->
                         {non_neg_integer(), non_neg_integer()}.
count_parts(Parts) ->
    Gets = [extract_gets(P) || {named, _, P} <- Parts],
    Gets1 = lists:flatten(Gets),
    Total = length(Gets1),
    Unique = length(lists:usort(Gets1)),
    {Total, Unique}.

%%--------------------------------------------------------------------
%% @private
%% @doc Extracts the buckets and metrics we get get from a query.
%% used to dertermine of we want to optimize the query or not.
%% At this point, wildcards would have been resolved, therefore `sget'
%% operations are not handled.
%% @end
%%--------------------------------------------------------------------
-spec extract_gets(dql:flat_stmt()) ->
                          {binary(), binary()}.
extract_gets({combine, _Fun, Parts}) ->
    [extract_gets(P) || P <- Parts];
extract_gets({calc, _, C}) ->
    extract_gets(C);

extract_gets(#{op := get, args := [_, _,_, B, M]}) ->
    {B, M}.


%%--------------------------------------------------------------------
%% @private
%% @doc Translates the AST of the dql parser into a tree for dflow.
%%
%% @end
%%--------------------------------------------------------------------
-spec translate(DQLTerm :: dql:query_part() | dql:dqe_fun()) ->
                       {ok, pos_integer(), dflow:step()}.
translate({calc, [], G}) ->
    translate(G);

%% Sadly this isn't really working, leave it in here.
%% translate({calc,
%%            [#{op := fcall,
%%               resolution := R,
%%               args :=
%%                   #{
%%                     mod := Mod,
%%                 state := State
%%               }}],
%%            #{op := get, args := Args}}) ->
%%     G1 = {dqe_get_fun, [Mod, State] ++ Args},
%%     {ok, R, G1};

%% translate({calc,
%%            [#{op := fcall,
%%               args := #{
%%                     mod := Mod,
%%                state := State
%%               }} | Aggrs],
%%            #{op := get, args := Args}}) ->
%%     FoldFn = fun(#{op := fcall,
%%                    args := #{
%%                          mod := ModX,
%%                      state := StateX
%%                     }}, Acc) ->
%%                      {dqe_fun_flow, [ModX, StateX, Acc]}
%%              end,
%%     #{resolution := R} = lists:last(Aggrs),
%%     G1 = {dqe_get_fun, [Mod, State] ++ Args},
%%     {ok, R, lists:foldl(FoldFn, G1, Aggrs)};

%% TODO we can do this better!

translate({calc, Aggrs, G}) ->
    FoldFn = fun(#{op := fcall,
                   args := #{
                     mod := Mod,
                     state := State
                    }}, Acc) ->
                     {dqe_fun_flow, [Mod, State, Acc]}
             end,
    #{resolution := R} = lists:last(Aggrs),
    {ok, _R, G1} = translate(G),
    {ok, R, lists:foldl(FoldFn, G1, Aggrs)};

translate(#{op := get, resolution := R, args := Args}) ->
    {ok, R, {dqe_get, Args}};

translate({combine,
             #{resolution := R, args := #{mod := Mod, state := State}},
             Parts}) ->
    Parts1 = [begin
                  {ok, _, P1} = translate(Part),
                  P1
              end || Part <- Parts],
    {ok, R, {dqe_fun_list_flow, [Mod, State | Parts1]}}.
