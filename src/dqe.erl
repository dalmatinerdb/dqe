%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc This is the DalmatinerDB query engine, it is run as part of
%%% the dalmatinerDB frontend but can be embedded in other applications
%%% just as well.
%%%
%%% @end
%%% Created : 14 Dec 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(dqe).

-include_lib("dproto/include/dproto.hrl").

-export([prepare/1, run/1, run/2, error_string/1,
         init/0,
         %% Exports for meck
         glob_match/2, pdebug/3]).


-type query_reply() :: [{Name :: binary(),
                         Data :: binary(),
                         Resolution :: pos_integer()}].

-type query_error() :: {'error', 'no_results' |
                        'timeout' |
                        binary() |
                        {'not_found',{binary(), binary()}}}.

error_string({error, {not_found, {var, Name}}}) ->
    ["Variable '", Name, "' referenced but not defined!"];

error_string({error, {not_found, {glob, Glob}}}) ->
    ["No series is matching ", glob_to_string(Glob), "!"];

error_string({error,no_results}) ->
    "No results were returned for the query.";

error_string({error, A}) when is_atom(A) ->
    atom_to_list(A);

error_string({error, B}) when is_binary(B) ->
    binary_to_list(B).

glob_to_string(G) ->
    G1 = [case E of
              '*' ->
                  "'*'";
              B when is_binary(B) ->
                  [$', binary_to_list(B), $']
          end || E <- G],
    string:join(G1, ".").


init() ->
    AggrFuns = [
                dqe_avg,
                dqe_max,
                dqe_min,
                dqe_sum_aggr,
                dqe_derivate
               ],
    CombFuns = [
                dqe_sum_comb
               ],
    AllFuns = AggrFuns ++ CombFuns,
    [dqe_fun:reg(F) || F <- AllFuns].

%%--------------------------------------------------------------------
%% @doc Same as {@link run/2} with the timeout set to <em>infinity</em>
%%
%% @spec run(Query :: string()) ->
%%           {error, timeout} |
%%           {ok, query_reply()}
%% @end
%%--------------------------------------------------------------------

-spec run(Query :: string()) ->
                 query_error() |
                 {ok, Start::pos_integer(), query_reply()}.
run(Query) ->
    run(Query, infinity).

%%--------------------------------------------------------------------
%% @doc Runs a query and returns the results or exits with a timeout.
%%
%% This call includes all optimisations made by dflow, as well as some
%% query planning done in the {@link prepare/1} function.
%%
%% @spec run(Query :: string()) ->
%%           {error, timeout} |
%%           {ok, query_reply()}
%% @end
%%--------------------------------------------------------------------

-spec run(Query :: string(), Timeout :: pos_integer() | infinity) ->
                 {error, _} |
                 {ok, Start::pos_integer(), query_reply()}.

run(Query, Timeout) ->
    put(start, erlang:system_time()),
    case prepare(Query) of
        {ok, {0, 0, _Parts}} ->
            pdebug('query', "prepare found no metrics.", []),
            {error, no_results};
        {ok, {Total, Unique, Parts}} ->
            pdebug('query', "preperation done.", []),
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
            pdebug('query', "flow generated.", []),
            dflow:start(Flow, run),
            case dflow_send:recv(WaitRef, Timeout) of
                {ok, [{error, no_results}]} ->
                    pdebug('query', "Query has no results.", []),
                    {error, no_results};
                {ok, [Result]} ->
                    pdebug('query', "Query complete.", []),
                    %% Result1 = [Element || {points, Element} <- Result],
                    Start = -423, %%TODO: this isn't correct any more!
                    {ok, Start, Result};
                {ok, []} ->
                    pdebug('query', "Query has no results.", []),
                    {error, no_results};
                E ->
                    pdebug('query', "Query error: ~p", [E]),
                    E
            end;
        E ->
            E
    end.
%%--------------------------------------------------------------------
%% @doc Prepares query exeuction, this can be used of the query is
%% desired to be executed asyncrounously instead of using {@link run/2}
%%
%% @spec prepare(Query :: string()) ->
%%                      {DFlows :: [dflow:step()],
%%                       Start :: pos_integer(),
%%                       Count :: pos_integer()}.
%% @end
%%--------------------------------------------------------------------

-spec prepare(Query :: string()) ->
                     {ok, {DFlows :: [dflow:step()],
                           Start :: pos_integer(),
                           Count :: pos_integer()}} |
                     {error, _}.

prepare(Query) ->
    case dql:prepare(Query) of
        {ok, {Parts, Metrics}} ->
            pdebug('prepare', "Parsing done.", []),
            Buckets = [{B, G} ||
                          {[B,G],{sget,_}} <- gb_trees:to_list(Metrics)],
            Buckets1 = orddict:from_list(Buckets),
            %% pdebug('prepare', "Buckets analyzed (~p).", [length(Buckets1)]),
            %% Buckets2 = [begin
            %%                 io:format("{~p, ~p}~n", [B, Gs]),
            %%                 {ok, BMs} = dqe_idx:expand(B, Gs),
            %%                 BMs
            %%             end || {B, Gs} <- Buckets1],
            %% pdebug('prepare', "Buckets fetched.", []),
            %% Parts1 = expand_parts(Parts, Buckets2),
            %% pdebug('prepare', "Parts expanded.", []),
            Parts1 = Parts,
            {Total, Unique} = count_parts(Parts1),
            pdebug('prepare', "Counting parts ~p total and ~p unique.",
                   [Total, Unique]),
            case add_collect(Parts1, [], Buckets1) of
                {ok, Parts2} ->
                    pdebug('prepare', "Naing applied.", []),

                    {ok, {Total, Unique, Parts2}};
                E ->
                    pdebug('prepare', "Naing failed.", []),
                    E
            end;
        E ->
            io:format("E: ~p~n", [E]),
            E
    end.

count_parts(Parts) ->
    Gets = [extract_gets(Part) || Part <- Parts],
    Gets1 = lists:flatten(Gets),
    Total = length(Gets1),
    Unique = length(lists:usort(Gets1)),
    {Total, Unique}.

extract_gets({named, _, P}) ->
    extract_gets(P);
extract_gets({combine, _Fun, Parts}) ->
    [extract_gets(P) || P <- Parts];
extract_gets({calc, _, C}) ->
    extract_gets(C);

extract_gets(#{op := get, args := [_, _,_, B, M]}) ->
    {get, {B, M}};

extract_gets({get, {B, M}}) ->
    {get, {B, M}}.

%% expand_parts(Parts, Buckets) ->
%%     Parts1 = [expand_part(P, Buckets) || P <- Parts],
%%     lists:flatten(Parts1).

%% expand_part({named, N, P}, Buckets) ->
%%     %% TODO adjust the name here!
%%     [{named, update_name(N, M, G), P1}
%%      || {M, G, P1} <- expand_part(P, Buckets)];
%% expand_part({combine, Fun, Parts}, Buckets) ->
%%     Parts1 = [P || {_, _, P} <- expand_parts(Parts, Buckets)],
%%     [{keep, keep, {combine, Fun, Parts1}}];
%% expand_part({calc, C, #{op := get, args := [_, _, _, B, M]}}, _Buckets) ->
%%     %% We convert the metric from a list to a propper metric here
%%     [{keep, keep, {calc, C, {get, {B, dproto:metric_from_list(M)}}}}];
%% expand_part({calc, C, {combine, _, _}= Comb} , Buckets) ->
%%     %% We convert the metric from a list to a propper metric here
%%     [{keep, keep, Comb1}] = expand_part(Comb, Buckets),
%%     [{keep, keep, {calc, C, Comb1}}];

%% expand_part({calc, C, {sget, {B, G}}}, Buckets) ->
%%     Ms = orddict:fetch(B, Buckets),
%%     {ok, Selected} = dqe:glob_match(G, Ms),
%%     [{{metric, M}, {glob, G}, {calc, C, {get, {B, M}}}} || M <- Selected];

%% expand_part({calc, C, {lookup, Query}}, _Buckets) ->
%%     {ok, Selected} = dqe_idx:lookup(Query),
%%     pdebug('prepare', "Looked up ~p metrics for ~p.",
%%            [length(Selected), Query]),
%%     [{{get, {B, M}}, {lookup, Query}, {calc, C, {get, {B, M}}}}
%%      || {B, M} <- Selected].

%% update_name(Name, keep, keep) ->
%%     Name;
%% update_name(Name, {metric, Metric}, {glob, Glob}) ->
%%     MList = dproto:metric_to_list(Metric),
%%     GStr = dql:unparse_metric(Glob),
%%     MStr = dql:unparse_metric(MList),
%%     binary:replace(Name, GStr, MStr);

%% update_name(Name, {get, {Bucket, Metric}}, Lookup = {lookup, _}) ->
%%     MList = dproto:metric_to_list(Metric),
%%     LStr = dql:unparse(Lookup),
%%     GStr = dql:unparse({get, {Bucket, MList}}),
%%     binary:replace(Name, LStr, GStr).

add_collect([Q | R], Acc, Buckets) ->
    case name(Q, Buckets) of
        {ok, {Name, Resolution, Translated}} ->
            Q1 = {dqe_collect, [Name, Resolution, Translated]},
            add_collect(R, [Q1 | Acc], Buckets);
        E ->
            io:format("E: ~p~n", [E]),
            E
    end;

add_collect([], Acc, _Buckets) ->
    {ok, lists:reverse(Acc)}.



%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc Translates the AST of the dql parser into a tree for dflow.
%%
%% @spec translate(DQLTerm :: term(),
%%                 Aliases :: dict:dict(),
%%                 Buckets :: orddict:orddict()) -> dflow:step().
%% @end
%%--------------------------------------------------------------------


translate({calc, [], G}, Buckets) ->
    translate(G, Buckets);

translate(#{op := get, resolution := R, args := Args}, _Buckets) ->
    {ok, R, {dqe_get, Args}};

%% TODO we can do this better!
translate({calc, [#{resolution := R} | _] = Aggrs, G}, Buckets) ->
    %% FoldFn = fun({histogram, HTV, SF, T}, Acc) ->
    %%                  {histogram, HTV, SF, Acc, T};
    %%             ({Type, Fun}, Acc) ->
    %%                  {Type, Fun, Acc};
    %%             ({Type, Fun, Arg1}, Acc) ->
    %%                  {Type, Fun, Acc, Arg1};
    %%             ({Type, Fun, Arg1, Arg2}, Acc) ->
    %%                  {Type, Fun, Acc, Arg1, Arg2}
    %%          end,
    %% Recursive = lists:foldl(FoldFn, G, Aggrs),
    %% translate(Recursive, Buckets);
    FoldFn = fun(#{op := fcall,
                   args := #{
                     mod := Mod,
                     state := State
                    }}, Acc) ->
                     {dqe_fun_flow, [Mod, State, Acc]}
             end,
    {ok, _R, G1} = translate(G, Buckets),
    {ok, R, lists:foldl(FoldFn, G1, Aggrs)};

translate({combine,
           #{resolution := R, args := #{mod := Mod, state := State}},
           Parts},
          Buckets) ->
    Parts1 = [begin
                  {ok, _, P1} = translate(Part, Buckets),
                  P1
              end|| Part <- Parts],
    {ok, R, {dqe_fun_list_flow, [Mod, State | Parts1]}};

%% translate({combine, Fun, Parts}, Buckets) ->
%%     Parts1 = [translate(P, Buckets) || P <- Parts],
%%     Parts2 = [P || {ok, P} <- Parts1],
%%     Parts3 = keep_optimizing_sum(Parts2),
%%     %% TODO: this is a hack we need to fix the optimisation!
%%     Parts4 = lists:flatten(Parts3),
%%     case Fun of
%%         sum ->
%%             {ok, {dqe_sum, [Parts4]}};
%%         avg ->
%%             {ok, {dqe_math, [divide, {dqe_sum, [Parts4]}, length(Parts)]}}
%%     end;


%%--------------------------------------------------------------------
%% One value aggregates
%%--------------------------------------------------------------------
translate({aggr, Aggr, SubQ}, Buckets) ->
    case translate(SubQ, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_aggr1, [Aggr, SubQ1]}};
        E ->
            E
    end;

%%--------------------------------------------------------------------
%% Historam
%%--------------------------------------------------------------------

translate({hfun, Fun, SubQ}, Buckets) ->
    case translate(SubQ, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_hfun1, [Fun, SubQ1]}};
        E ->
            E
    end;

translate({hfun, Fun, SubQ, Val}, Buckets) ->
    case translate(SubQ, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_hfun2, [Fun, Val, SubQ1]}};
        E ->
            E
    end;

translate({histogram, HighestTrackableValue,
           SignificantFigures, SubQ, Time}, Buckets)
  when SignificantFigures >= 1, SignificantFigures =< 5->
    case translate(SubQ, Buckets) of
        {ok, SubQ1} ->
            {ok,
             {dqe_hist, [HighestTrackableValue, SignificantFigures, SubQ1, Time]}};
        E ->
            E
    end;

translate({histogram, _HighestTrackableValue,
           _SignificantFigures, _SubQ, _Time}= E, _Buckets) ->
    io:format(user, "sig: ~p~n", [E]),
    {error, significant_figures};

%%--------------------------------------------------------------------
%% Math
%%--------------------------------------------------------------------

translate({math, multiply, SubQ, Arg}, Buckets) ->
    case translate(SubQ, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_math, [mul, SubQ1, Arg]}};
        E ->
            E
    end;

translate({math, divide, SubQ, Arg}, Buckets)
  when is_integer(Arg) ->
    case translate(SubQ, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_math, [divide, SubQ1, Arg]}};
        E ->
            E
    end;

%%--------------------------------------------------------------------
%% Two argument aggregaets
%%--------------------------------------------------------------------
translate({aggr, min, SubQ, Time}, Buckets) ->
    case translate(SubQ, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_aggr2, [min, SubQ1, dqe_time:to_ms(Time)]}};
        E ->
            E
    end;

translate({aggr, max, SubQ, Time}, Buckets) ->
    case translate(SubQ, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_aggr2, [max, SubQ1, dqe_time:to_ms(Time)]}};
        E ->
            E
    end;

translate({aggr, sum, SubQ, Time}, Buckets) ->
    case translate(SubQ, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_aggr2, [sum, SubQ1, dqe_time:to_ms(Time)]}};
        E ->
            E
    end;

translate({aggr, avg, SubQ, Time}, Buckets) ->
    case translate(SubQ, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_aggr2, [avg, SubQ1, dqe_time:to_ms(Time)]}};
        E ->
            E
    end;

%% translate({aggr, percentile, SubQ, Arg, Time}, Aliases, Buckets) ->
%%     case translate(SubQ, Aliases, Buckets) of
%%         {ok, SubQ1} ->
%%             {ok, {dqe_aggr3, [percentile, SubQ1, Arg, dqe_time:to_ms(Time)]}};
%%         E ->
%%             E
%%     end;

%% translate({var, Name}, Aliases, Buckets) ->
%%     case gb_trees:lookup(Name, Aliases) of
%%         {value, Resolved} ->
%%             translate(Resolved, Aliases, Buckets);
%%         _ ->
%%             {error, {not_found, {var, Name}}}
%%     end;

translate({get, {Bucket, Metric}}, _Buckets) when is_binary(Metric) ->
    %%{ok, {dqe_get, [Bucket, dproto:metric_from_list(Metric)]}}.
    {ok, {dqe_get, [Bucket, Metric]}}.

name({named, N, Q}, Buckets) ->
    case translate(Q, Buckets) of
        {ok, R, Q1} ->
            {ok, {N, R, Q1}};
        E ->
            E
    end.

%% keep_optimizing_sum([_, _, _, _, _ | _] = Gets) ->
%%     keep_optimizing_sum(optimize_sum(Gets));
%% keep_optimizing_sum(Gets) ->
%%     Gets.

%% optimize_sum([G1, G2, G3, G4]) ->
%%     [{dqe_sum, [[G1, G2, G3, G4]]}];

%% optimize_sum([G1, G2, G3, G4 | GRest]) ->
%%     [{dqe_sum, [[G1, G2, G3, G4]]} | optimize_sum(GRest)];


%% optimize_sum([Get]) ->
%%     [Get];

%% optimize_sum(Gets) ->
%%     [{dqe_sum, [Gets]}].


glob_match(G, Ms) ->
    F = fun(M) ->
                rmatch(G, M)
        end,
    case lists:filter(F, Ms) of
        [] ->
            {error, {not_found, {glob, G}}};
        Res ->
            {ok, Res}
    end.



rmatch(['*' | Rm], <<_S:?METRIC_ELEMENT_SS/?SIZE_TYPE, _:_S/binary, Rb/binary>>) ->
    rmatch(Rm, Rb);
rmatch([_M | Rm], <<_S:?METRIC_ELEMENT_SS/?SIZE_TYPE, _M:_S/binary, Rb/binary>>) ->
    rmatch(Rm, Rb);
rmatch([], <<>>) ->
    true;
rmatch(_, _) ->
    false.


%% needs_buckets(L,  Buckets) when is_list(L) ->
%%     lists:foldl(fun needs_buckets/2, Buckets, L);

%% needs_buckets({combine, _Func, Steps}, Buckets) ->
%%     lists:foldl(fun needs_buckets/2, Buckets, Steps);

%% needs_buckets({calc, _Steps, {combine, _Func, _CSteps} = Comb}, Buckets) ->
%%     needs_buckets(Comb, Buckets);

%% needs_buckets({calc, _Steps, {sget, {Bucket, Glob}}}, Buckets) ->
%%     orddict:append(Bucket, Glob, Buckets);

%% needs_buckets({calc, _Steps, {get, _}}, Buckets) ->
%%     Buckets;

%% needs_buckets({calc, _Steps, {lookup, _}}, Buckets) ->
%%     Buckets;

%% needs_buckets({aggr, _Aggr, SubQ}, Buckets) ->
%%     needs_buckets(SubQ, Buckets);

%% needs_buckets({math, _Aggr, SubQ, _}, Buckets) ->
%%     needs_buckets(SubQ, Buckets);

%% needs_buckets({aggr, _Aggr, SubQ, _}, Buckets) ->
%%     needs_buckets(SubQ, Buckets);

%% needs_buckets({aggr, _Aggr, SubQ, _, _}, Buckets) ->
%%     needs_buckets(SubQ, Buckets);

%% needs_buckets({named, _, SubQ}, Buckets) ->
%%     needs_buckets(SubQ, Buckets);

%% needs_buckets({var, _}, Buckets) ->
%%     Buckets;

%% needs_buckets({get, _}, Buckets) ->
%%     Buckets;

%% needs_buckets({lookup, _}, Buckets) ->
%%     Buckets.

pdebug(S, M, E) ->
    D =  erlang:system_time() - pstart(),
    MS = round(D / 1000 / 1000),
    lager:debug("[dqe:~s|~p|~pms] " ++ M, [S, self(), MS | E]).


pstart() ->
    case get(start) of
        N when is_integer(N) ->
            N;
        _ ->
            N = erlang:system_time(),
            put(start, N),
            N
    end.
