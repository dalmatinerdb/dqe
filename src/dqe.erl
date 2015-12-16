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

-export([prepare/1, run/1, run/2, error_string/1]).

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
    case prepare(Query) of
        {ok, {Parts, Start, Count}} ->
            WaitRef = make_ref(),
            Funnel = {dqe_funnel, [[{dqe_collect, [Part]} || Part <- Parts]]},
            Sender = {dflow_send, [self(), WaitRef, Funnel]},
            {ok, _Ref, Flow} = dflow:build(Sender, [optimize, terminate_when_done]),
            dflow:start(Flow, {Start, Count}),
            case  dflow_send:recv(WaitRef, Timeout) of
                {ok, [{error, no_results}]} ->
                    {error, no_results};
                {ok, [Result]} ->
                    {ok, Start, Result};
                {ok, []} ->
                    {error, no_results};
                E ->
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
        {ok, {Parts, Start, Count, _Res, Aliases, _SomethingElse}} ->
            Buckets = needs_buckets(Parts, []),
            Buckets1 = [{B, compress_prefixes(Ps)} || {B, Ps} <- Buckets],
            Buckets2 =
                [case Ps of
                     all ->
                         {Bkt, dalmatiner_connection:list(Bkt)};
                     _ ->
                         Ps1 = [begin
                                    {ok, Ms} = dalmatiner_connection:list(Bkt, P),
                                    Ms
                                end || P <- Ps],
                         Ps2 = lists:usort(lists:flatten(Ps1)),
                         {Bkt, Ps2}
                 end
                 || {Bkt, Ps} <- Buckets1],
            Parts1 = expand_parts(Parts, Buckets2),
            case name_parts(Parts1, [], Aliases, Buckets2) of
                {ok, Parts2} ->
                    {ok, {Parts2, Start, Count}};
                E ->
                    E
            end;
        E ->
            E
    end.

expand_parts(Parts, Buckets) ->
    Parts1 = [expand_part(P, Buckets) || P <- Parts],
    lists:flatten(Parts1).

expand_part({named, N, P}, Buckets) ->
    %% TODO adjust the name here!
    [{named, update_name(N, M, G), P1}
     || {M, G, P1} <- expand_part(P, Buckets)];
expand_part({combine, Fun, Parts}, Buckets) ->
    Parts1 = [P || {_, _, P} <- expand_parts(Parts, Buckets)],
    [{keep, keep, {combine, Fun, Parts1}}];
expand_part({calc, C, {get, {B, M}}}, _Buckets) ->
    %% We convert the metric from a list to a propper metric here
    [{keep, keep, {calc, C, {get, {B, dproto:metric_from_list(M)}}}}];
expand_part({calc, C, {sget, {B, G}}}, Buckets) ->
    Ms = orddict:fetch(B, Buckets),
    {ok, Selected} = glob_match(G, Ms),
    [{M, G, {calc, C, {get, {B, M}}}} || M <- Selected].



update_name(Name, keep, keep) ->
    Name;
update_name(Name, Metric, Glob) ->
    MList = dproto:metric_to_list(Metric),
    GStr = dql:unparse_metric(Glob),
    MStr = dql:unparse_metric(MList),
    binary:replace(Name, GStr, MStr).

name_parts([Q | R], Acc, Aliases, Buckets) ->
    case name(Q, Aliases, Buckets) of
        {ok, {Name, Translated}} ->
            Q1 = {dqe_name, [Name, Translated]},
            name_parts(R, [Q1 | Acc], Aliases, Buckets);
        E ->
            E
    end;

name_parts([], Acc, _Aliases, _Buckets) ->
    {ok, lists:reverse(Acc)}.


compress_prefixes(Prefixes) ->
    compress_prefixes(lists:sort(Prefixes), []).

compress_prefixes([[] | _], _) ->
    all;
compress_prefixes([], R) ->
    R;
compress_prefixes([E], R) ->
    [E | R];
compress_prefixes([A, B | R], Acc) ->
    case binary:longest_common_prefix([A, B]) of
        L when L == byte_size(A) ->
            compress_prefixes([B | R], Acc);
        _ ->
            compress_prefixes([B | R], [A | Acc])
    end.

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


translate({calc, [], {get, {Bucket, Metric}}}, _Aliases, _Buckets) ->
    {ok, {dqe_get, [Bucket, Metric]}};

%% TODO we can do this better!
translate({calc, Aggrs, G}, Aliases, Buckets) ->
    FoldFn = fun({Type, Fun}, Acc) ->
                     {Type, Fun, Acc};
                 ({Type, Fun, Arg1}, Acc) ->
                     {Type, Fun, Acc, Arg1};
                 ({Type, Fun, Arg1, Arg2}, Acc) ->
                     {Type, Fun, Acc, Arg1, Arg2}
             end,
    Recursive = lists:foldl(FoldFn, G, Aggrs),
    translate(Recursive, Aliases, Buckets);

translate({combine, Fun, Parts}, Aliases, Buckets) ->
    Parts1 = [translate(P, Aliases, Buckets) || P <- Parts],
    Parts2 = [P || {ok, P} <- Parts1],
    Parts3 = keep_optimizing_sum(Parts2),
    %% TODO: this is a hack we need to fix the optimisation!
    Parts4 = lists:flatten(Parts3),
    case Fun of
        sum ->
            {ok, {dqe_sum, [Parts4]}};
        avg ->
            {ok, {dqe_math, [divide_r, {dqe_sum, [Parts4]}, length(Parts)]}}
    end;

%%--------------------------------------------------------------------
%% One value aggregates
%%--------------------------------------------------------------------
translate({aggr, derivate, SubQ}, Aliases, Buckets) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_aggr1, [derivate_r, SubQ1]}};
        E ->
            E
    end;



%%--------------------------------------------------------------------
%% Math
%%--------------------------------------------------------------------

translate({math, multiply, SubQ, Arg}, Aliases, Buckets)
  when is_integer(Arg) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_math, [mul_r, SubQ1, Arg]}};
        E ->
            E
    end;

translate({math, multiply, SubQ, Arg}, Aliases, Buckets) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_math, [scale_r, SubQ1, Arg]}};
        E ->
            E
    end;

translate({math, divide, SubQ, Arg}, Aliases, Buckets)
  when is_integer(Arg) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_math, [divide_r, SubQ1, Arg]}};
        E ->
            E
    end;

translate({math, divide, SubQ, Arg}, Aliases, Buckets) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_math, [scale_r, SubQ1, 1/Arg]}};
        E ->
            E
    end;

%%--------------------------------------------------------------------
%% Two argument aggregaets
%%--------------------------------------------------------------------
translate({aggr, min, SubQ, Time}, Aliases, Buckets) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_aggr2, [min_r, SubQ1, dqe_time:to_ms(Time)]}};
        E ->
            E
    end;

translate({aggr, max, SubQ, Time}, Aliases, Buckets) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_aggr2, [max_r, SubQ1, dqe_time:to_ms(Time)]}};
        E ->
            E
    end;

translate({aggr, empty, SubQ, Time}, Aliases, Buckets) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_aggr2, [empty_r, SubQ1, dqe_time:to_ms(Time)]}};
        E ->
            E
    end;

translate({aggr, sum, SubQ, Time}, Aliases, Buckets) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_aggr2, [sum_r, SubQ1, dqe_time:to_ms(Time)]}};
        E ->
            E
    end;

translate({aggr, avg, SubQ, Time}, Aliases, Buckets) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_aggr2, [avg_r, SubQ1, dqe_time:to_ms(Time)]}};
        E ->
            E
    end;

translate({aggr, percentile, SubQ, Arg, Time}, Aliases, Buckets) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_aggr3, [percentile, SubQ1, Arg, dqe_time:to_ms(Time)]}};
        E ->
            E
    end;

translate({var, Name}, Aliases, Buckets) ->
    case gb_trees:lookup(Name, Aliases) of
        {value, Resolved} ->
            translate(Resolved, Aliases, Buckets);
        _ ->
            {error, {not_found, {var, Name}}}
    end;

translate({get, {Bucket, Metric}}, _Aliases, _Buckets) when is_binary(Metric) ->
    %%{ok, {dqe_get, [Bucket, dproto:metric_from_list(Metric)]}}.
    {ok, {dqe_get, [Bucket, Metric]}}.

name({named, N, Q}, Aliases, Buckets) ->
    case translate(Q, Aliases, Buckets) of
        {ok, Q1} ->
            {ok, {N, Q1}};
        E ->
            E
    end.

keep_optimizing_sum([_, _, _, _, _ | _] = Gets) ->
    keep_optimizing_sum(optimize_sum(Gets));
keep_optimizing_sum(Gets) ->
    Gets.

optimize_sum([G1, G2, G3, G4]) ->
    [{dqe_sum, [[G1, G2, G3, G4]]}];

optimize_sum([G1, G2, G3, G4 | GRest]) ->
    [{dqe_sum, [[G1, G2, G3, G4]]} | optimize_sum(GRest)];


optimize_sum([Get]) ->
    [Get];

optimize_sum(Gets) ->
    [{dqe_sum, [Gets]}].


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

glob_prefix([], Prefix) ->
    dproto:metric_from_list(lists:reverse(Prefix));
glob_prefix(['*' |_], Prefix) ->
    dproto:metric_from_list(lists:reverse(Prefix));
glob_prefix([E | R], Prefix) ->
    glob_prefix(R, [E | Prefix]).



rmatch(['*' | Rm], <<_S:?METRIC_ELEMENT_SS/?SIZE_TYPE, _:_S/binary, Rb/binary>>) ->
    rmatch(Rm, Rb);
rmatch([_M | Rm], <<_S:?METRIC_ELEMENT_SS/?SIZE_TYPE, _M:_S/binary, Rb/binary>>) ->
    rmatch(Rm, Rb);
rmatch([], <<>>) ->
    true;
rmatch(_, _) ->
    false.

needs_buckets(L,  Buckets) when is_list(L) ->
    lists:foldl(fun needs_buckets/2, Buckets, L);

needs_buckets({combine, _Func, Steps}, Buckets) ->
    lists:foldl(fun needs_buckets/2, Buckets, Steps);

needs_buckets({calc, _Steps, {sget, {Bucket, Glob}}}, Buckets) ->
    orddict:append(Bucket, glob_prefix(Glob, []), Buckets);

needs_buckets({calc, _Steps, {get, _}}, Buckets) ->
    Buckets;

needs_buckets({aggr, _Aggr, SubQ}, Buckets) ->
    needs_buckets(SubQ, Buckets);

needs_buckets({math, _Aggr, SubQ, _}, Buckets) ->
    needs_buckets(SubQ, Buckets);

needs_buckets({aggr, _Aggr, SubQ, _}, Buckets) ->
    needs_buckets(SubQ, Buckets);

needs_buckets({aggr, _Aggr, SubQ, _, _}, Buckets) ->
    needs_buckets(SubQ, Buckets);

needs_buckets({named, _, SubQ}, Buckets) ->
    needs_buckets(SubQ, Buckets);

needs_buckets({var, _}, Buckets) ->
    Buckets;

needs_buckets({get, _}, Buckets) ->
    Buckets.
