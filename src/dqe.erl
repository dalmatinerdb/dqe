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

-export([prepare/1, run/1, run/2]).

-type query_reply() :: [{Name :: binary(),
                         Data :: binary(),
                         Resolution :: pos_integer()}].

%%--------------------------------------------------------------------
%% @doc Same as {@link run/2} with the timeout set to <em>infinity</em>
%%
%% @spec run(Query :: string()) ->
%%           {error, timeout} |
%%           {ok, query_reply()}
%% @end
%%--------------------------------------------------------------------

-spec run(Query :: string()) ->
                 {error, timeout} |
                 {ok, query_reply()}.
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
                 {error, timeout} |
                 {ok, query_reply()}.

run(Query, Timeout) ->
    {Parts, Start, Count} = prepare(Query),
    WaitRef = make_ref(),
    Funnel = {dqe_funnel, [[{dqe_collect, [Part]} || Part <- Parts]]},
    Sender = {dflow_send, [self(), WaitRef, Funnel]},
    {ok, _Ref, Flow} = dflow:build(Sender, [optimize, terminate_when_done]),
    dflow:start(Flow, {Start, Count}),
    dflow_send:recv(WaitRef, Timeout).


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
                     {DFlows :: [dflow:step()],
                      Start :: pos_integer(),
                      Count :: pos_integer()}.

prepare(Query) ->
    {Parts, Start, Count, _Res, Aliases, _SomethingElse} = dql:prepare(Query),
    Buckets = needs_buckets(Parts, []),
    Buckets1 = [{Bkt, dalmatiner_connection:list(Bkt)} || Bkt <- Buckets],
    Parts1 = [begin
                  {Name, Translated} = name(Q, Aliases, Buckets1),
                  {dqe_name, [Name, Translated]}
              end || Q <- Parts],
    {Parts1, Start, Count}.

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

translate({aggr, Aggr, SubQ}, Aliases, Buckets) ->
    {dqe_aggr1, [Aggr, translate(SubQ, Aliases, Buckets)]};

translate({aggr, multiply, SubQ, Arg}, Aliases, Buckets) ->
    {dqe_math, [scale, translate(SubQ, Aliases, Buckets), Arg]};

translate({aggr, divide, SubQ, Arg}, Aliases, Buckets) ->
    {dqe_math, [scale, translate(SubQ, Aliases, Buckets), 1/Arg]};

translate({aggr, Aggr, SubQ, Time}, Aliases, Buckets) ->
    {dqe_aggr2, [Aggr, translate(SubQ, Aliases, Buckets), dqe_time:to_ms(Time)]};

translate({mget, avg, {Bucket, Glob}}, _Aliases, Buckets) ->
    {ok, Metrics} = orddict:fetch(Bucket, Buckets),
    Gets = [{dqe_get, [Bucket, Metric]} || Metric <- glob_match(Glob, Metrics)],
    Gets1 = keep_optimizing_mget(Gets),
    {dqe_math, [scale, {dqe_mget, [Gets1]}, 1/length(Gets)]};

translate({mget, sum, {Bucket, Glob}}, _Aliases, Buckets) ->
    {ok, Metrics} = orddict:fetch(Bucket, Buckets),
    Gets = [{dqe_get, [Bucket, Metric]} || Metric <- glob_match(Glob, Metrics)],
    Gets1 = keep_optimizing_mget(Gets),
    {dqe_mget, [Gets1]};

translate({var, Name}, Aliases, Buckets) ->
    translate(gb_trees:get(Name, Aliases), Aliases, Buckets);

translate({get, {Bucket, Metric}}, _Aliases, _Buckets) ->
    {dqe_get, [Bucket, Metric]}.

name({named, N, Q}, Aliases, Buckets) ->
    {N, translate(Q, Aliases, Buckets)};

name(Q, Aliases, Buckets) ->
    {dql:unparse(Q), translate(Q, Aliases, Buckets)}.

keep_optimizing_mget([_, _, _, _, _ | _] = Gets) ->
    keep_optimizing_mget(optimize_mget(Gets));
keep_optimizing_mget(Gets) ->
    Gets.

optimize_mget([G1, G2, G3, G4 | GRest]) ->
    [{dqe_mget, [[G1, G2, G3, G4]]} | optimize_mget(GRest)];

optimize_mget([Get]) ->
    [Get];

optimize_mget(Gets) ->
    [{dqe_mget, [Gets]}].


glob_match(G, Ms) ->
    GE = re:split(G, "\\*"),
    F = fun(M) ->
                rmatch(GE, M)
        end,
    lists:filter(F, Ms).


rmatch([<<>>, <<$., Ar1/binary>> | Ar], B) ->
    rmatch([Ar1 | Ar], skip_one(B));
rmatch([<<>> | Ar], B) ->
    rmatch(Ar, skip_one(B));
rmatch([<<$., Ar1/binary>> | Ar], B) ->
    rmatch([Ar1 | Ar], skip_one(B));
rmatch([A | Ar], B) ->
    case binary:longest_common_prefix([A, B]) of
        L when L == byte_size(A) ->
            <<_:L/binary, Br/binary>> = B,
            rmatch(Ar, Br);
        _ ->
            false
    end;
rmatch([], <<>>) ->
    true;
rmatch(_A, _B) ->
    false.

skip_one(<<$., R/binary>>) ->
    R;
skip_one(<<>>) ->
    <<>>;
skip_one(<<_, R/binary>>) ->
    skip_one(R).


needs_buckets(L,  Buckets) when is_list(L) ->
    lists:foldl(fun needs_buckets/2, Buckets, L);

needs_buckets({aggr, _Aggr, SubQ}, Buckets) ->
    needs_buckets(SubQ, Buckets);

needs_buckets({aggr, _Aggr, SubQ, _}, Buckets) ->
    needs_buckets(SubQ, Buckets);

needs_buckets({mget, _, {Bucket, _}}, Buckets) ->
    ordsets:add_element(Bucket, Buckets);

needs_buckets({named, _, SubQ}, Buckets) ->
    needs_buckets(SubQ, Buckets);

needs_buckets({var, _}, Buckets) ->
    Buckets;

needs_buckets({get, _}, Buckets) ->
    Buckets.
