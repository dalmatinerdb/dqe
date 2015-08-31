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

error_string({error, {not_found, {var, Name}}}) ->
    ["Variable '", Name, "' referenced but not defined!"];

error_string({error, {not_found, {glob, Glob}}}) ->
    ["No series is matching ", glob_to_string(Glob), "!"];

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
                 {error, _} |
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
                 {error, _} |
                 {ok, query_reply()}.

run(Query, Timeout) ->
    case prepare(Query) of
        {ok, {Parts, Start, Count}} ->
            WaitRef = make_ref(),
            Funnel = {dqe_funnel, [[{dqe_collect, [Part]} || Part <- Parts]]},
            Sender = {dflow_send, [self(), WaitRef, Funnel]},
            {ok, _Ref, Flow} = dflow:build(Sender, [optimize, terminate_when_done]),
            dflow:start(Flow, {Start, Count}),
            case  dflow_send:recv(WaitRef, Timeout) of
                {ok, [Result]} ->
                    {ok, Start, Result};
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
            case name_parts(Parts, [], Aliases, Buckets2) of
                {ok, Parts1} ->
                    {ok, {Parts1, Start, Count}};
                E ->
                    E
            end;
        E ->
            E
    end.

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

translate({aggr, Aggr, SubQ}, Aliases, Buckets) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_aggr1, [Aggr, SubQ1]}};
        E ->
            E
    end;

translate({math, multiply, SubQ, Arg}, Aliases, Buckets)
  when is_integer(Arg) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_math, [mul, SubQ1, Arg]}};
        E ->
            E
    end;

translate({math, multiply, SubQ, Arg}, Aliases, Buckets) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_math, [scale, SubQ1, Arg]}};
        E ->
            E
    end;

translate({math, divide, SubQ, Arg}, Aliases, Buckets)
  when is_integer(Arg) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_math, [divide, SubQ1, Arg]}};
        E ->
            E
    end;

translate({math, divide, SubQ, Arg}, Aliases, Buckets) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_math, [scale, SubQ1, 1/Arg]}};
        E ->
            E
    end;

translate({aggr, Aggr, SubQ, Time}, Aliases, Buckets) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_aggr2, [Aggr, SubQ1, dqe_time:to_ms(Time)]}};
        E ->
            E
    end;

translate({aggr, Aggr, SubQ, Arg, Time}, Aliases, Buckets) ->
    case translate(SubQ, Aliases, Buckets) of
        {ok, SubQ1} ->
            {ok, {dqe_aggr3, [Aggr, SubQ1, Arg, dqe_time:to_ms(Time)]}};
        E ->
            E
    end;

translate({mget, avg, {Bucket, Glob}}, _Aliases, Buckets) ->
    Metrics = orddict:fetch(Bucket, Buckets),
    case glob_match(Glob, Metrics) of
        {ok, Metrics1} ->
            Gets = [{dqe_get, [Bucket, Metric]} || Metric <- Metrics1],
            Gets1 = keep_optimizing_mget(Gets),
            {ok, {dqe_math, [divide, {dqe_mget, [Gets1]}, length(Gets)]}};
        E ->
            E
    end;

translate({mget, sum, {Bucket, Glob}}, _Aliases, Buckets) ->
    Metrics = orddict:fetch(Bucket, Buckets),
    case glob_match(Glob, Metrics) of
        {ok, Metrics1} ->
            Gets = [{dqe_get, [Bucket, Metric]} || Metric <- Metrics1],
            Gets1 = keep_optimizing_mget(Gets),
            {ok, {dqe_mget, [Gets1]}};
        E ->
            E
    end;

translate({var, Name}, Aliases, Buckets) ->
    case gb_trees:get(Name, Aliases) of
        {value, Resolved} ->
            translate(Resolved, Aliases, Buckets);
        _ ->
            {error, {not_found, {var, Name}}}
    end;

translate({get, {Bucket, Metric}}, _Aliases, _Buckets) ->
    {ok, {dqe_get, [Bucket, dproto:metric_from_list(Metric)]}}.

name({named, N, Q}, Aliases, Buckets) ->
    case translate(Q, Aliases, Buckets) of
        {ok, Q1} ->
            {ok, {N, Q1}};
        E ->
            E
    end;

name(Q, Aliases, Buckets) ->
    case translate(Q, Aliases, Buckets) of
        {ok, Q1} ->
            {ok, {dql:unparse(Q), Q1}};
        E ->
            E
    end.

keep_optimizing_mget([_, _, _, _, _ | _] = Gets) ->
    keep_optimizing_mget(optimize_mget(Gets));
keep_optimizing_mget(Gets) ->
    Gets.

optimize_mget([G1, G2, G3, G4]) ->
    [{dqe_mget, [[G1, G2, G3, G4]]}];

optimize_mget([G1, G2, G3, G4 | GRest]) ->
    [{dqe_mget, [[G1, G2, G3, G4]]} | optimize_mget(GRest)];


optimize_mget([Get]) ->
    [Get];

optimize_mget(Gets) ->
    [{dqe_mget, [Gets]}].


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

needs_buckets({aggr, _Aggr, SubQ}, Buckets) ->
    needs_buckets(SubQ, Buckets);

needs_buckets({math, _Aggr, SubQ, _}, Buckets) ->
    needs_buckets(SubQ, Buckets);

needs_buckets({aggr, _Aggr, SubQ, _}, Buckets) ->
    needs_buckets(SubQ, Buckets);

needs_buckets({aggr, _Aggr, SubQ, _, _}, Buckets) ->
    needs_buckets(SubQ, Buckets);

needs_buckets({mget, _, {Bucket, Glob}}, Buckets) ->
    orddict:append(Bucket, glob_prefix(Glob, []), Buckets);

needs_buckets({named, _, SubQ}, Buckets) ->
    needs_buckets(SubQ, Buckets);

needs_buckets({var, _}, Buckets) ->
    Buckets;

needs_buckets({get, _}, Buckets) ->
    Buckets.
