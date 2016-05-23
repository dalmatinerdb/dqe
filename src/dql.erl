-module(dql).
-export([parse/1, unparse/1]).
-export([prepare/1, glob_match/2, flatten/1, unparse_metric/1,
         unparse_where/1, resolve_functions/1]).


-type bm() :: {binary(), [binary()]}.

-type gbm() :: {binary(), [binary() | '*']}.

-type time() :: {time, pos_integer(), ms | s | m | h | d | w} | pos_integer().

-type resolution() :: time().

-type relative_time() :: time() |
                         now |
                         {'after' | before, pos_integer(), time()} |
                         {ago, time()}.


-type range() :: {between, relative_time(), relative_time()} |
                 {last, time()}.

-type comb_fun() :: avg | min | max | sum.
-type aggr_fun1() :: derivate.
-type aggr_fun2() :: avg | sum | min | max.

-type get_stmt() ::
        {get, bm()} |
        {sget, gbm()}.

-type aggr_stmt() ::
        {aggr, aggr_fun1(), statement()} |
        {aggr, aggr_fun2(), statement(), time()}.

-type cmb_stmt() ::
        {combine, comb_fun(), [statement()]}.

-type statement() ::
        get_stmt() |
        aggr_stmt() |
        cmb_stmt().

-type flat_aggr_fun() ::
        {aggr, aggr_fun2(), time()} |
        {aggr, aggr_fun1()}.

-type flat_terminal() ::
        {combine, comb_fun(), [flat_stmt()]}.

-type flat_stmt() ::
        flat_terminal() |
        {calc, [flat_aggr_fun()], flat_terminal() | get_stmt()}.

-type parser_error() ::
        {error, binary()}.

-type alias() :: term().

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

resolve_functions(#{op := fcall, args := #{name   := Function,
                                           inputs := Args}}) ->
    case resolve_functions(Args, []) of
        {ok, Args1} ->
            Types = [T || #{return := T} <- Args1],
            Args2 = [{C, is_constant(T)} || C = #{return := T} <- Args1],
            Constants = [C || {C, true} <- Args2],
            Inputs = [C || {C, false} <- Args2],
            case dqe_fun:lookup(Function, Types) of
                {ok,{{_, none, _}, ReturnType, FunMod}} ->
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
                {error,not_found} ->
                    {error, {not_found, Function, Types}}
            end;
        E ->
            E
    end;

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


is_constant(metric) -> false;
is_constant(metric_list) -> false;
is_constant(histogram) -> false;
is_constant(histogram_list) -> false;
is_constant(_) -> true.

%% TODO: Look this up from DDB!

bucket_resolution(O = #{args := A = [_Bucket, _]}, BucketResolutions) ->
    %% gb_trees:enter(Alias, Res, A1)
    Res = 1000,
    {ok, O#{args => [Res | A]}, BucketResolutions}.


-spec parse(string() | binary()) ->
                   parser_error() |
                   {ok, {select, [statement()], range(), resolution()}} |
                   {ok, {select, [statement()], [alias()], range(), resolution()}}.

parse(S) when is_binary(S)->
    parse(binary_to_list(S));

parse(S) ->
    case dql_lexer:string(S) of
        {error,{Line, dql_lexer,E},1} ->
            lexer_error(Line, E);
        {ok, L, _} ->
            case dql_parser:parse(L) of
                {error, {Line, dql_parser, E}} ->
                    parser_error(Line, E);
                R ->
                    R
            end
    end.

get_times(O = #{op := named, args := [N, C]}, T, BucketResolutions) ->
    case get_times(C, T, BucketResolutions) of
        {ok, C1, BucketResolutions1} ->
            {ok, O#{args => [N, C1]}, BucketResolutions1};
        E ->
            E
    end;
get_times({calc, Chain,
           {combine,
            F = #{args := A = #{mod := FMod, constants := Cs}}, Elements}
          }, T, BucketResolutions) ->
    Res = lists:foldl(fun (E, {ok, Acc, BRAcc}) ->
                              case get_times(E, T, BRAcc) of
                                  {ok, E1, BR1} ->
                                      {ok, [E1 | Acc], BR1};
                                  Error ->
                                      Error
                              end;
                          (_, E) ->
                              E
                      end, {ok, [], BucketResolutions}, Elements),
    case Res of
        {ok, Elements1, BR} ->
            Rmss = [get_resolution(E) || E <- Elements1],
            case lists:usort(Rmss) of
                [{ok, Rms}] ->
                    Elements2 = lists:reverse(Elements1),
                    Cs1 = [map_costants(C) || C <- Cs],
                    State = FMod:init(Cs1),
                    Chunk = FMod:resolution(Rms, State),
                    Rms1 = Rms * Chunk,
                    A1 = A#{constants => Cs1, state => State},
                    F1 = F#{args => A1,
                            resolution => Rms1},
                    Comb1 = {combine, F1, Elements2},
                    Calc1 = {calc, Chain, Comb1},
                    {ok, apply_times(Calc1), BR};
                _ ->
                    {error, resolution_conflict}
            end;
        E ->
            E
    end;

get_times({calc, Chain, Get}, T, BucketResolutions) ->
    case bucket_resolution(Get, BucketResolutions) of
        {ok, Get1 = #{args := A = [Rms | _]}, BucketResolutions1} ->
            T1 = apply_times(T, Rms),
            {Start, Count} = compute_se(T1, Rms),
            Calc1 = {calc, Chain, Get1#{resolution => Rms,
                                        args => [Start, Count | A]}},
            {ok, apply_times(Calc1), BucketResolutions1};
        E ->
            E
    end.

get_resolution(#{op := O, args := [_Start, _Count, Rms | _]})
  when O =:= get;
       O =:= sget ->
    {ok, Rms};

get_resolution({calc, [], Get}) ->
    get_resolution(Get);

get_resolution({calc, Chain, _}) ->
    #{resolution := Rms} = lists:last(Chain),
    {ok, Rms}.

apply_times(#{op := named, args := [N, C]}) ->
    C1 = apply_times(C),
    {named, N, C1};

apply_times({calc, Chain, {combine, F, Elements}}) ->
    Elements1 = [apply_times(E) || E <- Elements],
    {calc, Chain, {combine, F, Elements1}};

apply_times({calc, Chain, Get}) ->
    {ok, Rms} = get_resolution(Get),
    Chain1 = time_walk_chain(Chain, Rms, []),
    Chain2 = lists:reverse(Chain1),
    {calc, Chain2, Get}.

time_walk_chain([], _Rms, Acc) ->
    Acc;

time_walk_chain([E = #{op := fcall,
                       args := A = #{mod := FMod, constants := Cs}} | R],
                Rms, Acc) ->
    Cs1 = [map_costants(C) || C <- Cs],
    State = FMod:init(Cs1),
    Chunk = FMod:resolution(Rms, State),
    Rms1 = Rms * Chunk,
    A1 = A#{constants => Cs1, state => State},
    time_walk_chain(R, Rms1, [E#{chunk => Chunk,
                                 resolution => Rms1,
                                 args => A1} | Acc]);

time_walk_chain([E | R], Rms, Acc) ->
    time_walk_chain(R, Rms, [E | Acc]).

map_costants(T = #{op := time}) ->
    to_ms(T);
map_costants(#{op := integer, args := [N]}) ->
    N;
map_costants(#{op := float, args := [N]}) ->
    N;
map_costants(E) ->
    E.


flatten(#{op := named, args := [N, Child]}) ->
    {named, N, flatten(Child, [])};

flatten(Child = #{return := R}) ->
    N = unparse(Child),
    C = flatten(Child, []),
    #{
       op => named,
       args => [N, C],
       signature => [string, R],
       return => R
    }.

-spec flatten(statement(), [flat_aggr_fun()]) ->
                     flat_stmt().

flatten(F = #{op   := fcall,
              args := Args = #{inputs := [Child]}}, Chain) ->
    Args1 = maps:remove(inputs, Args),
    Args2 = maps:remove(orig_args, Args1),
    flatten(Child, [F#{args => Args2} | Chain]);

flatten(F = #{op   := combine,
              args := Args = #{inputs := Children}}, Chain) ->
    Args1 = maps:remove(orig_args, Args),
    Args2 = maps:remove(inputs, Args1),
    Children1 = [flatten(C, []) || C <- Children],
    Comb = {combine, F#{args => Args2}, Children1},
    {calc, Chain, Comb};

flatten(Get = #{op := get},
        Chain) ->
    {calc, Chain, Get};

flatten(Get = #{op := lookup},
        Chain) ->
    {calc, Chain, Get};

flatten(Get = #{op := sget},
        Chain) ->
    {calc, Chain, Get};

flatten(Op, Chain) ->
    io:format("flatten_old: ~p~n", [Op]),
    flatten_old(Op, Chain).

flatten_old({sget, _} = Get, Chain) ->
    {calc, Chain, Get};

flatten_old({get, _} = Get, Chain) ->
    {calc, Chain, Get};

flatten_old({lookup, _} = Lookup, Chain) ->
    {calc, Chain, Lookup};

flatten_old({combine, Aggr, Children}, []) ->
    Children1 = [flatten_old(C, []) || C <- Children],
    {combine, Aggr, Children1};

flatten_old({combine, Aggr, Children}, Chain) ->
    Children1 = [flatten_old(C, []) || C <- Children],
    {calc, Chain, {combine, Aggr, Children1}};


flatten_old({hfun, Fun, Child, Val}, Chain) ->
    flatten_old(Child, [{hfun, Fun, Val} | Chain]);

flatten_old({hfun, Fun, Child}, Chain) ->
    flatten_old(Child, [{hfun, Fun} | Chain]);

flatten_old({histogram, HighestTrackableValue,
         SignificantFigures, Child, Time}, Chain) ->
    flatten_old(Child, [{histogram, HighestTrackableValue,
                     SignificantFigures, Time} | Chain]);

flatten_old({math, Fun, Child, Val}, Chain) ->
    flatten_old(Child, [{math, Fun, Val} | Chain]);

flatten_old({aggr, Aggr, Child}, Chain) ->
    flatten_old(Child, [{aggr, Aggr} | Chain]);


flatten_old({aggr, Aggr, Child, Time}, Chain) ->
    flatten_old(Child, [{aggr, Aggr, Time} | Chain]).


parser_error(Line, E)  ->
    {error, list_to_binary(io_lib:format("Parser error in line ~p: ~s",
                                         [Line, E]))}.

lexer_error(Line, {illegal, E})  ->
    {error, list_to_binary(io_lib:format("Lexer error in line ~p illegal: ~s",
                                         [Line, E]))};

lexer_error(Line, E)  ->
    {error, list_to_binary(io_lib:format("Lexer error in line ~p: ~s",
                                         [Line, E]))}.

%% Start here
prepare(S) ->
    case parse(S) of
        {ok, {select, Qs, Aliases, T}} ->            dqe:pdebug('parse', "Query parsed: ~s", [S]),
            extract_aliases(Qs, T, Aliases);
        E ->
            E
    end.

extract_aliases(Qs, T, Aliases) ->
    {AliasesF, MetricsF} =
        lists:foldl(fun({alias, Alias, Res}, {AAcc, MAcc}) ->
                            {_, A1, M1} = preprocess_qry(Res, AAcc, MAcc),
                            {gb_trees:enter(Alias, Res, A1), M1}
                    end, {gb_trees:empty(), gb_trees:empty()}, Aliases),
    dqe:pdebug('parse', "Aliases resolved.", []),
    preprocess(Qs, T, AliasesF, MetricsF) .


preprocess(Qs, T, Aliases, Metrics) ->
    io:format("~p~n", [Qs]),
    {QQ, _AliasesQ, MetricsQ} =
        lists:foldl(fun(Q, {QAcc, AAcc, MAcc}) ->
                            {Q1, A1, M1} = preprocess_qry(Q, AAcc, MAcc),
                            {[Q1 | QAcc] , A1, M1}
                    end, {[], Aliases, Metrics}, Qs),
    dqe:pdebug('parse', "Preprocessor done.", []),
    QQ1 = lists:reverse(QQ),
    resolve_query_functions(QQ1, T, MetricsQ).

resolve_query_functions(Qs, T, Metrics) ->
    case resolve_functions(Qs, []) of
        {ok, Qs1} ->
            flatten(Qs1, T, Metrics);
        E ->
            E
    end.

flatten(Qs, T, Metrics) ->
    Qs1 = [flatten(Q) || Q <- Qs],
    dqe:pdebug('parse', "Query flattened.", []),
    expand(Qs1, T, Metrics).

expand(Qs, T, Metrics) ->
    %%io:format("Qs:~p~n", [Qs]),
    Qs1 = [expand(Q) || Q <- Qs],
    Qs2 = lists:flatten(Qs1),
    get_resolution(Qs2, T, Metrics).

get_resolution(Qs, T, Metrics) ->
    {Qs1, _} = lists:foldl(fun (Q, {QAcc, RAcc}) ->
                                  {ok, Q1, RAcc1} = get_times(Q, T, RAcc),
                                  {[Q1 | QAcc], RAcc1}
                          end, {[], gb_trees:empty()}, Qs),
    Qs2 = lists:reverse(Qs1),
    propagate_resolutions(Qs2, Metrics).

propagate_resolutions(Qs, Metrics) ->
    Qs1 = [apply_times(Q) || Q <- Qs],
    {ok, {Qs1, Metrics}}.


expand(Q = #{op := named, args := [N, S]}) ->
    [Q#{args => [N, S1]} || S1 <- expand(S)];
expand({calc, Fs, Q}) ->
    io:format("~p~n", [Q]),
    [{calc, Fs, Q1} || Q1 <- expand(Q)];
expand({combine, F, Qs}) ->
    [{combine, F, lists:flatten([expand(Q) || Q <- Qs])}];
expand(Q = #{op := get}) ->
    [Q];
expand(Q = #{op := lookup,
             args := [Collection, Metric, Where]}) ->
    {ok, BMs} = dqe_idx:lookup({in, Collection, Metric, Where}),
    Q1 = Q#{op := get},
    [Q1#{args => [Bucket, Key]} || {Bucket, Key} <- BMs];
expand(Q = #{op := lookup,
             args := [Collection, Metric]}) ->
    {ok, BMs} = dqe_idx:lookup({in, Collection, Metric}),
    Q1 = Q#{op := get},
    [Q1#{args => [Bucket, Key]} || {Bucket, Key} <- BMs];

expand(Q = #{op := sget,
             args := [Bucket, Glob]}) ->
    %% Glob is in an extra list since expand is build to use one or more
    %% globs
    {ok, {_Bucket, Ms}} = dqe_idx:expand(Bucket, [Glob]),
    Q1 = Q#{op := get},
    [Q1#{args => [Bucket, Key]} || Key <- Ms].


%%     compute_times(Qs1, T, Metrics).

%% compute_times(Qs, T, Metrics) ->
%%     %%T1 = apply_times(T, Rms),
%%     dqe:pdebug('parse', "Times normalized.", []),
%%     apply_times(Qs, T, Aliases).


%% apply_times(Qs1, T, MetricsQ) ->
%%     {Start, Count} = compute_se(T1, 1000), %Rms
%%     dqe:pdebug('parse', "Time and resolutions adjusted.", []),
%%     dqe:pdebug('parse', "Query Translated.", []),
%%     {ok, {Qs1, MetricsQ}}.


compute_se(#{ op := between, args := [S, E]}, _Rms) when E > S->
    {S, E - S};
compute_se(#{ op := between, args := [S, E]}, _Rms) ->
    {E, S - E};

compute_se(#{ op := last, args := [N]}, Rms) ->
    NowMs = erlang:system_time(milli_seconds),
    RelativeNow = NowMs div Rms,
    {RelativeNow - N, N};

compute_se(#{op := before, args := [E, D]}, _Rms) ->
    {E - D, D};
compute_se(#{op :='after', args := [S, D]}, _Rms) ->
    {S, D}.

preprocess_qry(O = #{op := G, args := BM}, Aliases, Metrics)
  when G =:= get; G =:= sget->
    Metrics1 = case gb_trees:lookup(BM, Metrics) of
                   none ->
                       gb_trees:insert(BM, {G, 0}, Metrics);
                   {value, {get, N}} ->
                       gb_trees:update(BM, {G, N + 1}, Metrics)
               end,
    {O, Aliases, Metrics1};

preprocess_qry(O = #{op := lookup}, Aliases, Metrics) ->
    {O, Aliases, Metrics};

preprocess_qry(O = #{op   := fcall,
                     args := Args = #{inputs := Input}},
               Aliases, Metrics) ->
    {Input1, A1, M1} =
        lists:foldl(fun (Q, {QAcc, AAcc, Macc}) ->
                            {Qx, Ax, Mx} = preprocess_qry(Q, AAcc, Macc),
                            {[Qx | QAcc], Ax, Mx}
                    end, {[], Aliases, Metrics}, Input),
    Input2 = lists:reverse(Input1),
    {O#{args => Args#{inputs => Input2}}, A1, M1};

preprocess_qry(O = #{op := named, args := [N, Q]}, Aliases, Metrics) ->
    {Q1, A1, M1} = preprocess_qry(Q, Aliases, Metrics),
    {O#{args => [N, Q1]}, A1, M1};

preprocess_qry(#{op := var, args := [V]}, Aliases, Metrics) ->
    {Metrics1, G} = case gb_trees:lookup(V, Aliases) of
                        {value, Q = #{ op := sget, args := BM}} ->
                            M1 = case gb_trees:lookup(BM, Metrics) of
                                     none ->
                                         gb_trees:insert(BM, {sget, 0}, Metrics);
                                     {value, {sget, N}} ->
                                         gb_trees:update(BM, {sget, N + 1}, Metrics)
                                 end,
                            {M1, Q};
                        {value, Q = #{ op := get, args := BM}} ->
                            M1 = case gb_trees:lookup(BM, Metrics) of
                                     none ->
                                         gb_trees:insert(BM, {get, 1}, Metrics);
                                     {value, {get, N}} ->
                                         gb_trees:update(BM, {get, N + 1}, Metrics)
                                 end,
                            {M1, Q}
                    end,
    {G, Aliases, Metrics1};

preprocess_qry(O = #{op := time}, A, M) ->
    {O, A, M};

preprocess_qry(N, A, M) when is_number(N)->
    {N, A, M};

preprocess_qry(Q, A, M) ->
    io:format("preprocess_qry_old: ~p~n", [Q]),
    preprocess_qry_old(Q, A, M, 1000).

preprocess_qry_old({named, N, Q}, Aliases, Metrics, Rms) ->
    {Q1, A1, M1} = preprocess_qry_old(Q, Aliases, Metrics, Rms),
    {{named, N, Q1}, A1, M1};

preprocess_qry_old({aggr, AggF, Q}, Aliases, Metrics, Rms) ->
    {Q1, A1, M1} = preprocess_qry_old(Q, Aliases, Metrics, Rms),
    {{aggr, AggF, Q1}, A1, M1};

preprocess_qry_old({aggr, AggF, Q, T}, Aliases, Metrics, Rms) ->
    {Q1, A1, M1} = preprocess_qry_old(Q, Aliases, Metrics, Rms),
    {{aggr, AggF, Q1, T}, A1, M1};

preprocess_qry_old({aggr, AggF, Q, Arg, T}, Aliases, Metrics, Rms) ->
    {Q1, A1, M1} = preprocess_qry_old(Q, Aliases, Metrics, Rms),
    {{aggr, AggF, Q1, Arg, T}, A1, M1};

preprocess_qry_old({math, MathF, Q, V}, Aliases, Metrics, Rms) ->
    {Q1, A1, M1} = preprocess_qry_old(Q, Aliases, Metrics, Rms),
    {{math, MathF, Q1, V}, A1, M1};

preprocess_qry_old({hfun, HFun, Q}, Aliases, Metrics, Rms) ->
    {Q1, A1, M1} = preprocess_qry_old(Q, Aliases, Metrics, Rms),
    {{hfun, HFun, Q1}, A1, M1};

preprocess_qry_old({hfun, HFun, Q, V}, Aliases, Metrics, Rms) ->
    {Q1, A1, M1} = preprocess_qry_old(Q, Aliases, Metrics, Rms),
    {{hfun, HFun, Q1, V}, A1, M1};

preprocess_qry_old({histogram, HighestTrackableValue, SignificantFigures,
                Q, Time}, Aliases, Metrics, Rms)
  when is_integer(HighestTrackableValue),
       SignificantFigures >= 1,
       SignificantFigures =< 5 ->
    {Q1, A1, M1} = preprocess_qry_old(Q, Aliases, Metrics, Rms),
    {{histogram, HighestTrackableValue, SignificantFigures,
      Q1, Time}, A1, M1};


preprocess_qry_old({get, BM}, Aliases, Metrics, _Rms) ->
    Metrics1 = case gb_trees:lookup(BM, Metrics) of
                   none ->
                       gb_trees:insert(BM, {get, 0}, Metrics);
                   {value, {get, N}} ->
                       gb_trees:update(BM, {get, N + 1}, Metrics)
               end,
    {{get, BM}, Aliases, Metrics1};

preprocess_qry_old({sget, BM}, Aliases, Metrics, _Rms) ->
    Metrics1 = case gb_trees:lookup(BM, Metrics) of
                   none ->
                       gb_trees:insert(BM, {sget, 0}, Metrics);
                   {value, {sget, N}} ->
                       gb_trees:update(BM, {sget, N + 1}, Metrics)
               end,
    {{sget, BM}, Aliases, Metrics1};

preprocess_qry_old({var, V}, Aliases, Metrics, _Rms) ->
    {Metrics1, G} = case gb_trees:lookup(V, Aliases) of
                        {value, {sget, BM}} ->
                            case gb_trees:lookup(BM, Metrics) of
                                none ->
                                    gb_trees:insert(BM, {sget, 0}, Metrics);
                                {value, {sget, N}} ->
                                    gb_trees:update(BM, {sget, N + 1}, Metrics)
                            end;
                        {value, {get, BM}} ->
                            case gb_trees:lookup(BM, Metrics) of
                                none ->
                                    gb_trees:insert(BM, {get, 1}, Metrics);
                                {value, {get, N}} ->
                                    gb_trees:update(BM, {get, N + 1}, Metrics)
                            end
                    end,
    {G, Aliases, Metrics1};

preprocess_qry_old(Q, A, M, _) ->
    {Q, A, M}.

combine([], Acc) ->
    Acc;
combine([E | R], <<>>) ->
    combine(R, E);
combine([E | R], Acc) ->
    combine(R, <<Acc/binary, ", ", E/binary>>).


unparse_metric(Ms) ->
    <<".", Result/binary>> = unparse_metric(Ms, <<>>),
    Result.
unparse_metric(['*' | R], Acc) ->
    unparse_metric(R, <<Acc/binary, ".*">>);
unparse_metric([Metric | R], Acc) ->
    unparse_metric(R, <<Acc/binary, ".'", Metric/binary, "'">>);
unparse_metric([], Acc) ->
    Acc.

unparse_tag({tag, <<>>, K}) ->
    <<"'", K/binary, "'">>;
unparse_tag({tag, N, K}) ->
    <<"'", N/binary, "':'", K/binary, "'">>.
unparse_where({'=', T, V}) ->
    <<(unparse_tag(T))/binary, " = '", V/binary, "'">>;
unparse_where({'or', Clause1, Clause2}) ->
    P1 = unparse_where(Clause1),
    P2 = unparse_where(Clause2),
    <<P1/binary, " OR (", P2/binary, ")">>;
unparse_where({'and', Clause1, Clause2}) ->
    P1 = unparse_where(Clause1),
    P2 = unparse_where(Clause2),
    <<P1/binary, " AND (", P2/binary, ")">>.


unparse(L) when is_list(L) ->
    Ps = [unparse(Q) || Q <- L],
    Unparsed = combine(Ps, <<>>),
    Unparsed;

%% unparse(#{op   := fcall,
%%           args := #{name      := Name,
%%                     orig_args := Args}}) ->
%%                Qs = unparse(Args),
%%                <<Name/binary, "(", Qs/binary, ")">>;

unparse(#{op   := fcall,
          args := #{name      := Name,
                    inputs    := Args}}) ->
               Qs = unparse(Args),
               <<Name/binary, "(", Qs/binary, ")">>;

unparse(#{op   := combine,
          args := #{name      := Name,
                    inputs    := Args}}) ->
               Qs = unparse(Args),
               <<Name/binary, "(", Qs/binary, ")">>;

unparse(#{op := named, args := [N, Q]}) ->
    Qs = unparse(Q),
    <<Qs/binary, " AS '", N/binary, "'">>;

unparse(#{op := time, args := [N, U]}) ->
    Us = atom_to_binary(U, utf8),
    <<(integer_to_binary(N))/binary, " ", Us/binary>>;


unparse(#{ op := get, args := [B, M] }) ->
    <<(unparse_metric(M))/binary, " BUCKET '", B/binary, "'">>;

unparse(#{ op := sget, args := [B, M] }) ->
    <<(unparse_metric(M))/binary, " BUCKET '", B/binary, "'">>;

unparse({select, Q, [], T}) ->
    <<"SELECT ", (unparse(Q))/binary, " ",
      (unparse(T))/binary>>;

unparse({select, Q, A, T}) ->
    <<"SELECT ", (unparse(Q))/binary, " FROM ", (unparse(A))/binary, " ",
      (unparse(T))/binary>>;

unparse(#{op := last, args := [Q]}) ->
    <<"LAST ", (unparse(Q))/binary>>;
unparse(#{op := between, args := [A, B]}) ->
    <<"BETWEEN ", (unparse(A))/binary, " AND ", (unparse(B))/binary>>;
unparse(#{op := 'after', args := [A, B]}) ->
    <<"AFTER ", (unparse(A))/binary, " FOR ", (unparse(B))/binary>>;
unparse(#{op := before, args := [A, B]}) ->
    <<"BEFORE ", (unparse(A))/binary, " FOR ", (unparse(B))/binary>>;

unparse(#{op := ago, args := [T]}) ->
    <<(unparse(T))/binary, " AGO">>;

unparse(now) ->
    <<"NOW">>;

unparse(N) when is_integer(N)->
    <<(integer_to_binary(N))/binary>>;

unparse(#{op := lookup, args := [B, M]}) ->
    <<(unparse_metric(M))/binary, " IN '", B/binary, "'">>;
unparse(#{op := lookup, args := [B, M, Where]}) ->
    <<(unparse_metric(M))/binary, " IN '", B/binary,
      "' WHERE ", (unparse_where(Where))/binary>>;

unparse(X) ->
    io:format("Unparse old: ~p~n", [X]),
    unparse_old(X).

unparse_old({select, Q, A, T, R}) ->
    <<"SELECT ", (unparse_old(Q))/binary, " FROM ", (unparse_old(A))/binary, " ",
      (unparse_old(T))/binary, " IN ", (unparse_old(R))/binary>>;
unparse_old({select, Q, T, R}) ->
    <<"SELECT ", (unparse_old(Q))/binary, " ", (unparse_old(T))/binary, " IN ",
      (unparse_old(R))/binary>>;
unparse_old({last, Q}) ->
    <<"LAST ", (unparse_old(Q))/binary>>;
unparse_old({between, A, B}) ->
    <<"BETWEEN ", (unparse_old(A))/binary, " AND ", (unparse_old(B))/binary>>;
unparse_old({'after', A, B}) ->
    <<"AFTER ", (unparse_old(A))/binary, " FOR ", (unparse_old(B))/binary>>;
unparse_old({before, A, B}) ->
    <<"BEFORE ", (unparse_old(A))/binary, " FOR ", (unparse_old(B))/binary>>;

unparse_old({var, V}) ->
    V;


unparse_old({alias, A, V}) ->
    <<(unparse_old(V))/binary, " AS '", A/binary, "'">>;
unparse_old({get, {B, M}}) ->
    <<(unparse_metric(M))/binary, " BUCKET '", B/binary, "'">>;
unparse_old({sget, {B, M}}) ->
    <<(unparse_metric(M))/binary, " BUCKET '", B/binary, "'">>;
unparse_old({lookup, {in, B, M}}) ->
    <<(unparse_metric(M))/binary, " IN '", B/binary, "'">>;
unparse_old({lookup, {in, B, M, Where}}) ->
    <<(unparse_metric(M))/binary, " IN '", B/binary,
      "' WHERE ", (unparse_where(Where))/binary>>;
unparse_old({combine, Fun, L}) ->
    Funs = list_to_binary(atom_to_list(Fun)),
    <<Funs/binary, "(", (unparse_old(L))/binary, ")">>;

unparse_old(N) when is_integer(N)->
    <<(integer_to_binary(N))/binary>>;

unparse_old(F) when is_float(F)->
    <<(float_to_binary(F))/binary>>;

unparse_old(now) ->
    <<"NOW">>;

unparse_old({ago, T}) ->
    <<(unparse_old(T))/binary, " AGO">>;

unparse_old({time, N, ms}) ->
    <<(integer_to_binary(N))/binary, " ms">>;
unparse_old({time, N, s}) ->
    <<(integer_to_binary(N))/binary, " s">>;
unparse_old({time, N, m}) ->
    <<(integer_to_binary(N))/binary, " m">>;
unparse_old({time, N, h}) ->
    <<(integer_to_binary(N))/binary, " h">>;
unparse_old({time, N, d}) ->
    <<(integer_to_binary(N))/binary, " d">>;
unparse_old({time, N, w}) ->
    <<(integer_to_binary(N))/binary, " w">>;

unparse_old({histogram, HighestTrackableValue, SignificantFigures, Q, T}) ->
    <<"histogram(",
      (integer_to_binary(HighestTrackableValue))/binary, ", ",
      (integer_to_binary(SignificantFigures))/binary, ", ",
      (unparse_old(Q))/binary, ", ",
      (unparse_old(T))/binary, ")">>;
unparse_old({hfun, Fun, Q}) ->
    Funs = list_to_binary(atom_to_list(Fun)),
    Qs = unparse_old(Q),
    <<Funs/binary, "(", Qs/binary, ")">>;

unparse_old({hfun, percentile, Q, P}) ->
    Qs = unparse_old(Q),
    Ps = unparse_old(P),
    <<"percentile(", Qs/binary, ", ", Ps/binary, ")">>;

unparse_old({aggr, Fun, Q}) ->
    Funs = list_to_binary(atom_to_list(Fun)),
    Qs = unparse_old(Q),
    <<Funs/binary, "(", Qs/binary, ")">>;
unparse_old({maggr, Fun, Q}) ->
    Funs = list_to_binary(atom_to_list(Fun)),
    Qs = unparse_old(Q),
    <<Funs/binary, "(", Qs/binary, ")">>;
unparse_old({aggr, Fun, Q, T}) ->
    Funs = list_to_binary(atom_to_list(Fun)),
    Qs = unparse_old(Q),
    Ts = unparse_old(T),
    <<Funs/binary, "(", Qs/binary, ", ", Ts/binary, ")">>;
unparse_old({aggr, Fun, Q, A, T}) ->
    Funs = list_to_binary(atom_to_list(Fun)),
    Qs = unparse_old(Q),
    As = unparse_old(A),
    Ts = unparse_old(T),
    <<Funs/binary, "(", Qs/binary, ", ", As/binary, ", ", Ts/binary, ")">>;
unparse_old({math, multiply, Q, V}) ->
    Qs = unparse_old(Q),
    Vs = unparse_old(V),
    <<Qs/binary, " * ", Vs/binary>>;
unparse_old({math, divide, Q, V}) ->
    Qs = unparse_old(Q),
    Vs = unparse_old(V),
    <<Qs/binary, " / ", Vs/binary>>;
unparse_old({math, Fun, Q, V}) ->
    Funs = list_to_binary(atom_to_list(Fun)),
    Qs = unparse_old(Q),
    Vs = unparse_old(V),
    <<Funs/binary, "(", Qs/binary, ", ", Vs/binary, ")">>.

apply_times(#{op := last, args := [L]}, R) ->
    #{op => last, args => [apply_times(L, R)]};

apply_times(#{op := between, args := [S, E]}, R) ->
    #{op => between, args => [apply_times(S, R), apply_times(E, R)]};

apply_times(#{op := 'after', args := [S, D]}, R) ->
    #{op => 'after', args => [apply_times(S, R), apply_times(D, R)]};

apply_times(#{op := before, args := [E, D]}, R) ->
    #{op => before, args => [apply_times(E, R), apply_times(D, R)]};

apply_times(N, _) when is_integer(N) ->
    erlang:max(1, N);

apply_times(now, R) ->
    NowMs = erlang:system_time(milli_seconds),
    erlang:max(1, NowMs div R);

apply_times(#{op := ago, args := [T]}, R) ->
    NowMs = erlang:system_time(milli_seconds),
    erlang:min(1, (NowMs - to_ms(T)) div R);

apply_times(T, R) ->
    erlang:max(1, to_ms(T) div R).

to_ms(#{op := time, args := [N, U]}) ->
    to_ms({time, N, U});

to_ms({time, N, ms}) ->
    N;
to_ms({time, N, s}) ->
    N*1000;
to_ms({time, N, m}) ->
    N*1000*60;
to_ms({time, N, h}) ->
    N*1000*60*60;
to_ms({time, N, d}) ->
    N*1000*60*60*24;
to_ms({time, N, w}) ->
    N*1000*60*60*24*7.

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
