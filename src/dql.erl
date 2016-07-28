-module(dql).

-export([prepare/1]).
-ifdef(TEST).
-export([parse/1]).
-endif.

-export_type([query_part/0, dqe_fun/0, query_stmt/0]).

-type time() :: #{ op => time, args => [pos_integer() | ms | s | m | h | d | w]} | pos_integer().

-type relative_time() :: time() |
                         now |
                         #{ op => 'after' | before, args => [pos_integer() | time()]} |
                         #{op => ago, ags => [time()]}.


-type range() :: #{ op => between, args => [relative_time() | relative_time()]} |
                 #{ op => last, args => [time()]}.

-type sig_element() :: string | number | metric.

-type dqe_fun() ::
        #{
           op => fcall,
           signature => [sig_element()],
           args => #{}
         }.

-type get_stmt() ::
        #{op => get, args => [[binary()] | non_neg_integer()]} |
        #{op => sget, args => [[binary() | '*'] | non_neg_integer()]}.

-type query_part() :: cmb_stmt() | flat_stmt().

-type cmb_stmt() ::
        {combine, dqe_fun(), [statement()] | get_stmt()}.

-type statement() ::
        get_stmt() |
        dqe_fun() |
        cmb_stmt().

-type flat_terminal() ::
        {combine, dqe_fun(), [flat_stmt() | get_stmt()]}.

-type flat_stmt() ::
        {calc, [dqe_fun()], flat_terminal() | get_stmt()}.

-type named() :: #{op => named, args => [binary() | flat_stmt()]}.

-type query_stmt() :: {named, binary(), flat_stmt()}.

-type parser_error() ::
        {error, binary()}.

-type alias() :: term().

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Takes a query, parses it and prepares it for the query
%% engine to execute.
%% @end
%%--------------------------------------------------------------------
-spec prepare(string()) ->
                     {error, term()} |
                     {ok, [query_stmt()], pos_integer()}.
prepare(S) ->
    case parse(S) of
        {ok, {select, Qs, Aliases, T}} ->
            dqe_lib:pdebug('parse', "Query parsed: ~s", [S]),
            extract_aliases(Qs, T, Aliases);
        E ->
            E
    end.

%%%===================================================================
%%% Parsing steps
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc Extract the alises from all query parts
%% @end
%%--------------------------------------------------------------------
-spec extract_aliases([statement()], time(), [term()]) ->
                     {error, term()} |
                     {ok, [query_stmt()], pos_integer()}.
extract_aliases(Qs, T, Aliases) ->
    AliasesF =
        lists:foldl(fun({alias, Alias, Res}, AAcc) ->
                            gb_trees:enter(Alias, Res, AAcc)
                    end, gb_trees:empty(), Aliases),
    dqe_lib:pdebug('parse', "Aliases resolved.", []),
    resolve_aliases(Qs, T, AliasesF).

%%--------------------------------------------------------------------
%% @private
%% @doc Resolves the aliases.
%% @end
%%--------------------------------------------------------------------
-spec resolve_aliases([statement()], time(), gb_trees:tree()) ->
                        {error, term()} |
                        {ok, [query_stmt()], pos_integer()}.
resolve_aliases(Qs, T, Aliases) ->
    {QQ, _AliasesQ} =
        lists:foldl(fun(Q, {QAcc, AAcc}) ->
                            {Q1, A1} = resolve_aliases(Q, AAcc),
                            {[Q1 | QAcc], A1}
                    end, {[], Aliases}, Qs),
    dqe_lib:pdebug('parse', "Preprocessor done.", []),
    QQ1 = lists:reverse(QQ),
    resolve_query_functions(QQ1, T).

%%--------------------------------------------------------------------
%% @private
%% @doc Apply types and resolve the functions called in the query
%% to dqe_fun functions.
%% @end
%%--------------------------------------------------------------------
-spec resolve_query_functions([statement()], time()) ->
                     {error, term()} |
                     {ok, [query_stmt()], pos_integer()}.
resolve_query_functions(Qs, T) ->
    case resolve_functions(Qs, []) of
        {ok, Qs1} ->
            flatten_step(Qs1, T);
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc Flattens the tree into a list of flat queries.
%% @end
%%--------------------------------------------------------------------
-spec flatten_step([statement()], time()) ->
                          {ok, [query_stmt()], pos_integer()}.
flatten_step(Qs, T) ->
    Qs1 = [flatten(Q) || Q <- Qs],
    dqe_lib:pdebug('parse', "Query flattened.", []),
    expand(Qs1, T).

%%--------------------------------------------------------------------
%% @private
%% @doc Expand lookup and glob.
%% @end
%%--------------------------------------------------------------------
-spec expand([flat_stmt()], time()) ->
                    {'ok',[query_stmt()], pos_integer()}.
expand(Qs, T) ->
    Qs1 = [expand(Q) || Q <- Qs],
    Qs2 = lists:flatten(Qs1),
    get_resolution(Qs2, T).

%%--------------------------------------------------------------------
%% @private
%% @doc Fetch the resolution for each sub query.
%% @end
%%--------------------------------------------------------------------
-spec get_resolution([flat_stmt()], time()) ->
                            %% {error, term()} |
                            {'ok',[{named, binary(), flat_stmt()}],
                             pos_integer()}.
get_resolution(Qs, T) ->
    {Qs1, _, _} = lists:foldl(fun get_resolution_fn/2,
                           {[], T, #{}}, Qs),
    Qs2 = lists:reverse(Qs1),
    propagate_resolutions(Qs2, T).

%%--------------------------------------------------------------------
%% @private
%% @doc calculate resolutions for the whole call stack.
%% @end
%%--------------------------------------------------------------------
propagate_resolutions(Qs, T) ->
    Qs1 = [apply_times(Q) || Q <- Qs],
    {Start, _End} = compute_se(apply_times(T, 1000), 1000),
    apply_names(Qs1, Start).

apply_names(Qs, Start) ->
    Qs1 = [update_name(Q) || Q <- Qs],
    {ok, Qs1, Start}.

update_name({named, L, C}) when is_list(L)->
    {Path, Gs} = extract_path_and_groupings(C),
    Name = [update_name_element(N, Path, Gs) || N <- L],
    {named, dql_unparse:unparse_metric(Name), C};

update_name({named, _N, _C} = Q) when is_binary(_N) ->
    Q;

update_name({named, _N, _C} = Q) ->
    io:format("~p~n", [Q]),
    Q.


update_name_element({dvar, N}, _Path, Gs) ->
    io:format("~p < ~p~n", [N, Gs]),
    {_, Name} = lists:keyfind(N, 1, Gs),
    Name;
update_name_element({pvar, N}, Path, _Gs) ->
    lists:nth(N, Path);
update_name_element(N, _, _) ->
    N.

extract_path_and_groupings(G = #{op := get, args := [_,_,_,_,Path]})
  when is_list(Path) ->
    io:format("~p~n", [G]),
    {Path, extract_groupings(G)};
extract_path_and_groupings(G = #{op := get, args := [_,_,_,_,Path]})
  when is_binary(Path) ->
    io:format("~p~n", [G]),
    {dproto:metric_to_list(Path), extract_groupings(G)};
extract_path_and_groupings({calc, _, G}) ->
    extract_path_and_groupings(G).

extract_groupings(#{groupings := Gs}) ->
    Gs;
extract_groupings(_) ->
    [].



%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc Run a string through the lexer and parser and return somewhat
%% sensible errors if requried.
%% @end
%%--------------------------------------------------------------------
-spec parse(string() | binary()) ->
                   parser_error() |
                   {ok, {select, [statement()], [alias()], range()}}.

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

parser_error(Line, E)  ->
    {error, list_to_binary(io_lib:format("Parser error in line ~p: ~s",
                                         [Line, E]))}.

lexer_error(Line, {illegal, E})  ->
    {error, list_to_binary(io_lib:format("Lexer error in line ~p illegal: ~s",
                                         [Line, E]))};

lexer_error(Line, E)  ->
    {error, list_to_binary(io_lib:format("Lexer error in line ~p: ~s",
                                         [Line, E]))}.


%%--------------------------------------------------------------------
%% @private
%% @doc Fetch the resulution for a single sub query.
%% @end
%%--------------------------------------------------------------------
-spec get_resolution_fn(named(),
                        {[named()], time(), #{}}) ->
                               {[named()], time(), #{}}.
get_resolution_fn(Q, {QAcc, T, #{} = RAcc}) when is_list(QAcc) ->
    {ok, Q1, RAcc1} = get_times(Q, T, RAcc),
    {[Q1 | QAcc], T, RAcc1}.

%%--------------------------------------------------------------------
%% @private
%% @doc Resolves the aliases
%% @end
%%--------------------------------------------------------------------
-spec resolve_aliases(statement(), gb_trees:tree()) -> {term(),
                                                        gb_trees:tree()}.

resolve_aliases(O = #{op  := fcall,
                     args := Args = #{inputs := Input}},
               Aliases) ->
    {Input1, A1} =
        lists:foldl(fun (Q, {QAcc, AAcc}) ->
                            {Qx, Ax} = resolve_aliases(Q, AAcc),
                            {[Qx | QAcc], Ax}
                    end, {[], Aliases}, Input),
    Input2 = lists:reverse(Input1),
    {O#{args => Args#{inputs => Input2}}, A1};
resolve_aliases(O = #{op := named, args := [N, Q]}, Aliases) ->
    {Q1, A1} = resolve_aliases(Q, Aliases),
    {O#{args => [N, Q1]}, A1};
resolve_aliases(#{op := var, args := [V]}, Aliases) ->
    {value, G} = gb_trees:lookup(V, Aliases),
    {G, Aliases};
resolve_aliases(O = #{}, Aliases) ->
    {O, Aliases};
resolve_aliases(N, A) when is_number(N)->
    {N, A}.

%%--------------------------------------------------------------------
%% @private
%% @doc Resolves functions based on their parameter types
%% @end
%%--------------------------------------------------------------------
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
            %% Determine the type of each argument
            Types = [T || #{return := T} <- Args1],
            %% Decide wather an argument is a constant (passed to init)
            %% or a nested function
            Args2 = [{C, is_constant(T)} || C = #{return := T} <- Args1],
            Constants = [C || {C, true} <- Args2],
            Inputs = [C || {C, false} <- Args2],
            %% Lookup if we know a function with the given types
            case dqe_fun:lookup(Function, Types) of
                %% If we find a function that does not take a
                %% list of sub functions we know this is a normal
                %%aggregate.
                {ok,{{_, _, none}, ReturnType, FunMod}} ->
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
                %% If we find one that takes a list of sub functions
                %% we know it is a combinator function.
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
                {error, not_found} ->
                    {error, {not_found, Function, Types}}
            end;
        E ->
            E
    end;
%% Thse are constatns, we don't need to resolve any further.
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


%%--------------------------------------------------------------------
%% @doc Determines if a type is a constant or sub function.
%% @end
%%--------------------------------------------------------------------
is_constant(metric) -> false;
is_constant(metric_list) -> false;
is_constant(histogram) -> false;
is_constant(histogram_list) -> false;
is_constant(_) -> true.

%%--------------------------------------------------------------------
%% @doc Add start and end times to a statment.
%% @end
%%--------------------------------------------------------------------
-spec get_times(named(), time(), #{}) ->
                       {ok, named(), #{}} |
                       {error, resolution_conflict}.
get_times(O = #{op := named, args := [N, C]}, T, #{} = BucketResolutions) ->
    case get_times_(C, T, BucketResolutions) of
        {ok, C1, BucketResolutions1} ->
            {ok, O#{args => [N, C1]}, BucketResolutions1};
        E ->
            E
    end.
-spec get_times_(flat_stmt(), time(), #{}) ->
                        {ok, flat_stmt(), #{}} |
                        {error, resolution_conflict}.
get_times_({calc, Chain,
            {combine,
             F = #{args := A = #{mod := FMod, constants := Cs}}, Elements}
           }, T, #{} = BucketResolutions) ->
    Res = lists:foldl(fun (E, {ok, Acc, #{} = BRAcc}) ->
                              case get_times_(E, T, BRAcc) of
                                  {ok, E1, BR1} ->
                                      {ok, [E1 | Acc], BR1};
                                  Error ->
                                      Error
                              end;
                          (_, E) ->
                              E
                      end, {ok, [], BucketResolutions}, Elements),
    case Res of
        {ok, Elements1, #{} = BR} ->
            Rmss = [get_resolution(E) || E <- Elements1],
            case lists:usort(Rmss) of
                [{ok, Rms}] ->
                    Elements2 = lists:reverse(Elements1),
                    Cs1 = [map_constants(C) || C <- Cs],
                    State = FMod:init(Cs1),
                    {Chunk, State1} = FMod:resolution(Rms, State),
                    Rms1 = Rms * Chunk,
                    A1 = A#{constants => Cs1, state => State1},
                    F1 = F#{args => A1,
                            resolution => Rms1},
                    Comb1 = {combine, F1, Elements2},
                    Calc1 = {calc, Chain, Comb1},
                    {ok, apply_times(Calc1), BR};
                _Error ->
                    io:format("Elements: ~p~n", [Elements]),
                    {error, resolution_conflict}
            end;
        {error, E} ->
            {error, E}
    end;

get_times_({calc, Chain, Get}, T, BucketResolutions) ->
    {ok, Get1 = #{args := A = [Rms | _]}, BucketResolutions1} =
        bucket_resolution(Get, BucketResolutions),
    T1 = apply_times(T, Rms),
    {Start, Count} = compute_se(T1, Rms),
    Calc1 = {calc, Chain, Get1#{resolution => Rms,
                                args => [Start, Count | A]}},
    {ok, apply_times(Calc1), BucketResolutions1}.

%%--------------------------------------------------------------------
%% @doc Look up resolution of a get statment.
%% @end
%%--------------------------------------------------------------------
-spec bucket_resolution(get_stmt(), #{}) ->
                               {ok, get_stmt(), #{}}.
bucket_resolution(O = #{args := A = [Bucket, _]}, BucketResolutions) ->
    {Res, BR1} = case maps:get(Bucket, BucketResolutions, undefined) of
                     undefined ->
                         {ok, R} = get_br(Bucket),
                         {R, maps:put(Bucket, R, BucketResolutions)};
                     R ->
                         {R, BucketResolutions}
                 end,
    {ok, O#{args => [Res | A]}, BR1}.

%%--------------------------------------------------------------------
%% @doc Fetch the resolution of a bucket from DDB
%% @end
%%--------------------------------------------------------------------
-spec get_br(binary()) -> {ok, pos_integer()}.
get_br(Bucket) ->
    ddb_connection:resolution(Bucket).

%%--------------------------------------------------------------------
%% @doc Resolves constants to their numeric representation
%% @end
%%--------------------------------------------------------------------
map_constants(T = #{op := time}) ->
    dqe_time:to_ms(T);
map_constants(#{op := integer, args := [N]}) ->
    N;
map_constants(#{op := float, args := [N]}) ->
    N;
map_constants(E) ->
    E.

%%--------------------------------------------------------------------
%% @doc Propagates resolution from the bottom of a call to the top.
%% @end
%%--------------------------------------------------------------------
get_resolution(#{op := O, args := [_Start, _Count, Rms | _]})
  when O =:= get;
       O =:= sget ->
    {ok, Rms};

get_resolution({calc, [], Get}) ->
    get_resolution(Get);

get_resolution({calc, Chain, _}) ->
    #{resolution := Rms} = lists:last(Chain),
    {ok, Rms};

get_resolution({combine, #{resolution := Rms}, _Elements}) ->
    {ok, Rms}.


apply_times(#{op := named, args := [N, C]}) ->
    C1 = apply_times(C),
    {named, N, C1};

apply_times({calc, Chain, {combine, F = #{resolution := Rms}, Elements}}) ->
    Elements1 = [apply_times(E) || E <- Elements],
    Chain1 = time_walk_chain(Chain, Rms, []),
    Chain2 = lists:reverse(Chain1),
    {calc, Chain2, {combine, F, Elements1}};

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
    Cs1 = [map_constants(C) || C <- Cs],
    State = FMod:init(Cs1),
    {Chunk, State1} = FMod:resolution(Rms, State),
    Rms1 = Rms * Chunk,
    A1 = A#{constants => Cs1, state => State1},
    time_walk_chain(R, Rms1, [E#{chunk => Chunk,
                                 resolution => Rms1,
                                 args => A1} | Acc]);

time_walk_chain([E | R], Rms, Acc) ->
    time_walk_chain(R, Rms, [E | Acc]).


-spec flatten(dqe_fun() | get_stmt()) ->
                     #{op => named, args => [binary() | flat_stmt()]}.
flatten(#{op := named, args := [N, Child]}) ->
    {C, R} = case flatten(Child, []) of
                 Cx = {calc, [#{return := Rx} | _], _} ->
                     {Cx, Rx};
                 Cx = {calc, [], #{return := Rx}} ->
                     {Cx, Rx}
             end,

    #{
       op => named,
       args => [N, C],
       signature => [string, R],
       return => R
     };

flatten(Child = #{return := R}) ->
    N = dql_unparse:unparse(Child),
    C = flatten(Child, []),
    #{
       op => named,
       args => [N, C],
       signature => [string, R],
       return => R
     }.

-spec flatten(statement(), [dqe_fun()]) ->
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
    {calc, Chain, Get}.

expand(Q) ->
    expand_grouped(Q, []).

expand_grouped(Q = #{op := named, args := [L, S]}, Groupings) when is_list(L) ->
    Gs = [N || {dvar, N} <- L],
    [Q#{args => [L, S1]} || S1 <- expand_grouped(S, Gs ++  Groupings)];

expand_grouped(Q = #{op := named, args := [N, S]}, Groupings) ->
    [Q#{args => [N, S1]} || S1 <- expand_grouped(S, Groupings)];

expand_grouped({calc, Fs, Q}, Groupings) ->
    [{calc, Fs, Q1} || Q1 <- expand_grouped(Q, Groupings)];

expand_grouped({combine, F, Qs}, Groupings) ->
    [{combine, F, lists:flatten([expand_grouped(Q, Groupings) || Q <- Qs])}];

expand_grouped(Q = #{op := get}, _Groupings) ->
    [Q];

expand_grouped(Q = #{op := lookup,
             args := [Collection, Metric, Where]}, []) ->
    {ok, BMs} = dqe_idx:lookup({in, Collection, Metric, Where}),
    Q1 = Q#{op := get},
    [Q1#{args => [Bucket, Key]} || {Bucket, Key} <- BMs];

expand_grouped(Q = #{op := lookup,
             args := [Collection, Metric, Where]}, Groupings) ->
    {ok, BMs} = dqe_idx:lookup({in, Collection, Metric, Where}, Groupings),
    Q1 = Q#{op := get},
    [Q1#{args => [Bucket, Key], groupings => lists:zip(Groupings, GVs)}
     || {Bucket, Key, GVs} <- BMs];

expand_grouped(Q = #{op := lookup,
             args := [Collection, Metric]}, []) ->
    {ok, BMs} = dqe_idx:lookup({in, Collection, Metric}),
    Q1 = Q#{op := get},
    [Q1#{args => [Bucket, Key]} || {Bucket, Key} <- BMs];

expand_grouped(Q = #{op := lookup,
             args := [Collection, Metric]}, Groupings) ->
    {ok, BMs} = dqe_idx:lookup({in, Collection, Metric}, Groupings),
    Q1 = Q#{op := get},
    [Q1#{args => [Bucket, Key], grouping => lists:zip(Groupings, GVs)}
     || {Bucket, Key, GVs} <- BMs];

expand_grouped(Q = #{op := sget,
             args := [Bucket, Glob]}, _Groupings) ->
    %% Glob is in an extra list since expand is build to use one or more
    %% globs
    {ok, {_Bucket, Ms}} = dqe_idx:expand(Bucket, [Glob]),
    Q1 = Q#{op := get},
    [Q1#{args => [Bucket, Key]} || Key <- Ms].


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
    erlang:min(1, (NowMs - dqe_time:to_ms(T)) div R);

apply_times(T, R) ->
    erlang:max(1, dqe_time:to_ms(T) div R).
