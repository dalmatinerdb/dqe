%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@project-fifo.net>
%%% @copyright (C) 2016, Heinz Nikolaus Gies
%%% @doc
%%% Resolution related functionality
%%% @end
%%% Created :  2 Aug 2016 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(dql_resolution).

-export([resolve/1, propagate/1, time_range/1]).

resolve(Qs) ->
    case lists:foldl(fun get_resolution_fn/2, {[], #{}}, Qs) of
        {error, E} ->
            {error, E};
        {Qs1, _} ->
            {ok, lists:reverse(Qs1)}
    end.

propagate(Qs)->
    Qs1 = [apply_times(Q) || Q <- Qs],
    case lists:foldl(fun get_start/2, undefined, Qs1) of
        {error, E} ->
            {error, E};
        Start ->
            {ok, Qs1, Start}
    end.

time_range(T) ->
    T1 = apply_times(T, 1),
    {Start, Count} = compute_sc(T1),
    {Start, Start+Count}.

%%--------------------------------------------------------------------
%% @private
%% @doc Fetch the resulution for a single sub query.
%% @end
%%--------------------------------------------------------------------
-spec get_resolution_fn(dql:named(),
                        {[dql:named()], #{}} |
                        {error, resolution_conflict}) ->
                               {[dql:named()], #{}} |
                               {error, no_results} |
                               {error, resolution_conflict}.
get_resolution_fn(Q, {QAcc, #{} = RAcc}) when is_list(QAcc) ->
    case get_times(Q, RAcc) of
        {ok, Q1, RAcc1} ->
            {[Q1 | QAcc], RAcc1};
        {error, no_results} ->
            {error, no_results};
        {error, resolution_conflict} ->
            {error, resolution_conflict}
    end;
get_resolution_fn(_, {error, no_results}) ->
    {error, no_results};
get_resolution_fn(_, {error, resolution_conflict}) ->
    {error, resolution_conflict}.

%%--------------------------------------------------------------------
%% @doc Add start and end times to a statment.
%% @end
%%--------------------------------------------------------------------
-spec get_times(dql:named(), #{}) ->
                       {ok, dql:named(), #{}} |
                       {error, no_results} |
                       {error, resolution_conflict}.
get_times(O = #{op := named, args := [N, M, C]},
          #{} = BucketResolutions) ->
    case get_times_(C, 0, BucketResolutions) of
        {ok, C1, BucketResolutions1} ->
            {ok, O#{args => [N, M, C1]}, BucketResolutions1};
        E ->
            E
    end.

-spec get_times_(dql:flat_stmt(), integer(), #{}) ->
                        {ok, dql:flat_stmt(), #{}} |
                        {error, no_results} |
                        {error, resolution_conflict}.

get_times_({calc, _Chain, {combine, F = #{}, [] = _Elements}},
           _Shift, #{} = _BucketResolutions) ->
    dqe_lib:pdebug(prepare, "No input for combiner Fn: ~p", [F]),
    {error, no_results};

get_times_({calc, Chain,
            {combine,
             F = #{args := A = #{mod := FMod, constants := Cs}}, Elements}
           }, Shift, #{} = BucketResolutions) ->
    Res = lists:foldl(fun (E, {ok, Acc, #{} = BRAcc}) ->
                              case get_times_(E, Shift, BRAcc) of
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
                    {error, resolution_conflict}
            end;
        {error, E} ->
            {error, E}
    end;

get_times_(#{op := timeshift,
             args := [NewShift, C]}, Shift, BucketResolutions) ->
    {ok, C1, BucketResolutions1} =
        get_times_(C, Shift + NewShift, BucketResolutions),
    {ok, C1, BucketResolutions1};

get_times_({calc, Chain, E = #{op := events}}, Shift, BucketResolutions) ->
    C1 = {calc, Chain, E#{shift => Shift}},
    {ok, C1, BucketResolutions};
get_times_({calc, Chain, Get}, Shift, BucketResolutions) ->
    {ok, Get1 = #{ranges := Ranges,
                  resolution := Rms}, BucketResolutions1} =
        bucket_resolution(Get, BucketResolutions),
    Ranges1 = [{(Start - Shift) div Rms, (End - Shift) div Rms, Endpoint}
               || {Start, End, Endpoint} <- Ranges],
    Calc1 = {calc, Chain, Get1#{resolution => Rms,
                                ranges => Ranges1}},
    {ok, apply_times(Calc1), BucketResolutions1}.

%%--------------------------------------------------------------------
%% @doc Walksa  callchain and applies times to all the functions
%% @end
%%--------------------------------------------------------------------

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
%% @doc Look up resolution of a get statment.
%% @end
%%--------------------------------------------------------------------

-spec bucket_resolution(dql:get_stmt(), #{}) ->
                               {ok, dql:get_stmt(), #{}}.
bucket_resolution(O = #{args := [Bucket| _],
                       ranges := Ranges},
                  BucketResolutions)
  when is_binary(Bucket)->
    Endpoint = extract_endpoint(Ranges),
    {Res, BR1} = case  maps:get({Endpoint, Bucket},
                                BucketResolutions, undefined) of
                     undefined ->
                         {ok, R} = get_br(Endpoint, Bucket),
                         {R, maps:put({Endpoint, Bucket}, R,
                                      BucketResolutions)};
                     R ->
                         {R, BucketResolutions}
                 end,
    {ok, O#{resolution => Res}, BR1}.

extract_endpoint([{_, _, null} | R]) ->
    extract_endpoint(R);
extract_endpoint([{_, _, E} | _]) ->
    E.

%%--------------------------------------------------------------------
%% @doc Fetch the resolution of a bucket from DDB
%% @end
%%--------------------------------------------------------------------
-spec get_br(term(), binary()) -> {ok, pos_integer()}.
get_br(Endpoint, Bucket) ->
    ddb_connection:resolution(dqe_util:get_pool(Endpoint), Bucket).

%%--------------------------------------------------------------------
%% @doc Propagates resolution from the bottom of a call to the top.
%% @end
%%--------------------------------------------------------------------
get_resolution(#{op := O, resolution := Rms})
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

%%--------------------------------------------------------------------
%% @doc Compute start and endtime of a query.
%% @end
%%--------------------------------------------------------------------



compute_sc(#{ op := between, args := [S, E]}) when E > S->
    {S, E - S};

compute_sc(#{ op := between, args := [S, E]}) ->
    {E, S - E};

compute_sc(#{ op := last, args := [N]}) ->
    NowMs = erlang:system_time(milli_seconds),
    {NowMs - N, N};

compute_sc(#{op := before, args := [E, D]}) ->
    {E - D, D};

compute_sc(#{op := timeshift, args := [Shift, T]}) ->
    {S, D} = compute_sc(T),
    {S - Shift, D};

compute_sc(#{op :='after', args := [S, D]}) ->
    {S, D}.

%%--------------------------------------------------------------------
%% @doc Applies time computations to a query range
%% @end
%%--------------------------------------------------------------------

apply_times(#{op := last, args := [L]}, R) ->
    #{op => last, args => [apply_times(L, R)]};

apply_times(#{op := between, args := [S, E]}, R) ->
    #{op => between, args => [apply_times(S, R), apply_times(E, R)]};

apply_times(#{op := 'after', args := [S, D]}, R) ->
    #{op => 'after', args => [apply_times(S, R), apply_times(D, R)]};

apply_times(#{op := before, args := [E, D]}, R) ->
    #{op => before, args => [apply_times(E, R), apply_times(D, R)]};

apply_times(#{op := timeshift, args := [Shift, T]}, R) ->
    T1 = apply_times(T, R),
    S1 = apply_times(Shift, R),
    #{op => timeshift, args => [S1, T1]};


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

%%--------------------------------------------------------------------
%% @doc Applies times to parts of the query logic
%% @end
%%--------------------------------------------------------------------

apply_times(#{op := named, args := [N, M, C]}) ->
    C1 = apply_times(C),
    {named, N, M, C1};

apply_times({calc, Chain, {combine, F = #{resolution := Rms}, Elements}}) ->
    Elements1 = [apply_times(E) || E <- Elements],
    Chain1 = time_walk_chain(Chain, Rms, []),
    Chain2 = lists:reverse(Chain1),
    {calc, Chain2, {combine, F, Elements1}};

apply_times({calc, Chain, Q = #{op := events, ranges := [{Start, End, _}]}}) ->
    {calc, Chain, Q#{times => [Start, End]}};

apply_times({calc, Chain, Get}) ->
    {ok, Rms} = get_resolution(Get),
    Chain1 = time_walk_chain(Chain, Rms, []),
    Chain2 = lists:reverse(Chain1),
    {calc, Chain2, Get}.

%%--------------------------------------------------------------------
%% @doc Fold function to find astual data start time
%% @end
%%--------------------------------------------------------------------

get_start(_, {error, _} = Error) ->
    Error;

get_start({named, _N, _M, Chain}, Start) ->
    get_start(Chain, Start);

get_start({calc, _, {combine, _F, Elements}}, Start) ->
    lists:foldl(fun (El, S) ->
                        case get_start(El, S) of
                            {error, E} ->
                                {error, E};
                            S1 when S =:= undefined ->
                                S1;
                            S ->
                                S;
                            _ ->
                                {error, resolution_conflict}
                        end
                end, Start, Elements);

get_start({calc, _, #{times := [Start, _End]}}, undefined) ->
    Start;
get_start({calc, _, #{times := [Start, _End]}}, ExpectedStart)
      when Start =:= ExpectedStart->
    Start;
get_start({calc, _, #{times := [_Start, _End]}}, _ExpectedStart) ->
    {error, resolution_conflict};

get_start({calc, _, #{resolution := Rms,
                      ranges := Ranges}}, Start) ->
    Offset = lists:min([S || {S, _, _} <- Ranges]),
    DataStart = Offset * Rms,
    case Start of
        undefined ->
            DataStart;
        DataStart ->
            DataStart;
        _ ->
            {error, resolution_conflict}
    end.
