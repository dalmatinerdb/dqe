%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@project-fifo.net>
%%% @copyright (C) 2016, Heinz Nikolaus Gies
%%% @doc
%%% Resolution related functionality
%%% @end
%%% Created :  2 Aug 2016 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(dql_resolution).

-export([resolve/2, propagate/1, start_time/1]).

resolve(Qs, T) ->
    case lists:foldl(fun get_resolution_fn/2, {[], T, #{}}, Qs) of
        {error, E} ->
            {error, E};
        {Qs1, _, _} ->
            {ok, lists:reverse(Qs1)}
    end.

propagate(Qs)->
    [apply_times(Q) || Q <- Qs].

start_time(T) ->
    {Start, _End} = compute_se(apply_times(T, 1000), 1000),
    Start.

%%--------------------------------------------------------------------
%% @private
%% @doc Fetch the resulution for a single sub query.
%% @end
%%--------------------------------------------------------------------
-spec get_resolution_fn(dql:named(),
                        {[dql:named()], dql:time(), #{}} |
                        {error, resolution_conflict}) ->
                               {[dql:named()], dql:time(), #{}} |
                               {error,resolution_conflict}.
get_resolution_fn(Q, {QAcc, T, #{} = RAcc}) when is_list(QAcc) ->
    case get_times(Q, T, RAcc) of
        {ok, Q1, RAcc1} ->
            {[Q1 | QAcc], T, RAcc1};
        {error, resolution_conflict} ->
            {error, resolution_conflict}
    end;
get_resolution_fn(_, {error, resolution_conflict}) ->
    {error, resolution_conflict}.

%%--------------------------------------------------------------------
%% @doc Add start and end times to a statment.
%% @end
%%--------------------------------------------------------------------
-spec get_times(dql:named(), dql:time(), #{}) ->
                       {ok, dql:named(), #{}} |
                       {error, resolution_conflict}.
get_times(O = #{op := named, args := [N, C]}, T, #{} = BucketResolutions) ->
    case get_times_(C, T, BucketResolutions) of
        {ok, C1, BucketResolutions1} ->
            {ok, O#{args => [N, C1]}, BucketResolutions1};
        E ->
            E
    end.

-spec get_times_(dql:flat_stmt(), dql:time(), #{}) ->
                        {ok, dql:flat_stmt(), #{}} |
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

get_times_({calc, Chain, E = #{op := events}}, T, BucketResolutions) ->
    C1 = {calc, Chain, E#{times => T}},
    {ok, C1, BucketResolutions};
get_times_({calc, Chain, Get}, T, BucketResolutions) ->
    {ok, Get1 = #{args := A = [Rms | _]}, BucketResolutions1} =
        bucket_resolution(Get, BucketResolutions),
    T1 = apply_times(T, 1000),
    {Start, Count} = compute_se(T1, Rms),
    Calc1 = {calc, Chain, Get1#{resolution => Rms,
                                args => [Start, Count | A]}},
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

%%--------------------------------------------------------------------
%% @doc Compute start and endtime of a query.
%% @end
%%--------------------------------------------------------------------

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

compute_se(#{op := timeshift, args := [Shift, T]}, Rms) ->
    {S, D} = compute_se(T, Rms),
    {S - Shift, D};

compute_se(#{op :='after', args := [S, D]}, _Rms) ->
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

apply_times(#{op := named, args := [N, C]}) ->
    C1 = apply_times(C),
    {named, N, C1};

apply_times({calc, Chain, {combine, F = #{resolution := Rms}, Elements}}) ->
    Elements1 = [apply_times(E) || E <- Elements],
    Chain1 = time_walk_chain(Chain, Rms, []),
    Chain2 = lists:reverse(Chain1),
    {calc, Chain2, {combine, F, Elements1}};

apply_times({calc, Chain, Q = #{op := events, times := T}}) ->
    {StartMs, CountMs} = compute_se(apply_times(T, 1), 1),
    Start = erlang:convert_time_unit(StartMs, milli_seconds, nano_seconds),
    Count = erlang:convert_time_unit(CountMs, milli_seconds, nano_seconds),
    End = Start + Count,
    {calc, Chain, Q#{times => [Start, End]}};

apply_times({calc, Chain, Get}) ->
    {ok, Rms} = get_resolution(Get),
    Chain1 = time_walk_chain(Chain, Rms, []),
    Chain2 = lists:reverse(Chain1),
    {calc, Chain2, Get}.


