-module(dqe_funnel).

-behaviour(dflow).

-export([init/2, describe/1, start/2, emit/3, done/2]).

-record(state, {buffer = [], refs = [], limit}).

init([Limit], SubQRefs)  ->
    {ok, #state{refs = SubQRefs, limit = Limit}}.

describe(_) ->
    "funnel".

start(_, State) ->
    {ok, State}.

emit(Child, Data, State = #state{buffer = Buffer}) ->
    State1 = State#state{buffer = orddict:store(Child, Data, Buffer)},
    {ok, State1}.


done({last, _}, State = #state{buffer = []}) ->
    {done, {error, no_results}, State};

done({last, _}, State = #state{buffer = B, refs = Rs, limit = Limit}) ->
    Data = [case orddict:find(R, B) of
                error -> [];
                {ok, V} -> V
            end || R <- Rs],
    {done, apply_limit(Data, Limit), State};

done(_O, State) ->
    {ok, State}.

apply_limit(Data, undefined) ->
    Data;
apply_limit(Data, {Direction, N, Mod}) ->
    case [E || E = #{type := metrics} <- Data] of
        _Metrics when length(_Metrics) =< N ->
            Data;
        Metrics ->
            Events = [E || E = #{type := events} <- Data],
            Metrics1 = [calculate_limit_value(E, Mod) ||
                           E <- Metrics],
            %% We don't need the ranking after we sort,
            Metrics2 = [E || {_, E} <- lists:sort(Metrics1)],
            take(Metrics2 ++ Events, Direction, N)
    end.

%% When we use top we want the 'biggest' elements that are
%% last in the list so we reverse the list and treat it as
%% bottom
take(Data, top, N) ->
    take(lists:reverse(Data), bottom, N);
take(Data, bottom, N) ->
    lists:sublist(Data, N).

calculate_limit_value(#{data := Data} = M,
                      #{args := #{constants := Cs,
                                  %%inputs => [#{op => dummy,return => metric}],
                                  mod := Mod},
                        op := fcall}) ->
    Cs1 = [to_v(C) || C <- Cs],
    Cs2 = [C || C <- Cs1, C =/= undefined],
    Count = mmath_bin:length_r(Data),
    S0 = Mod:init(Cs2 ++ [Count]),
    {_, S1} = Mod:resolution(1, S0),
    {V, _} = Mod:run([Data], S1),
    [V0] = mmath_bin:to_list(mmath_bin:derealize(V)),
    {V0, M}.


to_v(#{op := float, args := [V]}) ->
    V;
to_v(#{op := integer, args := [V]}) ->
    V;
to_v(_) ->
    undefined.
