-module(dqe_funnel).

-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {buffer = [], refs = [], limit}).

init([Limit, SubQs])  ->
    SubQs1 = [{make_ref(), Q} || Q <- SubQs],
    Refs = [R || {R, _} <- SubQs1],
    {ok, #state{refs = Refs, limit = Limit}, SubQs1}.

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
apply_limit(Data, {_, N, _})
  when length(Data) =< N ->
    Data;
apply_limit(Data, {Direction, N, Mod}) ->
    Data1 = [calculate_limit_value(E, Mod) ||
                E <- Data],
    %% We don't need the ranking after we sort,
    Data2 = [E || {_, E} <- lists:sort(Data1)],
    take(Data2, Direction, N).

%% When we use top we want the 'biggest' elements that are
%% last in the list so we reverse the list and treat it as
%% bottom
take(Data, top, N) ->
    take(lists:reverse(Data), bottom, N);
take(Data, bottom, N) ->
    lists:sublist(Data, N).

calculate_limit_value({N, Data, R}, 
                      #{args := #{constants := Cs,
                                  %%inputs => [#{op => dummy,return => metric}],
                                  mod := Mod},
                        op := fcall}) ->
    io:format("Mod ~p", [Mod]),
    Cs1 = [C || C <- Cs, not is_map(C)],
    io:format("Cs ~p", [Cs1]),
    Count = mmath_bin:length_r(Data),
    S0 = Mod:init(Cs1 ++ [Count]),
    {_, S1} = Mod:resolution(1, S0),
    {V, _} = Mod:run([Data], S1),
    [V0] = mmath_bin:to_list(mmath_bin:derealize(V)),
    {V0, {N, Data, R}}.
