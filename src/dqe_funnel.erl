-module(dqe_funnel).

-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {buffer = [], refs = []}).

init([SubQs])  ->
    SubQs1 = [{make_ref(), Q} || Q <- SubQs],
    Refs = [R || {R, _} <- SubQs1],
    {ok, #state{refs = Refs}, SubQs1}.

describe(_) ->
    "funnel".

start({_Start, _Count}, State) ->
    {ok, State}.

emit(Child, Data, State = #state{buffer = Buffer}) ->
    State1 = State#state{buffer = orddict:store(Child, Data, Buffer)},
    {ok, State1}.


done({last, _}, State = #state{buffer = []}) ->
    {done, {error, no_results}, State};

done({last, _}, State = #state{buffer = B, refs = Rs}) ->
    Data = [case orddict:find(R, B) of
                error -> [];
                {ok, V} -> V
            end || R <- Rs],
    {done, Data, State};

done(_O, State) ->
    {ok, State}.
