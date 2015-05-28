-module(dqe_funnel).

-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {buffer = [], refs = []}).

init([SubQs])  ->
    SubQs1 = [{make_ref(), Q} || Q <- SubQs],
    Refs = [R || {R, _} <- SubQs1],
    io:format("SubQs: ~p~n", [SubQs1]),
    io:format("Refs: ~p~n", [Refs]),
    {ok, #state{refs = Refs}, SubQs1}.

describe(_) ->
    "funnel".

start({_Start, _Count}, State) ->
    {ok, State}.

emit(Child, Data, State = #state{buffer = Buffer}) ->
    State1 = State#state{buffer = orddict:store(Child, Data, Buffer)},
    io:format("State1: ~p~n", [State1]),
    {ok, State1}.


done({last, _}, State = #state{buffer = B, refs = Rs}) ->
    Data = [orddict:fetch(R, B) || R <- Rs],
    io:format("Data: ~p~n", [Data]),
    {done, Data, State};

done(O, State) ->
    io:format("Done: ~p~n", [O]),
    {ok, State}.
