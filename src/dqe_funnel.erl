-module(dqe_funnel).

-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {}).

init([SubQs])  ->
    {ok, #state{}, SubQs}.

describe(_) ->
    "funnel".

start({_Start, _Count}, State) ->
    {ok, State}.

emit(_Child, Data, State) ->
    {emit, Data, State}.

done({last, _}, State) ->
    {done, State};

done(_, State) ->
    {ok, State}.
