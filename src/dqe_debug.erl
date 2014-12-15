-module(dqe_debug).

-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {start = now()}).

init([SubQ]) when not is_list(SubQ) ->
    {ok, #state{}, SubQ}.

describe(_) ->
    "debug".

start({_Start, _Count}, State) ->
    {ok, State}.

emit(Child, {Name, Data, Resolution}, State) ->
    io:format("[~p:~s] ~p~n", [Child, Name, mmath_bin:to_list(Data)]),
    {emit, {Name, Data, Resolution}, State};

emit(Child, {Data, Resolution}, State) ->
    io:format("[~p] ~p~n", [Child, mmath_bin:to_list(Data)]),
    {emit, {Data, Resolution}, State}.

done({last, Child}, State = #state{start = Start}) ->
    Diff  = round(timer:now_diff(now(), Start) / 1000),
    io:format("[~p] Finished after ~pms.~n", [Child, Diff]),
    {done, State}.
