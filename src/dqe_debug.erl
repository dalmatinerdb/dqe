-module(dqe_debug).

-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {start = erlang:system_time(milli_seconds)}).

init([SubQ]) when not is_list(SubQ) ->
    {ok, #state{}, SubQ}.

describe(_) ->
    "debug".

start({_Start, _Count}, State) ->
    {ok, State}.

emit(Child, {realized, {Name, Data, Resolution}}, State) ->
    lager:debug("[dqe|~p:~s] ~p~n", [Child, Name,
                                     mmath_bin:to_list(mmath_bin:derealize(Data))]),
    {emit, {Name, Data, Resolution}, State};

emit(Child, {points, {Name, Data, Resolution}}, State) ->
    lager:debug("[dqe|~p:~s] ~p~n", [Child, Name,
                                     mmath_bin:to_list(Data)]),
    {emit, {Name, Data, Resolution}, State};

emit(Child, {realized, {Data, Resolution}}, State) ->
    lager:debug("[dqe|~p] ~p~n", [Child, mmath_bin:to_list(mmath_bin:derealize(Data))]),
    {emit, {Data, Resolution}, State};

emit(Child, {points, {Data, Resolution}}, State) ->
    lager:debug("[dqe|~p] ~p~n", [Child, mmath_bin:to_list(Data)]),
    {emit, {Data, Resolution}, State}.

done({last, Child}, State = #state{start = Start}) ->
    Diff  = Start - erlang:system_time(milli_seconds),
    lager:debug("[dqe|~p] Finished after ~pms.~n", [Child, Diff]),
    {done, State}.
