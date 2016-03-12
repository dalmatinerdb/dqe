-module(dqe_name).

-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {
          name
         }).

init([Name, SubQ]) ->
    {ok, #state{name = Name}, SubQ}.

describe(#state{name = Name}) ->
    Name.

start({_Start, _Count}, State) ->
    {ok, State}.

emit(_C, {realized, {Data, Resolution}}, State = #state{name = Name}) ->
    {emit, {realized, {Name, Data, Resolution}}, State}.

done(_Child, State) ->
    {done, State}.


