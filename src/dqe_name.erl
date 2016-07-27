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

start(_, State) ->
    {ok, State}.

emit(_C, Data, State = #state{name = Name}) ->
    {emit, {Name, Data}, State}.

done(_Child, State) ->
    {done, State}.
