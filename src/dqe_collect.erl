-module(dqe_collect).

-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {
          acc  = <<>>:: binary(),
          name :: binary(),
          resolution :: pos_integer()
         }).

init([Name, SubQ]) ->
    init([Name, -1, SubQ]);
init([Name, Resolution, SubQ]) when not is_list(SubQ) ->
    {ok, #state{name = Name, resolution = Resolution}, SubQ}.

describe(_) ->
    "collect".

start(_, State) ->
    {ok, State}.

%% When we get the first data we can calculate both the applied
%% time and the upwards resolution.

emit(_C, Data, State = #state{acc = Acc})
  when is_binary(Data) ->
    {ok, State#state{acc = <<Acc/binary, Data/binary>>}}.

done(_Child, State = #state{resolution = undefined}) ->
    {done, State};

done(_Child, State = #state{resolution = Resolution, name = Name, acc = Acc}) ->
    {done, {Name, mmath_bin:derealize(Acc), Resolution},
     State#state{acc = <<>>}}.
