-module(dqe_collect).

-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {
          acc  = <<>>:: binary(),
          name :: binary(),
          resolution :: pos_integer(),
          mdata :: [{binary(), binary()}]
         }).

init([Name, MData, SubQ]) ->
    init([Name, MData, -1, SubQ]);
init([Name, MData, Resolution, SubQ]) when not is_list(SubQ) ->
    {ok, #state{name = Name, resolution = Resolution, mdata = MData}, SubQ}.

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

done(_Child, State = #state{resolution = Resolution, name = Name, acc = Acc,
                            mdata = MData}) ->
    {done, #{
       name => Name,
       data => mmath_bin:derealize(Acc),
       resolution => Resolution,
       metadata => maps:from_list(MData),
       type => metrics},
     State#state{acc = <<>>}}.
