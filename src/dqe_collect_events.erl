-module(dqe_collect_events).

-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {
          acc  = [] :: [maps:map()],
          mdata :: [{binary(), binary()}],
          name :: binary()
         }).

init([Name, MData, SubQ]) when not is_list(SubQ) ->
    {ok, #state{mdata = MData, name = Name}, SubQ}.

describe(_) ->
    "collect_events".

start(_, State) ->
    {ok, State}.

%% When we get the first data we can calculate both the applied
%% time and the upwards resolution.

emit(_C, Data, State = #state{acc = Acc})
  when is_list(Data) ->
    {ok, State#state{acc = [Data | Acc]}}.

done(_Child, State = #state{name = Name, acc = Acc, mdata = MData}) ->
    {done, #{name => Name,
             data => lists:flatten(Acc),
             metadata => maps:from_list(MData),
             resolution => 1.0e-6,
             type => events},
     State#state{acc = []}}.
