-module(dqe_percentile_aggr).
-behaviour(dqe_fun).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2,
         help/0]).

-record(state, {
          time :: pos_integer(),
          value :: float(),
          count :: pos_integer()
         }).

init([Value, Time]) when is_integer(Value) ->
    init([Value * 0.5, Time]);
init([Value, Time]) when is_integer(Time) ->
    #state{value = Value, time = Time}.

chunk(#state{time = Time}) ->
    Time * ?RDATA_SIZE.

resolution(Resolution, State = #state{time = Time}) ->
    Res = dqe_time:apply_times(Time, Resolution),
    {Res, State#state{count = Res}}.

describe(#state{time = Time}) ->
    ["percentile(", integer_to_list(Time), "ms)"].

spec() ->
    {<<"percentile">>, [metric, float, time], none, metric}.

run([Data], S = #state{value = V, count = Count}) ->
    {mmath_aggr:percentile(Data, V, Count), S}.

help() ->
    <<"Calculates a percentile over a given period of time.">>.
