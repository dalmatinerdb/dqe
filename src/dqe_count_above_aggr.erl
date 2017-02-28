-module(dqe_count_above_aggr).
-behaviour(dqe_fun).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2, help/0]).

-record(state, {
          time :: pos_integer(),
          count :: pos_integer() | undefined,
          const :: number()
         }).

init([Const, Time]) when is_integer(Const) ->
    #state{const = Const*1.0,
           time = Time};
init([Const, Time]) when is_float(Const) ->
    #state{const = Const,
           time = Time}.

chunk(#state{time = Time}) ->
    Time * ?RDATA_SIZE.

resolution(Resolution, State = #state{time = Time}) ->
    Res = dqe_time:apply_times(Time, Resolution),
    {Res, State#state{count = Res}}.

describe(#state{const = Const, time = Time})->
    ["count_above(", float_to_list(Const), ", ", integer_to_list(Time), ",s)"].

spec() ->
    [{<<"count_above">>, [metric, integer, time], none, metric},
     {<<"count_above">>, [metric, float, time], none, metric}].

run([Data], S = #state{const = Const, count = Count}) ->
    {mmath_aggr:count_above(Data, Const, Count), S}.

help() ->
    <<"Returns the count of value above a given threashold.">>.
