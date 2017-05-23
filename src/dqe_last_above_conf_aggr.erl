-module(dqe_last_above_conf_aggr).
-behaviour(dqe_fun).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2, help/0]).

-record(state, {
          time :: pos_integer(),
          last :: pos_integer() | undefined,
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
    {Res, State#state{last = Res}}.

describe(#state{const = Const, time = Time})->
    ["last_above_conf(", float_to_list(Const), ", ",
     integer_to_list(Time), ",s)"].

spec() ->
    [{<<"last_above_conf">>, [metric, integer, time], none, metric},
     {<<"last_above_conf">>, [metric, float, time], none, metric}].

run([Data], S = #state{const = Const, last = Last}) ->
    {mmath_aggr:last_above_conf(Data, Const, Last), S}.

help() ->
    <<"Returns the last value above a given confidence threashold.">>.
