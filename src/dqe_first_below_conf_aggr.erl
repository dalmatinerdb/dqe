-module(dqe_first_below_conf_aggr).
-behaviour(dqe_fun).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2, help/0]).

-record(state, {
          time :: pos_integer(),
          first :: pos_integer() | undefined,
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
    {Res, State#state{first = Res}}.

describe(#state{const = Const, time = Time})->
    ["first_below_conf(", float_to_list(Const), ", ",
     integer_to_list(Time), ",s)"].

spec() ->
    [{<<"first_below_conf">>, [metric, integer, time], none, metric},
     {<<"first_below_conf">>, [metric, float, time], none, metric}].

run([Data], S = #state{const = Const, first = First}) ->
    {mmath_aggr:first_below_conf(Data, Const, First), S}.

help() ->
    <<"Returns the first value below a given confidence threshold.">>.
