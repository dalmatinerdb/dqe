-module(dqe_first_below_aggr).
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
    ["first_below(", float_to_list(Const), ", ", integer_to_list(Time), ",s)"].

spec() ->
    [{<<"first_below">>, [metric, integer, time], none, metric},
     {<<"first_below">>, [metric, float, time], none, metric}].

run([Data], S = #state{const = Const, count = Count}) ->
    {mmath_aggr:first_below(Data, Const, Count), S}.

help() ->
    <<"Returns the first value below a given threshold.">>.
