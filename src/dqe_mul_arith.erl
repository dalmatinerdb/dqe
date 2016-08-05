-module(dqe_mul_arith).
-behaviour(dqe_fun).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2, help/0]).

-record(state, {
          const :: number()
         }).

init([Const]) when is_number(Const) ->
    #state{const = Const}.

chunk(#state{const = Const}) ->
    Const * ?RDATA_SIZE.

resolution(_Resolution, State) ->
    {1, State}.

describe(#state{const = Const}) when is_integer(Const)->
    ["mul(", integer_to_list(Const), ")"];
describe(#state{const = Const}) when is_float(Const)->
    ["mul(", float_to_list(Const), ")"].

spec() ->
    [{<<"mul">>, [metric, integer], none, metric},
     {<<"mul">>, [metric, float], none, metric}].

run([Data], S = #state{const = Const}) ->
    {mmath_trans:mul(Data, Const), S}.

help() ->
    <<"Multiplies each element of the series with a constant. This is "
      "equivalent to the infix opperator * when the right argument is a "
      "number.">>.

