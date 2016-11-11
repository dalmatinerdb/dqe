-module(dqe_divide_arith).
-behaviour(dqe_fun).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2, help/0]).

-record(state, {
          const :: number()
         }).

init([Const]) when is_number(Const), Const /= 0  ->
    #state{const = Const}.

chunk(#state{}) ->
    ?RDATA_SIZE.

resolution(_Resolution, State) ->
    {1, State}.

describe(#state{const = Const}) when is_integer(Const)->
    ["divide(", integer_to_list(Const), ")"];
describe(#state{const = Const}) when is_float(Const)->
    ["divide(", float_to_list(Const), ")"].

spec() ->
    [{<<"divide">>, [metric, integer], none, metric},
     {<<"divide">>, [metric, float], none, metric}].

run([Data], S = #state{const = Const}) ->
    {mmath_trans:divide(Data, Const), S}.

help() ->
    <<"Divides each element of the series with a constant. This is "
      "equivalent to the infix opperator / when the right argument is a "
      "number.">>.
