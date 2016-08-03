-module(dqe_sub_arith).
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
    ["sub(", integer_to_list(Const), ")"];
describe(#state{const = Const}) when is_float(Const)->
    ["sub(", float_to_list(Const), ")"].

spec() ->
    [{<<"sub">>, [metric, integer], none, metric},
     {<<"sub">>, [metric, float], none, metric}].

run([Data], S = #state{const = Const}) ->
    {mmath_trans:sub(Data, Const), S}.

help() ->
    <<"Subtracts a constant from each element of the series. This is "
      "equivalent to the infix opperator - when the right argument is a "
      "number.">>.

