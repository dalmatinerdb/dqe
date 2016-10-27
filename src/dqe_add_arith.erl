-module(dqe_add_arith).
-behaviour(dqe_fun).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2, help/0]).

-record(state, {
          const :: number()
         }).

init([Const]) when is_number(Const) ->
    #state{const = Const}.

chunk(#state{}) ->
    ?RDATA_SIZE.

resolution(_Resolution, State) ->
    {1, State}.

describe(#state{const = Const}) when is_integer(Const)->
    ["add(", integer_to_list(Const), ")"];
describe(#state{const = Const}) when is_float(Const)->
    ["add(", float_to_list(Const), ")"].

spec() ->
    [{<<"add">>, [metric, integer], none, metric},
     {<<"add">>, [metric, float], none, metric}].

run([Data], S = #state{const = Const}) ->
    {mmath_trans:add(Data, Const), S}.

help() ->
    <<"Adds a constant to each element of the series. This is equivalent to "
      "the infix opperator + when the right argument is a number.">>.
