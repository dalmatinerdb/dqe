-module(dqe_derivate).
-behaviour(dqe_fun).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2, help/0]).

-record(state, {
         }).

init([]) ->
    #state{}.

chunk(#state{}) ->
    ?RDATA_SIZE.

resolution(_Resolution, State) ->
    {1, State}.

describe(#state{}) ->
    ["derivate()"].

spec() ->
    {<<"derivate">>, [metric], none, metric}.

run([Data], S = #state{}) ->
    {mmath_trans:derivate(Data), S}.

help() ->
    <<"Calculates the derivate of a series so that v(t) = v(t - 1) - v(t). As "
      "this loses one element of the series the first element v(0) and v(1) "
      "are always equal.">>.
