-module(dqe_sqrt_scale_trans).
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
    ["sqrt_scale()"].

spec() ->
    {<<"sqrt_scale">>, [metric], none, metric}.

run([Data], S = #state{}) ->
    {mmath_trans:sqrt_scale(Data), S}.

help() ->
    <<"Calculates the square root of each element. For convinience: ",
      "sqrt_scale(0) = 0, sqrt_scale(-N) = - sqrt_scale(N).">>.
