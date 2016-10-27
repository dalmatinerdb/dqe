-module(dqe_abs_trans).
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
    ["abs()"].

spec() ->
    {<<"abs">>, [metric], none, metric}.

run([Data], S = #state{}) ->
    {mmath_trans:abs(Data), S}.

help() ->
    <<"Calculates the absolute of each element.">>.
