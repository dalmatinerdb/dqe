-module(dqe_derivate).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2]).

-record(state, {
         }).

init([]) ->
    #state{}.

chunk(#state{}) ->
    ?RDATA_SIZE.

resolution(Resolution, #state{}) ->
    Resolution.


describe(#state{}) ->
    ["derivate()"].

spec() ->
    {<<"derivate">>, [metric], none, metric}.

run([Data], S = #state{}) ->
    {mmath_trans:derivate(Data), S}.
