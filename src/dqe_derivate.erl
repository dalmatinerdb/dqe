-module(dqe_derivate).
-behaviour(dqe_fun).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2]).

-record(state, {
         }).

init([]) ->
    #state{}.

chunk(#state{}) ->
    ?RDATA_SIZE.

resolution(Resolution, State) ->
    {Resolution, State}.


describe(#state{}) ->
    ["derivate()"].

spec() ->
    {<<"derivate">>, [metric], none, metric}.

run([Data], S = #state{}) ->
    {mmath_trans:derivate(Data), S}.
