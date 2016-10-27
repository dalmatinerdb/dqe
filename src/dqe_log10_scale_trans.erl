-module(dqe_log10_scale_trans).
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
    ["log10_scale()"].

spec() ->
    {<<"log10_scale">>, [metric], none, metric}.

run([Data], S = #state{}) ->
    {mmath_trans:log10_scale(Data), S}.

help() ->
    <<"Calculates the log10 of each element. For convinience: ",
      "log10_scale(0) = 0, log10_scale(-N) = - log10_scale(N).">>.
