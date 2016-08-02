-module(dqe_min_comb).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2, help/0]).

-record(state, {
         }).

init([]) ->
    #state{}.

chunk(#state{}) ->
    1.

resolution(_Resolution, State) ->
    {1, State}.

describe(#state{}) ->
    ["min()"].

spec() ->
    {<<"min">>, [], metric, metric}.

run(Datas, S) ->
    {mmath_comb:min(Datas), S}.

help() ->
    <<"Combines multiple series into one by picking the smallest value of each "
      "time offset.">>.
