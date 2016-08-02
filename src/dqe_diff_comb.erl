-module(dqe_diff_comb).

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
    ["diff()"].

spec() ->
    {<<"diff">>, [], metric, metric}.

run(Datas, S) ->
    {mmath_comb:diff(Datas), S}.

help() ->
    <<"Combines multiple series into one by substracting the values for each "
      "time offset together.">>.
