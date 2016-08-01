-module(dqe_avg_comb).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2]).

-record(state, {
         }).

init([]) ->
    #state{}.

chunk(#state{}) ->
    1.

resolution(_Resolution, State) ->
    {1, State}.

describe(#state{}) ->
    ["avg()"].

spec() ->
    {<<"avg">>, [], metric, metric}.

run(Datas, S) ->
    {mmath_comb:avg(Datas), S}.
