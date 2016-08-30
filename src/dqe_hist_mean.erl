-module(dqe_hist_mean).
-behaviour(dqe_fun).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2, help/0]).

-record(state, {}).

init([]) ->
    #state{}.

chunk(#state{}) ->
    1.

resolution(_Resolution, State) ->
    {1, State}.

describe(_) ->
    ["mean()"].

spec() ->
    {<<"mean">>, [histogram], none, metric}.

run([Data], S) ->
    R = dqe_hist:compute(fun hdr_histogram:mean/1, Data),
    {R, S}.

help() ->
    <<"Calculates the mean value for a histogram.">>.
