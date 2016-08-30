-module(dqe_hist_percentile).
-behaviour(dqe_fun).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2, help/0]).

-record(state, {percentile}).

init([Percentile]) ->
    #state{percentile = Percentile}.

chunk(#state{}) ->
    1.

resolution(_Resolution, State) ->
    {1, State}.

describe(#state{percentile = Percentile}) ->
    ["percentile(", float_to_list(Percentile), ")"].

spec() ->
    {<<"percentile">>, [histogram, float], none, metric}.

run([Data], S = #state{percentile = Percentile}) ->
    R = dqe_hist:compute(fun hdr_histogram:percentile/2, Percentile, Data),
    {R, S}.

help() ->
    <<"Calculates the percentile value for a histogram.">>.
