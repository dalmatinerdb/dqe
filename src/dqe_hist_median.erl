-module(dqe_hist_median).
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
    ["median()"].

spec() ->
    {<<"median">>, [histogram], none, metric}.

run([Data], S) ->
    R = mmath_bin:from_list([hdr_histogram:median(H) || H <- Data]),
    {R, S}.

help() ->
    <<"Calculates the median value for a histogram.">>.
