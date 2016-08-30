-module(dqe_hist_max).
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
    ["max()"].

spec() ->
    {<<"max">>, [histogram], none, metric}.

run([Data], S) ->
    R = mmath_bin:from_list([hdr_histogram:max(H) || H <- Data]),
    {R, S}.

help() ->
    <<"Calculates the maximum value for a histogram.">>.
