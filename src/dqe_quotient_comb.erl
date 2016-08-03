-module(dqe_quotient_comb).

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
    ["quotient()"].

spec() ->
    {<<"quotient">>, [], metric, metric}.

run(Datas, S) ->
    {mmath_comb:quotient(Datas), S}.

help() ->
    <<"Combines quotienttiple series into one by deviding the values with "
      "each time offset. It should be noted that a division by zero is "
      "treated as a devision by one.">>.
