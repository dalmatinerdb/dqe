-module(dqe_ddb_lookup).
-behaviour(dqe_lookup).

-export([lookup/1]).


-spec lookup(dqe_lookup:query()) -> {ok, {binary(), binary()}}.
lookup({B, M}) ->
    {ok, [{B, dproto:metric_from_list(M)}]};
lookup({B, M, _Where}) ->
    {ok, [{B, dproto:metric_from_list(M)}]}.
