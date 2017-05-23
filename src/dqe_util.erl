-module(dqe_util).

-export([get_pool/1]).

get_pool(default) ->
    ddb_connection:pool();
get_pool({pool, P}) ->
    P.
