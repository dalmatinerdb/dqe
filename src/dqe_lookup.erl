-module(dqe_lookup).

-export([lookup/1]).

-type where() :: {binary(), binary()} |
                 {'and', where(), where()} |
                 {'or', where(), where()}.
-type query() :: dql:bm() | 
                 {binary(), [binary()], where()}.

-callback lookup(query()) -> {ok, {binary(), binary()}}.


-spec lookup(query()) -> {ok, [{binary(), binary()}]}.
lookup(Query) ->
    LE = application:get_env(dqe, lookup_module, dqe_ddb_lookup),
    LE:lookup(Query).
