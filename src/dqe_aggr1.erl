-module(dqe_aggr1).
-behaviour(dflow).

-export([describe/1, init/1, start/2, emit/3, done/2]).

init([Aggr, SubQ]) ->
    {ok, Aggr, SubQ}.

describe(Aggr) ->
    atom_to_list(Aggr).

start({_Start, _Count}, Aggr) ->
    {ok, Aggr}.

emit(_Child, {Data, Resolution}, Aggr) ->
    {emit, {mmath_aggr:Aggr(Data), Resolution}, Aggr}.

done({last, _Child}, Aggr) ->
    {done, Aggr}.
