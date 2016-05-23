-module(dqe_trans1).
-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

init([Op, SubQ]) ->
    {ok, Op, SubQ}.

describe(Op) ->
    atom_to_list(Op).

start({_Start, _Count}, Op) ->
    {ok, Op}.

emit(_Child, {realized, {Data, Resolution}}, Op) ->
    {emit, {realized, {mmath_trans:Op(Data), Resolution}}, Op}.

done({last, _Child}, Aggr) ->
    {done, Aggr}.
