-module(dqe_hfun2).
-behaviour(dflow).

-export([describe/1, init/1, start/2, emit/3, done/2]).

init([Aggr, V, SubQ]) ->
    {ok, {Aggr, V}, SubQ}.

describe({Aggr, V}) ->
    [atom_to_list(Aggr), "(", float_to_binary(V), ")"].

start({_Start, _Count}, Aggr) ->
    {ok, Aggr}.

emit(_Child, {histogram, {Histogram, Resolution}}, {Aggr, V}) ->
    Vs = [hdr_histogram:Aggr(H, V) || H <- Histogram],
    Data = mmath_bin:realize(mmath_bin:from_list(Vs)),
    {emit, {realized, {Data, Resolution}}, {Aggr, V}}.

done({last, _Child}, Aggr) ->
    {done, Aggr}.
