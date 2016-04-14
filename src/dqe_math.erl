-module(dqe_math).
-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).
-record(state, {
          op :: atom(),
          arg :: number()
         }).

init([Op, SubQ, Arg]) ->
    {ok, #state{op = Op, arg = Arg}, SubQ}.

describe(#state{op = Op, arg = Arg}) when is_float(Arg) ->
    [atom_to_list(Op), "(", float_to_list(Arg), ")"];

describe(#state{op = Op, arg = Arg}) when is_integer(Arg) ->
    [atom_to_list(Op), "(", integer_to_list(Arg), ")"].

start({_Start, _Count}, State) ->
    {ok, State}.

emit(_Child, {realized, {Data, Resolution}}, State = #state{op = Op, arg = Arg}) ->
    {emit, {realized, {mmath_aggr:Op(Data, Arg), Resolution}}, State}.

done({last, _Child}, Aggr) ->
    {done, Aggr}.
