-module(dqe_hist).
-behaviour(dqe_fun).

-include_lib("mmath/include/mmath.hrl").

-export([spec/0, describe/1, init/1, chunk/1, resolution/2, run/2, help/0]).

-record(state, {
          time :: pos_integer(),
          count :: pos_integer(),
          htv :: pos_integer(),
          sf :: pos_integer(),
          acc = <<>> :: binary()
         }).

init([HTV, SF, Time]) when is_integer(Time) ->
    #state{time = Time, htv = HTV, sf = SF}.

chunk(#state{time = Time}) ->
    Time * ?RDATA_SIZE.

resolution(Resolution, State = #state{time = Time}) ->
    Res = dqe_time:apply_times(Time, Resolution),
    {Res, State#state{count = Res}}.

describe(#state{time = Time}) ->
    ["histogram(", integer_to_list(Time), "ms)"].

spec() ->
    {<<"histogram">>, [metric, integer, integer, time], none, histogram}.

run([Data], S = #state{htv = HTV, sf = SF, count = Count, acc = Acc}) ->
    Acc1 = <<Acc/binary, Data/binary>>,
    {Acc2, Hists} = execute(HTV, SF, Acc1, Count, []),
    {Hists, S#state{acc = Acc2}}.

help() ->
    <<"Converts measurement points of a timerange into a histogram. "
      "The arguments are a metric, the highest trackable value, "
      "and the significant figures (1..5).">>.

execute(HTV, SF, Acc, Count, AccEmit)
  when byte_size(Acc) >= Count * ?RDATA_SIZE ->
    MinSize = Count * ?RDATA_SIZE,
    <<Data:MinSize/binary, Acc1/binary>> = Acc,
    H = mk_hist(HTV, SF, Data),
    execute(HTV, SF, Acc1, Count, [H | AccEmit]);

execute(_HTV, _SF, Acc, _, AccEmit) ->
    {Acc, lists:reverse(AccEmit)}.

mk_hist(HVT, SF, Data) ->
    {ok, H} = hdr_histogram:open(HVT, SF),
    Values = mmath_bin:to_list(mmath_bin:derealize(Data)),
    [hdr_histogram:record(H, round(V)) || V <- Values],
    H.
