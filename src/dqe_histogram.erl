-module(dqe_histogram).

-behaviour(dflow).

-include_lib("mmath/include/mmath.hrl").

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {
          htv :: pos_integer(),
          sf :: pos_integer(),
          time :: pos_integer(),
          acc = <<>> :: binary(),
          resolution :: pos_integer()
         }).

init([HTV, SF, SubQ, Time]) ->
    {ok, #state{htv = HTV, sf = SF, time = Time}, SubQ}.

start({_Start, _Count}, State) ->
    {ok, State}.

describe(#state{htv = HTV, sf = SF, time = Time}) ->
    ["histogram(", integer_to_list(HTV), integer_to_list(SF),
     integer_to_list(Time), "ms)"].

%% When we get the first data we can calculate both the applied
%% time and the upwards resolution.
emit(Child, {realized, {Data, Resolution}},
     State = #state{resolution = undefined, time = Time}) ->
    Time1 = dqe_time:apply_times(Time, Resolution),
    emit(Child, {realized, {Data, Resolution}},
         State#state{resolution = Time1 * Resolution, time = Time1});

emit(_Child, {realized, {Data, _R}},
     State = #state{htv = HTV, sf = SF, acc = Acc, time = Time}) ->
    case execute(HTV, SF, <<Data/binary, Acc/binary>>, Time, []) of
        {Acc1, []} ->
            {ok, State#state{acc = Acc1}};
        {Acc2, Histograms} ->
            Histograms1 = lists:reverse(Histograms),
            {emit, {histograms, {Histograms1, State#state.resolution}},
             State#state{acc = Acc2}}
    end.


done(_Child, State = #state{acc = <<>>}) ->
    {done, State};

done(_Child, State = #state{htv = HTV, sf = SF, acc = Acc}) ->
    Data = mk_hist(HTV, SF, Acc),
    {done, {histograms, {[Data], State#state.resolution}}, State#state{acc = <<>>}}.


execute(HTV, SF, Acc, T1, AccEmit) when byte_size(Acc) >= T1 * ?DATA_SIZE ->
    MinSize = T1 * ?DATA_SIZE,
    <<Data:MinSize/binary, Acc1/binary>> = Acc,
    H = mk_hist(HTV, SF, Data),
    execute(HTV, SF, Acc1, T1, [H | AccEmit]);

execute(_HTV, _SF, Acc, _, AccEmit) ->
    {Acc, AccEmit}.

mk_hist(HVT, SF, Data) ->
    {ok, H} = hdr_histogram:open(HVT, SF),
    _ = [ hdr_histogram:record(H, V)
          || V <- mmath_bin:to_list(mmath_bin:derealize(Data))],
    H.
