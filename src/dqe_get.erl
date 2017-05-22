-module(dqe_get).
-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {
          bucket :: binary(),
          key :: binary(),
          ranges :: [{non_neg_integer(), non_neg_integer(), term()}],
          chunk :: pos_integer(),
          logged = false :: boolean()
         }).

init([Ranges, Bucket, Key]) ->
    {ok, Chunk} = application:get_env(dqe, get_chunk),
    init([Ranges, Bucket, Key, Chunk]);
init([Ranges, Bucket, KeyL, Chunk]) when is_list(KeyL)->
    Key = dproto:metric_from_list(KeyL),
    init([Ranges, Bucket, Key, Chunk]);
init([Ranges, Bucket, Key, Chunk]) ->
    Ranges1 = rearrange_ranges(Ranges, Chunk, []),
    {ok, #state{ranges = Ranges1, bucket = Bucket, key = Key,
                chunk = Chunk}, []}.

rearrange_ranges([], _Chunk, Acc) ->
    lists:reverse(Acc);

rearrange_ranges([{Start, End, _} = E | Rest], Chunk, Acc)
  when End - Start =< Chunk ->
    rearrange_ranges(Rest, Chunk, [E | Acc]);

rearrange_ranges([{Start, End, Endpoint} | Rest], Chunk, Acc) ->
    NewEnd = Start + Chunk - 1,
    Acc1 = [{Start, NewEnd, Endpoint} | Acc],
    NewIn = [{NewEnd + 1, End, Endpoint} | Rest],
    rearrange_ranges(NewIn, Chunk,  Acc1).

describe(#state{bucket = Bucket, key = Key}) ->
    [Bucket, "/", dproto:metric_to_string(Key, <<".">>)].

start(run, State = #state{ranges = []}) ->
    {done, State};

start(run, State = #state{logged = false,
                          ranges = [{Start, _End, _Endpoint} | _],
                          bucket = Bucket, key = Key}) ->
    %% TODO = count
    dflow_span:tag(bucket, Bucket),
    dflow_span:tag(metric, Key),
    dflow_span:tag(start, Start),
    start(run, State#state{logged = true});

start(run,
      State = #state{ranges = [{Start, End, null} | Rest]})  ->
    Count = End - Start,
    dflow_span:log("null ~p @ ~p", [Count, Start]),
    %% We do a bit of cheating here this allows us to loop.
    State1 = State#state{ranges = Rest},
    {emit, mmath_bin:realize(mmath_bin:empty(Count)), State1};

start(run,
      State = #state{ranges = [{Start, End, default} | Rest]})  ->
    Ranges1 = [{Start, End, {pool, ddb_connection:pool()}} | Rest],
    start(run, State#state{ranges = Ranges1});

start(run,
      State = #state{ranges = [{Start, End, {pool, Pool}} | Rest],
                     bucket = Bucket, key = Key})  ->
    Count = End - Start,
    dflow_span:log("read ~p @ ~p", [Count, Start]),
    %% We do a bit of cheating here this allows us to loop.
    State1 = State#state{ranges = Rest},
    case ddb_connection:get(Pool, Bucket, Key, Start, Count,
                            ottersp:get_span()) of
        {error, _Error} ->
            dflow_span:log("read failed"),
            {done, State};
        {ok, <<>>} ->
            dflow_span:log("empty result"),
            dflow:start(self(), run),
            {emit, mmath_bin:realize(mmath_bin:empty(Count)), State1};
        {ok, Data} ->
            dflow_span:log("read ~p datapoints, ~p bytes",
                           [mmath_bin:length(Data), byte_size(Data)]),
            dflow:start(self(), run),
            {emit, mmath_bin:realize(Data), State1}
    end.


emit(_Child, _Data, State) ->
    {ok, State}.

done(_, State) ->
    {done, State}.
