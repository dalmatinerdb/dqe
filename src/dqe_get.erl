-module(dqe_get).
-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {
          bucket :: binary(),
          key :: binary(),
          start :: non_neg_integer(),
          count :: non_neg_integer(),
          chunk :: pos_integer()
         }).

init([Start, Count, Resolution, Bucket, Key]) ->
    {ok, Chunk} = application:get_env(dqe, get_chunk),
    init([Start, Count, Resolution, Bucket, Key, Chunk]);
init([Start, Count, Resolution, Bucket, KeyL, Chunk]) when is_list(KeyL)->
    Key = dproto:metric_from_list(KeyL),
    init([Start, Count, Resolution, Bucket, Key, Chunk]);
init([Start, Count, _Resolution, Bucket, Key, Chunk]) ->
    {ok, #state{start = Start, count = Count, bucket = Bucket, key = Key,
                chunk = Chunk}, []}.

describe(#state{bucket = Bucket, key = Key}) ->
    [Bucket, "/", dproto:metric_to_string(Key, <<".">>)].

start(run, State = #state{count = 0}) ->
    {done, State};

start(run,
      State = #state{start = Start, count = Count, chunk = Chunk,
                     bucket = Bucket, key = Key}) when
      Count >= Chunk ->
    %% We do a bit of cheating here this allows us to loop.
    State1 = State#state{start = Start + Chunk, count = Count - Chunk},
    case ddb_connection:get(Bucket, Key, Start, Chunk) of
        {error, _Error} ->
            {done, State};
        {ok, <<>>} ->
            dflow:start(self(), run),
            {emit, mmath_bin:realize(mmath_bin:empty(Chunk)), State1};
        {ok, Data} ->
            dflow:start(self(), run),
            {emit, mmath_bin:realize(Data), State1}
    end;

start(run, State = #state{start = Start, count = Count,
                     bucket = Bucket, key = Key}) ->
    case ddb_connection:get(Bucket, Key, Start, Count) of
        {error, _Error} ->
            {done, State};
        {ok, <<>>} ->
            {done, mmath_bin:realize(mmath_bin:empty(Count)), State};
        {ok, Data} ->
            {done, mmath_bin:realize(Data), State}
    end.

emit(_Child, _Data, State) ->
    {ok, State}.

done(_, State) ->
    {done, State}.
