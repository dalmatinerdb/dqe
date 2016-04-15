-module(dqe_get).

-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {
          bucket :: binary(),
          metric :: binary(),
          chunk :: pos_integer()
         }).

init([Bucket, Metric]) when is_binary(Metric) ->
    {ok, Chunk} = application:get_env(dqe, get_chunk),
    init([Bucket, Metric, Chunk]);

init([Bucket, Metric, Chunk]) ->
    {ok, #state{bucket = Bucket, metric = Metric, chunk = Chunk}, []}.

describe(#state{bucket = Bucket, metric = Metric}) ->
    [Bucket, "/", Metric].

start({_Start, 0}, State) ->
    {done, State};

start({Start, Count},
      State = #state{bucket = Bucket, metric = Metric, chunk = Chunk}) when
      Count >= Chunk ->
    %% We do a bit of cheating here this allows us to loop.
    case ddb_connection:get(Bucket, Metric, Start, Chunk) of
        {error, _Error} ->
            {done, State};
        {ok, Res, <<>>} ->
            dflow:start(self(), {Start + Chunk, Count - Chunk}),
            {emit, {realized, {mmath_bin:empty(Chunk), Res}}, State};
        {ok, Res, Data} ->
            dflow:start(self(), {Start + Chunk, Count - Chunk}),
            {emit, {realized, {mmath_bin:realize(Data), Res}}, State}
    end;

start({Start, Count}, State = #state{bucket = Bucket, metric = Metric}) ->
    case ddb_connection:get(Bucket, Metric, Start, Count) of
        {error, _Error} ->
            {done, State};
        {ok, Res, <<>>} ->
            {done, {realized, {mmath_bin:empty(Count), Res}}, State};
        {ok, Res, Data} ->
            {done, {realized, {mmath_bin:realize(Data), Res}}, State}
    end.

emit(_Child, _Data, State) ->
    {ok, State}.

done(_, State) ->
    {done, State}.
