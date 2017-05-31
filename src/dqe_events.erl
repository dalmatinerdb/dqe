-module(dqe_events).
-behaviour(dflow).

-export([init/2, describe/1, start/2, emit/3, done/2]).

-record(state, {
          bucket      :: binary(),
          start       :: non_neg_integer(),
          'end'       :: non_neg_integer(),
          filter = [],
          chunk       :: pos_integer()
         }).

init([Bucket, Start, End, Filter], []) ->
    {ok, ChunkMs} = application:get_env(dqe, get_chunk),
    Chunk = erlang:convert_time_unit(ChunkMs, milli_seconds, nano_seconds),
    {ok, #state{start = Start, 'end' = End, bucket = Bucket, filter = Filter,
                chunk = Chunk}}.

describe(#state{bucket = Bucket}) ->
    [Bucket].

start(run,
      State = #state{start = Start, 'end' = End, chunk = Chunk,
                     bucket = Bucket, filter = Filter}) when
      Start + Chunk < End  ->
    %% We do a bit of cheating here this allows us to loop.
    State1 = State#state{start = Start + Chunk},
    case ddb_connection:read_events(Bucket, Start, End, Filter) of
        {error, _Error} ->
            {done, State1};
        {ok, Events} ->
            dflow:start(self(), run),
            {emit, Events, State1}
    end;

start(run, State = #state{start = Start, 'end' = End,
                          bucket = Bucket, filter = Filter}) ->
    case ddb_connection:read_events(Bucket, Start, End, Filter) of
        {error, _Error} ->
            {done, State};
        {ok, Data} ->
            {done, Data, State}
    end.

emit(_Child, _Data, State) ->
    {ok, State}.

done(_, State) ->
    {done, State}.
