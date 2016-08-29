-module(dqe_get_fun).
-behaviour(dflow).

-export([init/1, describe/1, start/2, emit/3, done/2]).

-record(state, {
          bucket :: binary(),
          key :: binary(),
          start :: non_neg_integer(),
          count :: non_neg_integer(),
          get_chunk :: pos_integer(),

          dqe_fun :: atom(),
          chunk :: pos_integer(),
          acc = <<>> :: binary(),
          fun_state :: dqe_fun:fun_state()

         }).

init([Fun, FunState, Start, Count, Resolution, Bucket, Key]) ->
    {ok, Chunk} = application:get_env(dqe, get_chunk),
    init([Fun, FunState, Start, Count, Resolution, Bucket, Key, Chunk]);
init([Fun, FunState, Start, Count, Resolution, Bucket, KeyL, GetChunk])
  when is_list(KeyL)->
    Key = dproto:metric_from_list(KeyL),
    init([Fun, FunState, Start, Count, Resolution, Bucket, Key, GetChunk]);
init([Fun, FunState, Start, Count, _Resolution, Bucket, Key, GetChunk]) ->
    Chunk = Fun:chunk(FunState),
    {ok, #state{start = Start, count = Count, bucket = Bucket, key = Key,
                get_chunk = GetChunk, dqe_fun = Fun, chunk = Chunk,
                fun_state = FunState}, []}.

describe(#state{bucket = Bucket, key = Key}) ->
    [Bucket, "/", Key].

start(run, State = #state{count = 0}) ->
    {done, State};

start(run,
      State = #state{start = Start, count = Count, get_chunk = Get_Chunk,
                     bucket = Bucket, key = Key}) when
      Count >= Get_Chunk ->
    %% We do a bit of cheating here this allows us to loop.
    State1 = State#state{start = Start + Get_Chunk, count = Count - Get_Chunk},
    case ddb_connection:get(Bucket, Key, Start, Get_Chunk) of
        {error, _Error} ->
            {done, State1};
        {ok, <<>>} ->
            dflow:start(self(), run),
            do(State1, mmath_bin:realize(mmath_bin:empty(Count)), emit);
        {ok, Data} ->
            dflow:start(self(), run),
            do(State1, mmath_bin:realize(Data), emit)
    end;

start(run, State = #state{start = Start, count = Count,
                     bucket = Bucket, key = Key}) ->
    case ddb_connection:get(Bucket, Key, Start, Count) of
        {error, _Error} ->
            {done, State};
        {ok, <<>>} ->
            do(State, mmath_bin:realize(mmath_bin:empty(Count)), done);
        {ok, Data} ->
            do(State, mmath_bin:realize(Data), done)
    end.


do(State = #state{dqe_fun = Fun, fun_state = FunState,
                  chunk = ChunkSize, acc = Acc}, Data, Action)
  when byte_size(Acc) + byte_size(Data) >= ChunkSize ->
    Size = ((byte_size(Acc) + byte_size(Data)) div ChunkSize) * ChunkSize,
    <<ToCompute:Size/binary, Acc1/binary>> = <<Acc/binary, Data/binary>>,
    {Result, FunState1} = Fun:run([ToCompute], FunState),
    {Action, Result, State#state{fun_state = FunState1, acc = Acc1}};

do(State = #state{acc = Acc}, Data, emit) ->
    {ok, State#state{acc = <<Acc/binary, Data/binary>>}};

do(State = #state{dqe_fun = Fun, fun_state = FunState, acc = Acc},
   Data, Action) ->
    {Result, FunState1} = Fun:run([<<Acc/binary, Data/binary>>], FunState),
    {Action, Result, State#state{acc = <<>>, fun_state = FunState1}}.

emit(_Child, _Data, State) ->
    {ok, State}.

done(_, State) ->
    {done, State}.
