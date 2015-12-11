%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 23 Jun 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(dalmatiner_connection).

-behaviour(gen_server).
-behaviour(poolboy_worker).

%% API
-export([start_link/1, get/4, list/2, list/1, list/0]).
-ignore_xref([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(TIMEOUT, 30000).
-define(MAX_COUNT, 604800).
-record(state, {connection, metrics=gb_trees:empty(), host, port,
                max_read = ?MAX_COUNT}).

%%%===================================================================
%%% API
%%%===================================================================

get(Bucket, Metric, Time, Count) ->
    Worker = worker({get, Bucket, Metric, Time, Count}),
    poolboy:transaction(backend_connection, Worker, ?TIMEOUT).

list(Bucket) ->
    poolboy:transaction(backend_connection,
                        fun(Worker) ->
                                gen_server:call(Worker, {list, Bucket}, ?TIMEOUT)
                        end, ?TIMEOUT).
list(Bucket, Prefix) ->
    poolboy:transaction(backend_connection,
                        fun(Worker) ->
                                gen_server:call(Worker, {list, Bucket, Prefix}, ?TIMEOUT)
                        end, ?TIMEOUT).

list() ->
    poolboy:transaction(backend_connection,
                        fun(Worker) ->
                                gen_server:call(Worker, list, ?TIMEOUT)
                        end, ?TIMEOUT).

worker(Call) ->
    fun(Worker) ->
            gen_server:call(Worker, Call, ?TIMEOUT)
    end.
%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Host, Port]) ->
	{ok, MaxRead} = application:get_env(dqe, max_read),
    {ok, #state{max_read=MaxRead, host = Host, port = Port}, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({get, _, _, _, Count}, _From, State = #state{max_read = MaxRead})
  when Count > MaxRead ->
	{reply, {error, too_big}, State};

handle_call({get, Bucket, Metric, Time, Count}, _From,
            State = #state{connection = C}) ->
    case ddb_tcp:get(Bucket, Metric, Time, Count, C) of
        {ok, {D, Resolution}, C1} ->
            {reply, {ok, D, Resolution}, State#state{connection = C1}};
        {error, E, C1} ->
            {reply, {error, E}, State#state{connection = C1}}
    end;

handle_call({list, Bucket}, _From, State) ->
    case gb_trees:lookup(Bucket, State#state.metrics) of
        none ->
            {Ms, State1} = do_list(Bucket, State),
            {reply, {ok, Ms}, State1};
        {value, {LastRead, Ms}} ->
            Now = erlang:system_time(seconds),
            case Now - LastRead of
                T when T > 60  ->
                    {Ms, State1} = do_list(Bucket, State),
                    {reply, {ok, Ms}, State1};
                _ ->
                    {reply, {ok, Ms}, State}
            end
    end;

handle_call({list, Bucket, <<>>}, _From, State = #state{connection = C}) ->
    {ok, Ms, C1} = ddb_tcp:list(Bucket, C),
    {reply, {ok, Ms}, State#state{connection = C1}};

handle_call({list, Bucket, Prefix}, _From, State = #state{connection = C}) ->
    {ok, Ms, C1} = ddb_tcp:list(Bucket, Prefix, C),
    {reply, {ok, Ms}, State#state{connection = C1}};

handle_call(list, _From, State = #state{connection = C}) ->
    case ddb_tcp:list(C) of
        {ok, Bs, C1} ->
            {reply, {ok, Bs}, State#state{connection = C1}};
        {error, E, C1} ->
            {reply, {error, E}, State#state{connection = C1}}
    end;


handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State = #state{host = Host, port = Port}) ->
    {ok, C} = ddb_tcp:connect(Host, Port),
    {noreply, State#state{connection = C}};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #state{connection = C}) ->
    _ = ddb_tcp:close(C),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


do_list(Bucket, State = #state{connection = C}) ->
    {ok, Ms, C1} = ddb_tcp:list(Bucket, C),
    Tree1 = gb_trees:enter(Bucket, {erlang:system_time(seconds), Ms},
                           State#state.metrics),
    {Ms, State#state{metrics = Tree1, connection = C1}}.
