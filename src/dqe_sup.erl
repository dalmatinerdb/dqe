-module(dqe_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, {Host, Port}} = application:get_env(dqe, backend),
    {ok, PoolSize} = application:get_env(dqe, pool_size),
    {ok, PoolMax} = application:get_env(dqe, pool_max),
    Name = backend_connection,
    SizeArgs = [
                {size, PoolSize},
                {max_overflow, PoolMax}
               ],
    PoolArgs = [{name, {local, Name}},
                {worker_module, dalmatiner_connection}] ++ SizeArgs,
    WorkerArgs = [Host, Port],
    {ok, {{one_for_one, 5, 10},
          [poolboy:child_spec(Name, PoolArgs, WorkerArgs)]}}.

