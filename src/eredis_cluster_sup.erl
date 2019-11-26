-module(eredis_cluster_sup).
-behaviour(supervisor).

%% Supervisor.
-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()}.
start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([])
    -> {ok, {{supervisor:strategy(), 1, 5}, [supervisor:child_spec()]}}.
init([]) ->
  PoolSpec = init_pool(),
  MonitorSpec = init_cluster(),
  {ok, {{one_for_one, 1, 5}, PoolSpec ++ MonitorSpec}}.

init_pool() ->
  [
    {eredis_cluster_pool,
      {eredis_cluster_pool, start_link, []},
      permanent,
      5000,
      supervisor,
      [dynamic]}
  ].

init_cluster() ->
  Pools = application:get_env(eredis, pools, []),
  init_cluster(Pools, []).

init_cluster([{Name, Opts}|Rest], Acc) ->
  ClusterName = eredis_cluster:get_cluster_name(Name),
  ClusterSpec = {ClusterName,
                  {eredis_cluster_monitor, start_link, [Name, Opts]},
                  permanent,
                  5000,
                  worker,
                  [dynamic]},
  init_cluster(Rest, [ClusterSpec|Acc]);
init_cluster([], Acc) ->
  lists:reverse(Acc).


