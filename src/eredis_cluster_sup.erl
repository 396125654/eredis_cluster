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
  ClusterOpts = get_cluster_opts(),
  init_cluster(ClusterOpts, []).

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

get_cluster_opts() ->
  AllOpts = application:get_all_env(eredis_cluster),
  lists:filter(
    fun({included_applications, _}) -> false;
       ({_, Opts}) when is_list(Opts) -> lists:keymember(nodes, 1, Opts);
       (_) -> false
    end, AllOpts).
