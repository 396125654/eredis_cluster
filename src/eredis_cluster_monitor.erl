-module(eredis_cluster_monitor).
-behaviour(gen_server).

%% API.
-export([start_link/0, start_link/2]).
-export([connect/2]).
-export([refresh_mapping/2]).
-export([get_state/1, get_state_version/1]).
-export([get_pool_by_slot/2, get_pool_by_slot/3]).
-export([get_all_pools/1]).

%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

%% Type definition.
-include("eredis_cluster.hrl").
-record(state, {nodes       :: [#node{}],
                slots       :: tuple(), %% whose elements are integer indexes into slots_maps
                slots_maps  :: tuple(), %% whose elements are #slots_map{}
                version :: integer()}).

-type state() :: #state{}.

%% API.
-spec start_link() -> {ok, pid()}.
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_link(Name, Opts) ->
  MonitorName = eredis_cluster:get_cluster_name(Name),
  gen_server:start_link({local, MonitorName}, ?MODULE, [Name, Opts], []).

connect(Name, Nodes) ->
  Pools = application:get_env(eredis, pools, []),
  Opts = proplists:get_value(Name, Pools, []),
  MonitorName = eredis_cluster:get_cluster_name(Name),
  gen_server:call(MonitorName, {connect, Name, Nodes, Opts}).

refresh_mapping(Name, Version) ->
  Pools = application:get_env(eredis, pools, []),
  Opts = proplists:get_value(Name, Pools, []),
  MonitorName = eredis_cluster:get_cluster_name(Name),
  gen_server:call(MonitorName, {reload_slots_map, Name, Version, Opts}).

%% =============================================================================
%% @doc Given a slot return the link (Redis instance) to the mapped
%% node.
%% @end
%% =============================================================================

-spec get_state(Name :: atom()) -> state().
get_state(Name) ->
  MonitorName = eredis_cluster:get_cluster_name(Name),
  [{cluster_state, State}] = ets:lookup(MonitorName, cluster_state),
  State.

get_state_version(State) ->
  State#state.version.

-spec get_all_pools(Name :: atom()) -> [pid()].
get_all_pools(Name) ->
  State = get_state(Name),
  SlotsMapList = tuple_to_list(State#state.slots_maps),
  [SlotsMap#slots_map.node#node.pool || SlotsMap <- SlotsMapList,
    SlotsMap#slots_map.node =/= undefined].

%% =============================================================================
%% @doc Get cluster pool by slot. Optionally, a memoized State can be provided
%% to prevent from querying ets inside loops.
%% @end
%% =============================================================================
-spec get_pool_by_slot(Name :: atom(), Slot :: integer(), State :: state()) ->
  {PoolName :: atom() | undefined, Version :: integer()}.
get_pool_by_slot(_Name, Slot, State) ->
  Index = element(Slot + 1, State#state.slots),
  Cluster = element(Index, State#state.slots_maps),
  if
    Cluster#slots_map.node =/= undefined ->
      {Cluster#slots_map.node#node.pool, State#state.version};
    true ->
      {undefined, State#state.version}
  end.

-spec get_pool_by_slot(Name :: atom(), Slot :: integer()) ->
  {PoolName :: atom() | undefined, Version :: integer()}.
get_pool_by_slot(Name, Slot) ->
  State = get_state(Name),
  get_pool_by_slot(Name, Slot, State).

maybe_reload_slots_map(Name, Opts, State) ->
  try reload_slots_map(Name, Opts, State) of
    NewState -> {ok, NewState}
  catch
    {error, cannot_connect_to_cluster} ->
      {error, State};
    _ ->
      {error, State}
  end.

-spec reload_slots_map(Name :: atom(), [proplists:property()], State :: state()) -> NewState :: state().
reload_slots_map(Name, Opts, State) ->
  [close_connection(SlotsMap)
    || SlotsMap <- tuple_to_list(State#state.slots_maps)],

  ClusterSlots = get_cluster_slots(Name, Opts, State#state.nodes),

  SlotsMaps = parse_cluster_slots(ClusterSlots),
  ConnectedSlotsMaps = connect_all_slots(Name, Opts, SlotsMaps),
  Slots = create_slots_cache(ConnectedSlotsMaps),

  NewState = State#state{
    slots = list_to_tuple(Slots),
    slots_maps = list_to_tuple(ConnectedSlotsMaps),
    version = State#state.version + 1
  },

  TableName = eredis_cluster:get_cluster_name(Name),
  true = ets:insert(TableName, [{cluster_state, NewState}]),

  NewState.

-spec get_cluster_slots(Name :: atom(), [proplists:property()], [#node{}]) -> [[bitstring() | [bitstring()]]].
get_cluster_slots(_Name, _Opts, []) ->
  throw({error, cannot_connect_to_cluster});
get_cluster_slots(Name, Opts, [Node|T]) ->
  case safe_eredis_start_link(Node#node.address, Node#node.port, Opts) of
    {ok, Connection} ->
      case eredis:q(Connection, ["CLUSTER", "SLOTS"]) of
        {ok, ClusterInfo} ->
          eredis:stop(Connection),
          ClusterInfo;
        {error, <<"ERR unknown command 'CLUSTER'">>} ->
          error_logger:info_msg("use redis pool with name ~p as standalone~n", [Name]),
          get_cluster_slots_from_single_node(Node);
        {error, <<"ERR This instance has cluster support disabled">>} ->
          error_logger:info_msg("use redis pool with name ~p as standalone~n", [Name]),
          get_cluster_slots_from_single_node(Node);
        {error, Reason} ->
          error_logger:error_msg("get cluster ~p slots info from node [~p:~p] failed. error: ~p~n",
            [Name, Node#node.address, Node#node.port, Reason]),
          eredis:stop(Connection),
          get_cluster_slots(Name, Opts, T)
      end;
    _ ->
      get_cluster_slots(Name, Opts, T)
  end.

-spec get_cluster_slots_from_single_node(#node{}) ->
  [[bitstring() | [bitstring()]]].
get_cluster_slots_from_single_node(Node) ->
  [[<<"0">>, integer_to_binary(?REDIS_CLUSTER_HASH_SLOTS - 1),
    [list_to_binary(Node#node.address), integer_to_binary(Node#node.port)]]].

-spec parse_cluster_slots([[bitstring() | [bitstring()]]]) -> [#slots_map{}].
parse_cluster_slots(ClusterInfo) ->
  parse_cluster_slots(ClusterInfo, 1, []).

parse_cluster_slots([[StartSlot, EndSlot |[[Address, Port|_]|_]]|T], Index, Acc) ->
  SlotsMap =
    #slots_map{
      index = Index,
      start_slot = binary_to_integer(StartSlot),
      end_slot = binary_to_integer(EndSlot),
      node = #node{
        address = binary_to_list(Address),
        port = binary_to_integer(Port)
      }
    },
  parse_cluster_slots(T, Index + 1, [SlotsMap | Acc]);
parse_cluster_slots([], _Index, Acc) ->
  lists:reverse(Acc).

-spec close_connection(#slots_map{}) -> ok.
close_connection(SlotsMap) ->
  Node = SlotsMap#slots_map.node,
  if
    Node =/= undefined ->
      try eredis_cluster_pool:stop(Node#node.pool) of
        _ -> ok
      catch _ ->
        ok
      end;
    true ->
      ok
  end.

-spec connect_node(Name :: atom(), #node{}, [proplists:property()]) -> #node{} | undefined.
connect_node(Name, Node, Opts) ->
  case eredis_cluster_pool:create(Name, Node#node.address, Node#node.port, Opts) of
    {ok, Pool} ->
      Node#node{pool = Pool};
    _ ->
      undefined
  end.

safe_eredis_start_link(Address, Port, Opts) ->
  process_flag(trap_exit, true),
  DataBase = proplists:get_value(db, Opts, ?DEFAULT_DATABASE),
  Password = application:get_env(password, Opts, ?DEFAULT_PASSWORD),
  Payload = eredis:start_link(Address, Port, DataBase, Password),
  process_flag(trap_exit, false),
  Payload.

-spec create_slots_cache([#slots_map{}]) -> [integer()].
create_slots_cache(SlotsMaps) ->
  SlotsCache = [[{Index, SlotsMap#slots_map.index}
    || Index <- lists:seq(SlotsMap#slots_map.start_slot, SlotsMap#slots_map.end_slot)]
    || SlotsMap <- SlotsMaps],
  SlotsCacheF = lists:flatten(SlotsCache),
  SortedSlotsCache = lists:sort(SlotsCacheF),
  [Index || {_, Index} <- SortedSlotsCache].

-spec connect_all_slots(Name :: atom(), [proplists:property()], [#slots_map{}]) -> [integer()].
connect_all_slots(Name, Opts, SlotsMapList) ->
  [SlotsMap#slots_map{node = connect_node(Name, SlotsMap#slots_map.node, Opts)}
    || SlotsMap <- SlotsMapList].

-spec connect_(Name :: atom(), [{Address :: string(), Port :: integer()}], [proplists:property()]) -> state().
connect_(_Name, [], _Opts) ->
  #state{};
connect_(Name, Nodes, Opts) ->
  State = #state{
    slots = undefined,
    slots_maps = {},
    nodes = [#node{address = A, port = P} || {A, P} <- Nodes],
    version = 0
  },

  reload_slots_map(Name, Opts, State).

%% gen_server.
init([]) ->
  #state{};
init([Name, Opts]) ->
  TableName = eredis_cluster:get_cluster_name(Name),
  ets:new(TableName, [protected, set, named_table, {read_concurrency, true}]),
  InitNodes = proplists:get_value(nodes, Opts, []),
  {ok, connect_(Name, InitNodes, Opts)}.

handle_call({reload_slots_map, Name, Version, Opts}, _From, #state{version = Version} = State) ->
  {Reply, NewState} = maybe_reload_slots_map(Name, Opts, State),
  {reply, Reply, NewState};
handle_call({reload_slots_map, _Name, _Version, _Opts}, _From, State) ->
  {reply, ok, State};
handle_call({connect, Name, Nodes, Opts}, _From, _State) ->
  {reply, ok, connect_(Name, Nodes, Opts)};
handle_call(_Request, _From, State) ->
  {reply, ignored, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
