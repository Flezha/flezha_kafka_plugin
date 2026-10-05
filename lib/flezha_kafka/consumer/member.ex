defmodule FlezhaKafka.Consumer.Member do
  @moduledoc """
  The group half of `FlezhaKafka.Consumer`: one process per binding, owning the
  fork's `KafkaEx.Consumer.ConsumerGroup` for `group_id` and `topics`, a broker
  client of its own for the lag, the lag timer and the health facts.

  The group runs one `FlezhaKafka.Consumer.Partition` per assigned partition,
  which tells this process the outcome of every batch. A group that exits (a
  failed join, a partition consumer that failed three times in five seconds) is
  logged and started again after `lag_interval_ms`. Every `lag_interval_ms` a
  monitored child reads the group's assignment and, per assigned partition, the
  latest and the committed offset, so the probe is answered while a read waits
  on the broker; a read settles only when all of it was answered.

  Log lines, telemetry and verdicts name the group, a topic, a partition, a
  count or a reason, never a record or a credential.
  """

  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Process.Watch
  alias FlezhaKafka.Connection
  alias FlezhaKafka.Consumer.Partition
  alias KafkaEx.API
  alias KafkaEx.Consumer.ConsumerGroup

  use GenServer
  use Flezha.Process.Timer, keys: [:join, :lag]

  require Logger

  @provider :kafka_consumer

  # The group's timings. A member that misses heartbeats for the session is
  # dropped; a rebalance waits for every member to rejoin within its own
  # timeout, which must cover a heartbeat and a partition consumer's 2.5 s
  # `shutdown`. The broker holds a join for at most that timeout, and the fork's
  # client bounds every request by the `:kafka_ex` `sync_timeout` (5 s in the
  # package's config), so the rebalance timeout stays below it.
  @heartbeat_ms 1_000
  @session_ms 10_000
  @rebalance_ms 4_000

  # Offset fetch and commit v1 keep offsets in Kafka rather than ZooKeeper;
  # fetch v4 is the first to carry record headers.
  @api_versions %{fetch: 4, offset_fetch: 1, offset_commit: 1}

  @doc "Started by the extension table at activation, from the declared spec."
  @spec start_link(map()) :: GenServer.on_start()
  def start_link(args) when is_map(args),
    do: GenServer.start_link(__MODULE__, args, name: Map.fetch!(args, :name))

  @doc """
  The member's start arguments, from the plugin's prepared options. The broker
  credential reaches it only inside the `connect` closure, resolved at each
  group start.
  """
  @spec args(map()) :: map()
  def args(opts) do
    opts
    |> Map.take([
      :plugin_id,
      :group_id,
      :topics,
      :overwatcher_pid,
      :durable_input,
      :entry_channel,
      :start_at,
      :max_bytes,
      :max_wait_ms,
      :commit_interval_ms,
      :lag_interval_ms
    ])
    |> Map.put(:connect, fn -> Connection.client_opts(opts) end)
  end

  @doc """
  The facts the provider's `health/1` and `inspect_state/1` read: the refusal
  run and its last error, the time of the last settled lag read, the
  assignment and the lag it saw. `:timeout` when the member does not answer.
  """
  @spec probe(pid()) :: {:ok, map()} | :timeout
  def probe(pid), do: ProviderBehaviour.ask(pid, :health_probe)

  @impl true
  def init(args) do
    # A stop leaves the group, which only a trapped shutdown reaches.
    Process.flag(:trap_exit, true)

    state =
      args
      |> Map.drop([:name])
      |> Map.merge(%{
        group: nil,
        client: nil,
        watch: Watch.watch(Watch.new(), args.overwatcher_pid, :host),
        read: nil,
        assignment: nil,
        lag: %{},
        consecutive_failures: 0,
        last_error: nil,
        last_settled_at: now(),
        timers: %{}
      })

    # Scheduled, never run here: activation is synchronous in the host.
    {:ok, state |> arm_timer(:join, 0, :join) |> arm_lag()}
  end

  @impl true
  def handle_info(:join, state), do: {:noreply, state |> timer_fired(:join) |> join()}

  def handle_info(:lag, state),
    do: {:noreply, state |> timer_fired(:lag) |> start_read() |> arm_lag()}

  def handle_info({:lag_read, pid, result}, %{read: {pid, ref}} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, settle(%{state | read: nil}, result)}
  end

  def handle_info({:DOWN, ref, :process, _child, reason}, %{read: {_pid, ref}} = state),
    do: {:noreply, settle(%{state | read: nil}, {:error, {:exit, reason}})}

  # The relay this member feeds is gone.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Watch.down(state.watch, ref) do
      {[:host], _watch} -> {:stop, :normal, state}
      _stale -> {:noreply, state}
    end
  end

  def handle_info({:EXIT, pid, reason}, %{group: group, client: client} = state)
      when pid in [group, client] and is_pid(pid),
      do:
        {:noreply,
         rejoin_later(state, if(pid == group, do: "exited", else: "lost its lag client"), reason)}

  def handle_info(:batch_taken, state),
    do: {:noreply, %{state | consecutive_failures: 0, last_error: nil}}

  def handle_info({:batch_refused, error}, state),
    do:
      {:noreply,
       %{state | consecutive_failures: state.consecutive_failures + 1, last_error: error}}

  def handle_info({:health_probe, from, ref}, state) do
    send(
      from,
      {ref,
       Map.take(state, [:consecutive_failures, :last_error, :last_settled_at, :assignment, :lag])}
    )

    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if state.read, do: Process.exit(elem(state.read, 0), :kill)
    stop_group(state)
    :ok
  end

  # ---------- the group ----------

  defp join(%{group: nil} = state) do
    conn = state.connect.()

    case KafkaEx.Client.start_link(conn ++ [consumer_group: state.group_id], :no_name) do
      {:ok, client} -> start_group(%{state | client: client}, conn)
      {:error, reason} -> rejoin_later(state, "could not start", reason)
    end
  end

  defp join(state), do: state

  # A group that does not start leaves the client to `rejoin_later/3` to stop.
  defp start_group(state, conn) do
    case ConsumerGroup.start_link(
           Partition,
           state.group_id,
           state.topics,
           group_opts(state, conn)
         ) do
      {:ok, group} -> %{state | group: group}
      {:error, reason} -> rejoin_later(state, "could not start", reason)
    end
  end

  defp group_opts(state, conn) do
    conn ++
      [
        api_versions: @api_versions,
        auto_offset_reset: if(state.start_at == "beginning", do: :earliest, else: :latest),
        heartbeat_interval: @heartbeat_ms,
        session_timeout: @session_ms,
        rebalance_timeout: @rebalance_ms,
        fetch_options: [max_bytes: state.max_bytes, max_wait_time: state.max_wait_ms]
      ] ++ Partition.consumer_opts(state)
  end

  defp rejoin_later(state, what, reason) do
    Logger.warning(
      "[FlezhaKafka.Consumer] group=#{state.group_id} #{what} " <>
        "reason=#{inspect(reason)}; joining again in #{state.lag_interval_ms} ms"
    )

    emit(:failed, %{count: 1}, Map.put(labels(state), :reason, inspect(reason)))

    stop_group(state)

    %{state | group: nil, client: nil, assignment: nil}
    |> arm_timer(:join, state.lag_interval_ms, :join)
  end

  # Not waited for: the host stops this member inside its own call, and the
  # group's supervisor gives each partition consumer its `shutdown` on its own.
  defp stop_group(state) do
    for pid <- [state.group, state.client], is_pid(pid) do
      Process.unlink(pid)
      Process.exit(pid, :shutdown)
    end

    :ok
  end

  # ---------- the lag ----------

  defp arm_lag(state), do: arm_timer(state, :lag, state.lag_interval_ms, :lag)

  defp start_read(%{read: nil, group: group, client: client} = state) when is_pid(group) do
    member = self()
    facts = Map.take(state, [:group_id, :start_at, :lag_interval_ms])

    {pid, ref} =
      spawn_monitor(fn -> send(member, {:lag_read, self(), read_lag(group, client, facts)}) end)

    %{state | read: {pid, ref}}
  end

  defp start_read(state), do: state

  # A manager answers nothing while it joins, and nil before its first sync. A
  # broker that is away answers an error, or nothing at all.
  defp read_lag(group, client, facts) do
    case ConsumerGroup.assignments(group, facts.lag_interval_ms) do
      assignment when is_list(assignment) ->
        {:ok, Enum.sort(assignment), lag(client, assignment, facts)}

      nil ->
        {:error, :not_joined}
    end
  rescue
    error -> {:error, error}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  defp lag(client, assignment, facts) do
    for {topic, partitions} <- Enum.group_by(assignment, &elem(&1, 0), &elem(&1, 1)),
        {partition, committed} <- committed(client, facts.group_id, topic, partitions),
        into: %{} do
      {:ok, latest} = API.latest_offset(client, topic, partition)

      {{topic, partition},
       latest - position(client, {topic, partition}, committed, latest, facts)}
    end
  end

  defp committed(client, group_id, topic, partitions) do
    request = Enum.map(partitions, &%{partition_num: &1})

    {:ok, [%{partition_offsets: offsets}]} =
      API.fetch_committed_offset(client, group_id, topic, request, api_version: 1)

    Enum.map(offsets, &{&1.partition, &1.offset})
  end

  # A group with no committed offset reads from where `start_at` places it.
  defp position(_client, _partition, committed, _latest, _facts) when committed >= 0,
    do: committed

  defp position(client, {topic, partition}, _none, _latest, %{start_at: "beginning"}) do
    {:ok, earliest} = API.earliest_offset(client, topic, partition)
    earliest
  end

  defp position(_client, _partition, _none, latest, _end), do: latest

  defp settle(state, {:ok, assignment, lag}) do
    if assignment != state.assignment do
      emit(
        :rebalanced,
        %{count: length(assignment)},
        Map.put(labels(state), :partitions, Enum.map_join(assignment, ",", &partition_name/1))
      )
    end

    for {{topic, partition}, value} <- lag do
      emit(:lag, %{lag: value}, Map.merge(labels(state), %{topic: topic, partition: partition}))
    end

    %{state | assignment: assignment, lag: lag, last_settled_at: now()}
  end

  defp settle(state, {:error, _reason}), do: state

  @doc "A partition as the text a label or `flz relay info` shows: `topic/partition`."
  @spec partition_name({binary(), non_neg_integer()}) :: binary()
  def partition_name({topic, partition}), do: "#{topic}/#{partition}"

  defp labels(state), do: %{plugin_id: state.plugin_id, group_id: state.group_id}

  defp emit(verb, measurements, metadata),
    do: ProviderBehaviour.emit(@provider, verb, measurements, metadata)

  defp now(), do: :erlang.monotonic_time(:millisecond)
end
