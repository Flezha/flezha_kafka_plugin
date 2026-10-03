defmodule FlezhaKafka.ProducerWorker do
  @moduledoc """
  One GenServer per `(namespace, plugin_id, topic)`: every operator bound to the
  plugin casts its records here, and the worker chooses each record's partition
  as it takes the record off its mailbox. Per partition it keeps a lane
  (`FlezhaKafka.ProducerWorker.Lanes`) with a bounded outbound queue, linger
  batching, a window of in-flight Produce requests, retries of transient broker
  errors and an idempotent producer session (`FlezhaKafka.ProducerWorker.Window`).

  Issues real Produce/InitProducerId/CreateTopics requests through a
  `KafkaEx.Client` (Kayrock-based) shared worker pid acquired from
  `FlezhaKafka.WorkerRegistry` (`FlezhaKafka.ProducerWorker.Client`).

  Up to `:max_in_flight` Produce requests are outstanding per partition at a
  time: the worker sends them itself through the client's asynchronous API and
  reads the outcomes off its own mailbox. Metadata refreshes and topic creation
  are synchronous client calls and still run in short-lived spawned processes
  so the mailbox stays drainable.
  """

  use GenServer

  require Logger

  alias Flezha.Process.Backlog
  alias Flezha.Process.Cspec
  alias Flezha.SDK.SafeCall
  alias FlezhaKafka.ProducerWorker.Client
  alias FlezhaKafka.ProducerWorker.Lanes
  alias FlezhaKafka.ProducerWorker.Wire
  alias FlezhaKafka.ProducerWorker.Window
  alias KafkaEx.Producer.Partitioner

  # How long a caller waits for the worker's counters.
  @call_ms 5_000

  @doc """
  Started by the extension table at activation, from the provider's declared
  worker spec. Every tunable it reads is a declared option already converted and
  defaulted; what it reads with a fallback are the injected seams below, which
  are functions a test replaces and never settings.
  """
  @spec start_link(map()) :: GenServer.on_start()
  def start_link(opts) when is_map(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Map.fetch!(opts, :name))

  def child_spec(opts) do
    Cspec.build(
      {__MODULE__, opts.plugin_id, opts.topic},
      {__MODULE__, :start_link, [opts]},
      :worker_transient
    )
  end

  @doc """
  Synchronous enqueue intended for tests, ad-hoc tooling, and explicit-flush
  call sites. Blocks the caller until the message is acknowledged, dropped,
  or the producer's internal `:delivery_timeout_ms` is exhausted. The
  caller's GenServer.call timeout SHOULD exceed `:delivery_timeout_ms` by a
  small buffer; pass `:timeout` explicitly to override.

  Chain plugins MUST use `cast_enqueue/3` instead — `enqueue/4` blocks the
  caller for the duration of one broker round-trip and will saturate the
  host process's mailbox under sustained input above the broker drain rate.
  """
  @spec enqueue(GenServer.server(), binary(), map(), keyword()) ::
          :ok | {:error, term()}
  def enqueue(server, msg, metadata, call_opts \\ []) do
    SafeCall.bounded(server, {:enqueue, msg, metadata}, Keyword.get(call_opts, :timeout, 35_000),
      on_error: %{
        timeout: {:error, {:delivery_failed, :enqueue_timeout}},
        noproc: {:error, {:delivery_failed, :no_producer_worker}}
      }
    )
  end

  @doc """
  Asynchronous, non-blocking enqueue used by the chain dispatch path.
  Returns `:ok` immediately after handing the message off to the worker's
  mailbox. Admission against the bounded queue happens inside the worker;
  overflow is resolved per the configured `:queue_overflow` strategy. Under
  `:drop_nothing` a cast-admitted record waits for room instead of being
  dropped — the caller is never blocked, the record simply is not admitted yet,
  which is what back-pressures the outbox flush that produced it.

  Permanent delivery failures of cast-admitted records surface through
  `[:flezha, :plugin, :kafka, :failed]` telemetry tagged with
  `caller: :async`, and — for a delivery-guaranteed record — through the
  outbox park path; there is no caller to reply to.
  """
  @spec cast_enqueue(GenServer.server(), binary(), map()) :: :ok
  def cast_enqueue(server, msg, metadata) do
    GenServer.cast(server, {:enqueue_async, msg, metadata})
  end

  @doc """
  The worker's counters, summed over its partitions, and per partition under
  `partitions`; the empty map when the worker does not answer.
  """
  @spec stats(GenServer.server()) :: map()
  def stats(server) do
    SafeCall.bounded(server, :stats, @call_ms, on_error: %{})
  end

  @doc """
  Worker-reuse policy for `ProducerSup.ensure_worker/1`: alive AND not
  permanently parked (`last_error` is `{:swap_exhausted, _}` when the
  in-process broker-swap recovery is exhausted and the queue cannot
  drain). A wedged stats read errs on the side of reuse — only a
  definitively parked/dead worker is reaped.
  """
  @spec reusable?(pid()) :: boolean()
  def reusable?(pid) do
    Process.alive?(pid) and not parked?(pid)
  end

  defp parked?(pid) do
    stats = SafeCall.bounded(pid, :stats, 1_000, on_error: nil, log: :debug)
    match?(%{last_error: {:swap_exhausted, _}}, stats)
  end

  @doc """
  Force a fresh broker connection now, bypassing the stall counter — an
  on-demand recovery for a producer wedged against a healthy-but-unreachable
  broker, without waiting for the next failed batch. (The automatic path is
  `maybe_refresh_broker/2` on batch failure.) No-op for workers that own no
  shared registry seat (test-injected `:worker_pid`).
  """
  @spec refresh_broker(GenServer.server()) :: :ok
  def refresh_broker(server), do: GenServer.cast(server, :refresh_broker)

  @spec stop(GenServer.server()) :: :ok
  def stop(server), do: GenServer.stop(server)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    {:ok, initial_state(opts), {:continue, :connect}}
  end

  # Every tunable is read here and nowhere else, so the defaults are one table.
  # The client is acquired once `init/1` has returned: the registry it comes from
  # is started under the supervisor that is starting this worker.
  defp initial_state(opts) do
    unless opts[:worker_pid] || opts[:connection_identity],
      do:
        raise(ArgumentError, "ProducerWorker requires either :worker_pid or :connection_identity")

    state =
      %{
        worker_pid: opts[:worker_pid],
        registry_ref: nil,
        registry_owned?: is_nil(opts[:worker_pid])
      }
      |> Map.merge(configured(opts))
      |> Map.merge(fresh_slots())

    Map.merge(
      state,
      Lanes.new(state.fixed_partition, state.idempotent, state.max_in_flight, state.queue_max)
    )
  end

  # The declared tunables, taken as given, plus the injected seams. A tunable
  # has exactly one default and it is its row in `FlezhaKafka.Producer`;
  # a seam is a function a test replaces, never a setting, and its fallback is
  # the production implementation.
  @tunables [
    :connection_identity,
    :topic,
    :plugin_id,
    :partitioner,
    :compression,
    :linger_ms,
    :max_batch_bytes,
    :queue_max,
    :queue_overflow,
    :queue_block_timeout_ms,
    :retries,
    :retry_backoff_ms,
    :retry_max_backoff_ms,
    :delivery_timeout_ms,
    :dispatch_timeout_ms,
    :required_acks,
    :request_timeout_ms,
    :idempotent,
    :max_in_flight,
    :auto_create_topic,
    :auto_create_partitions,
    :auto_create_replication_factor,
    :topic_configs,
    :metadata_refresh_timeout_ms
  ]

  defp configured(opts) do
    opts
    |> Map.take(@tunables)
    |> Map.merge(%{
      fixed_partition: opts[:partition],
      configured_idempotent: opts.idempotent,
      configured_max_in_flight: opts.max_in_flight,
      static_headers: Wire.static_headers(opts[:headers] || %{}),
      send_fn: seam(opts, :send_fn, &Wire.default_send/3),
      init_producer_id_fn: seam(opts, :init_producer_id_fn, &Wire.default_init_producer_id/2),
      create_topic_fn: seam(opts, :create_topic_fn, &Wire.default_create_topic/4),
      metadata_refresh_fn: seam(opts, :metadata_refresh_fn, &Wire.default_metadata_refresh/2),
      partition_count_fn: seam(opts, :partition_count_fn, &Wire.default_partition_count/3),
      now_fn: seam(opts, :now_fn, fn -> System.monotonic_time(:millisecond) end),
      wall_now_fn: seam(opts, :wall_now_fn, fn -> System.system_time(:millisecond) end),
      stall_threshold: seam(opts, :stall_threshold, Client.default_stall_threshold())
    })
  end

  # The one read a seam takes. It is not a setting: a plugin cannot write one,
  # and the fallback is what production runs.
  defp seam(opts, key, default), do: Map.get(opts, key) || default

  # What a fresh worker starts with. None of it is configurable.
  defp fresh_slots do
    %{
      lanes: %{},
      # The topic's partition count and the partition unkeyed records go to,
      # once known; a fixed `partition` needs neither.
      partitions: nil,
      sticky: nil,
      # The broker client's own trouble (a death, a swap that failed or gave
      # up, a partition count it cannot read), apart from any partition's.
      client_error: nil,
      count_failures: 0,
      dropped_total: 0,
      last_force_replace_at: nil,
      swap_started_at: nil,
      swap_attempt: 0,
      pending_swap_telemetry: nil
    }
  end

  @impl true
  def handle_continue(:connect, %{worker_pid: nil} = state), do: Client.connect(state)

  def handle_continue(:connect, state),
    do: {:noreply, state, {:continue, :maybe_init_producer_id}}

  def handle_continue(:maybe_init_producer_id, state) do
    state = state |> read_partitions(1) |> Lanes.each(&start_session/1)
    {:noreply, Client.emit_pending_swap_telemetry(state)}
  end

  # A lane's idempotent producer session, then its first batch. A session that
  # failed to open is tried again at the lane's next start, after a swap. The
  # call is synchronous and bounded by `metadata_refresh_timeout_ms`: a lane
  # starts on its partition's first record, so the worker waits at most that
  # long once per new partition and once per lane after a swap.
  defp start_session(%{partition: nil} = state), do: state
  defp start_session(%{configured_idempotent: false} = state), do: Window.maybe_kick_batch(state)

  defp start_session(state) do
    case state.init_producer_id_fn.(state.worker_pid, state.metadata_refresh_timeout_ms) do
      {:ok, %{producer_id: pid, producer_epoch: epoch}} ->
        Window.maybe_kick_batch(%{
          state
          | producer_id: pid,
            producer_epoch: epoch,
            idempotent: true,
            max_in_flight: state.configured_max_in_flight
        })

      {:error, reason} ->
        Logger.warning(
          "#{Window.tag(state)} " <>
            "InitProducerId failed: #{inspect(reason)} — running in non-idempotent mode"
        )

        # Without a producer session nothing keeps a retry inside a window in
        # order, so the fallback narrows it to one as well.
        Window.maybe_kick_batch(%{
          state
          | idempotent: false,
            max_in_flight: 1,
            last_error: reason
        })
    end
  end

  @impl true
  def handle_call({:enqueue, msg, metadata}, from, state) do
    record = Wire.build_record(msg, metadata, from, state.wall_now_fn.())
    {:noreply, admit(state, record, from)}
  end

  def handle_call(:stats, _from, state) do
    totals = Lanes.totals(state)
    worst = Lanes.worst(state)

    partitions =
      for {p, lane} <- Lanes.all(state),
          p != nil,
          into: %{},
          do: {p, %{queue_depth: lane.queue.frames, in_flight: Lanes.window_records(lane)}}

    {:reply,
     %{
       queue_depth: totals.queue_depth,
       in_flight: totals.in_flight,
       dropped_total: state.dropped_total,
       last_error: state.client_error || worst.last_error,
       consecutive_failures: max(worst.consecutive_failures, state.count_failures),
       partitions: partitions
     }, state}
  end

  @impl true
  def handle_cast(:refresh_broker, %{registry_ref: nil} = state), do: {:noreply, state}

  def handle_cast(:refresh_broker, state) do
    Logger.info(
      "#{Window.tag(state)} " <>
        "convergence requested broker refresh; forcing fresh connection"
    )

    {:noreply, Client.force_replaced(state)}
  end

  def handle_cast({:enqueue_async, msg, metadata}, state) do
    record = Wire.build_record(msg, metadata, :async, state.wall_now_fn.())
    {:noreply, admit(state, record, :async)}
  end

  @impl true
  def handle_info({:flush_batch, partition}, state) do
    state = hold(state, partition)
    {:noreply, Window.maybe_send_batch(%{state | linger_timer: nil})}
  end

  # A response for a dispatch the window does not own (a resend already went
  # out, or the batch was rewound) is ignored — the same rule as the fresh
  # handle per dispatch it rests on.
  def handle_info({:kafka_ex_response, ref, result}, state) do
    case Lanes.find(state, fn lane -> Enum.any?(lane.in_flight, &(&1.ref == ref)) end) do
      {:ok, partition} ->
        state = hold(state, partition)
        {:noreply, Window.with_dispatch(state, ref, &Window.handle_batch_result(&1, &2, result))}

      :error ->
        {:noreply, state}
    end
  end

  # The send neither returned nor crashed in time (e.g. the shared client is
  # wedged on a dead socket after a broker restart). Treat the stranded dispatch
  # as a retriable failure so the batch advances toward fail_batch / broker
  # refresh instead of stalling the worker forever.
  def handle_info({:dispatch_timeout, partition, ref}, state) do
    outcome = &Window.handle_batch_result(&1, &2, {:error, :dispatch_timeout})
    {:noreply, state |> hold(partition) |> Window.with_dispatch(ref, outcome)}
  end

  def handle_info({:retry_window, partition, batch_id}, state),
    do: {:noreply, state |> hold(partition) |> Window.retry_window(batch_id)}

  # A topic with fewer partitions than the lane's, found by the creation or by a
  # metadata refresh (a broker may create the topic itself, with its own count,
  # when a metadata read names it): what the lane holds goes where the topic's
  # count sends it, instead of retrying a partition that does not exist until
  # its budget runs out.
  def handle_info({:topic_create_result, partition, batch_id, result, count}, state) do
    state = learn_partitions(state, count)

    if partition_exists?(state, partition) do
      outcome = &Window.handle_topic_create_result(&1, &2, result)
      {:noreply, state |> hold(partition) |> Window.with_batch(batch_id, outcome)}
    else
      {:noreply, reroute(state, partition)}
    end
  end

  def handle_info({:metadata_refresh_done, partition, batch_id, latency_ms, error, count}, state) do
    state = state |> learn_partitions(count) |> hold(partition)

    Window.emit_metadata_refresh(
      latency_ms,
      Map.merge(Window.partition_meta(state), %{trigger: :error_recovery, error: error})
    )

    if partition_exists?(state, partition),
      do: {:noreply, Window.with_batch(state, batch_id, &Window.resume_owed/2)},
      else: {:noreply, reroute(state, partition)}
  end

  def handle_info({:refresh_partitions, attempt}, state),
    do: {:noreply, read_partitions(state, attempt)}

  def handle_info({:kafka_worker_dead, ref, reason}, %{registry_ref: ref} = state),
    do: Client.on_worker_dead(state, reason)

  def handle_info({:kafka_worker_dead, _ref, _reason}, state) do
    Logger.debug(
      "#{Window.tag(state)} " <>
        "stale {:kafka_worker_dead, ...} for old registry_ref; ignoring"
    )

    {:noreply, state}
  end

  def handle_info({:retry_swap, ref, attempt}, %{registry_ref: ref} = state),
    do: Client.retry_swap(state, attempt)

  def handle_info({:retry_swap, _ref, _attempt}, state), do: {:noreply, state}

  def handle_info({:EXIT, _pid, _reason}, state) do
    Logger.debug(
      "#{Window.tag(state)} " <>
        "received unhandled :EXIT; swap is driven by {:kafka_worker_dead, ...} — ignoring"
    )

    {:noreply, state}
  end

  def handle_info({:block_timeout, partition, from}, state),
    do: {:noreply, state |> hold(partition) |> Window.block_timeout(from)}

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    {:noreply, state}
  end

  # ------------------------------------------------------------------
  # Partitions: which lane a record joins, and the count that decides it.
  # ------------------------------------------------------------------

  # The worker with the record's lane held, the lane started when it is new.
  defp routed(state, record), do: hold(state, partition_for(state, record))

  defp partition_for(%{fixed_partition: p}, _record) when is_integer(p), do: p
  defp partition_for(%{partitions: nil}, _record), do: nil

  defp partition_for(%{partitioner: :murmur2} = state, %{key: key}) when is_binary(key),
    do: Partitioner.Default.assign_partition(state.topic, key, nil, state.partitions)

  # An unkeyed record with a durable identity hashes that identity, so its
  # redrive lands where it did; only a record with neither takes the sticky
  # partition. Unkeyed records keep no order between them.
  defp partition_for(state, %{dedup_key: dedup_key}) when is_binary(dedup_key),
    do: Partitioner.Default.assign_partition(state.topic, dedup_key, nil, state.partitions)

  defp partition_for(state, _record), do: state.sticky

  defp partition_exists?(%{partitions: count}, partition) when is_integer(count),
    do: partition < count

  defp partition_exists?(_state, _partition), do: true

  defp hold(state, partition) do
    case Lanes.hold(state, partition) do
      {:held, state} -> state
      {:created, state} -> start_session(state)
    end
  end

  # The topic's partition count, read before anything is routed and after every
  # client swap. A read that fails is retried on the produce ladder while the
  # records wait in the waiting lane, and counts as a failed batch, so a broker
  # that stays unreachable shows in the plugin's health.
  defp read_partitions(%{fixed_partition: p} = state, _attempt) when is_integer(p), do: state

  defp read_partitions(state, attempt) do
    case state.partition_count_fn.(
           state.worker_pid,
           state.topic,
           state.metadata_refresh_timeout_ms
         ) do
      {:error, reason} ->
        backoff = Window.retry_backoff_ms(state, attempt)
        Process.send_after(self(), {:refresh_partitions, attempt + 1}, backoff)

        %{
          state
          | client_error: {:partitions_unknown, reason},
            count_failures: state.count_failures + 1
        }

      known ->
        learn_partitions(%{state | count_failures: 0}, known)
    end
  end

  # A topic the broker does not know yet is created with `auto_create_partitions`
  # on the first produce; one that will not be created routes everything to 0,
  # where the produce fails as it would have.
  defp learn_partitions(state, {:ok, :absent}),
    do: learn_partitions(state, {:ok, absent_count(state)})

  defp learn_partitions(state, {:ok, count}) when is_integer(count) and count > 0 do
    sticky = state.sticky || :rand.uniform(count) - 1
    state = %{state | partitions: count, sticky: rem(sticky, count)}

    state =
      if match?({:partitions_unknown, _}, state.client_error),
        do: %{state | client_error: nil},
        else: state

    reroute(state, nil)
  end

  defp learn_partitions(state, _unknown), do: state

  defp absent_count(%{auto_create_topic: true} = state), do: state.auto_create_partitions
  defp absent_count(_state), do: 1

  # Every record a lane holds, its window's first, joins the lane routing gives
  # it now, in order: the waiting lane's once the count is known, and a lane
  # whose partition the topic turned out not to have. Such a lane goes.
  defp reroute(state, partition) do
    {_, state} = Lanes.hold(state, partition)
    state = Window.cancel_timers(state)

    held =
      Enum.flat_map(state.in_flight, & &1.records) ++
        Backlog.to_list(state.queue) ++ :queue.to_list(state.blocked)

    state = %{
      state
      | queue: Backlog.clear(state.queue),
        blocked: :queue.new(),
        in_flight: []
    }

    state = Lanes.drop(state, partition)

    Enum.reduce(held, state, fn
      {from, record}, acc -> admit(acc, record, from)
      record, acc -> admit(acc, record, record.from)
    end)
  end

  # A record joins its partition's lane. A synchronous caller is answered here
  # only when its record is refused; otherwise once it is delivered or given up.
  defp admit(state, record, from) do
    case Window.admit_record(routed(state, record), record, from) do
      {:admitted, state} -> Window.maybe_kick_batch(state)
      {:replied, reply, state} -> tap(state, fn _ -> GenServer.reply(from, reply) end)
      {_blocked_or_dropped, state} -> state
    end
  end

  @impl true
  def terminate(_reason, %{registry_owned?: true, registry_ref: ref}) when not is_nil(ref) do
    _ = FlezhaKafka.WorkerRegistry.release(ref)
    :ok
  end

  def terminate(_reason, _state), do: :ok
end
