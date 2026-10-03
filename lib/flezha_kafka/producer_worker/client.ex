defmodule FlezhaKafka.ProducerWorker.Client do
  @moduledoc """
  The producer worker's broker client, one `KafkaEx.Client` shared through
  `FlezhaKafka.WorkerRegistry`: acquiring it when the worker starts, swapping it
  for a fresh one when it dies, and forcing a fresh one when it is alive but
  wedged. Every lane's work is rewound or restarted here, through
  `FlezhaKafka.ProducerWorker.Lanes`, because a client serves all of them.
  """

  require Logger

  alias Flezha.Process.Backlog
  alias FlezhaKafka.ProducerWorker.Lanes
  alias FlezhaKafka.ProducerWorker.Window
  alias FlezhaKafka.WorkerRegistry

  import Window,
    only: [
      tag: 1,
      partition_meta: 1,
      reply_to_record: 2,
      caller_tag: 1,
      retry_backoff_ms: 2,
      emit_worker_swapped: 2,
      emit_produce_failed: 3
    ]

  # Consecutive `fail_batch/3` occurrences (a batch that exhausted its
  # retries / delivery timeout) before we force a fresh broker connection.
  # A clean broker restart leaves the shared client alive but produce stalls
  # forever; refreshing the client is the only path back. Reset to 0 on any
  # successful batch.
  @default_stall_threshold 3

  # Error reasons that mean the shared client has NO working broker connection
  # (the classic clean-broker-restart wedge). A single batch failing this way
  # is authoritative that the client must be recreated — waiting for
  # `@default_stall_threshold` batches strands a low-traffic producer that may
  # emit only one batch per input burst. Recreate is throttled by the
  # `@force_replace_ms` so a genuinely-down broker is not
  # recreated on every batch.
  @client_wedged [
    :no_broker,
    :broker_not_available,
    :network_exception,
    :leader_not_available,
    :closed,
    :correlation_mismatch
  ]

  # The least time between two forced fresh broker connections, so a broker that
  # is genuinely down is not reconnected to on every failed batch.
  @force_replace_ms 2_000

  # On a sustained stall (N consecutive give-ups) force a fresh broker
  # client. A clean broker restart never kills the shared client, so the
  # worker-DOWN swap path never fires and produce stalls forever. Refresh
  # trips the registry's worker monitor, driving the standard
  # `{:kafka_worker_dead, ...}` swap that reconnects on a new client.
  # Reset the counter so we don't hammer refresh every subsequent batch.
  def maybe_refresh_broker(%{registry_ref: nil} = state, _reason), do: state

  def maybe_refresh_broker(state, reason) do
    cond do
      # A no-broker-connection error means the shared client is wedged on a
      # dead/stale socket (the clean-broker-restart case). One such batch is
      # authoritative — recreate now, throttled, instead of waiting for the
      # stall counter a low-traffic producer may never reach.
      reason in @client_wedged and force_replace_due?(state) ->
        Logger.warning(
          "#{tag(state)} " <>
            "no broker connection (reason=#{inspect(reason)}); forcing fresh broker connection"
        )

        force_replaced(state)

      state.consecutive_failures >= state.stall_threshold ->
        Logger.warning(
          "#{tag(state)} " <>
            "#{state.consecutive_failures} consecutive failed batches " <>
            "(last reason=#{inspect(reason)}); forcing fresh broker connection"
        )

        force_replaced(state)

      true ->
        state
    end
  end

  def force_replaced(state) do
    %{
      force_replace_broker(state)
      | consecutive_failures: 0,
        last_force_replace_at: state.now_fn.()
    }
  end

  defp force_replace_due?(%{last_force_replace_at: nil}), do: true

  defp force_replace_due?(state) do
    state.now_fn.() - state.last_force_replace_at >= @force_replace_ms
  end

  # Reliable recovery: re-acquire a freshly started shared client by identity
  # and rebind worker_pid directly. Avoids the monitor-DOWN swap fan-out, which
  # never reaches a stalled caller once the entry has been torn down.
  defp force_replace_broker(%{connection_identity: nil} = state), do: state

  defp force_replace_broker(state) do
    case WorkerRegistry.force_replace(state.connection_identity) do
      {:ok, new_pid} when is_pid(new_pid) ->
        # Every lane's window is renumbered, so every lane is owed its resend.
        Lanes.each(
          %{state | worker_pid: new_pid, last_error: nil},
          &(&1 |> Window.restart_producer_session() |> Window.maybe_resend())
        )

      _ ->
        state
    end
  end

  @doc "The consecutive failed batches that force a fresh client unless a test sets its own."
  def default_stall_threshold, do: @default_stall_threshold

  @doc """
  Acquire the worker's client, through the path a swap takes: a broker that
  cannot be reached yet is retried on the produce ladder, and the records wait.
  """
  def connect(state), do: attempt_swap(%{state | swap_started_at: state.now_fn.()}, :connect)

  @doc """
  The client died: every lane's window goes back to the head of its queue, oldest
  first, and the worker swaps to a fresh client for the same identity.
  """
  def on_worker_dead(state, reason) do
    Logger.warning(
      "#{tag(state)} " <>
        "worker pid #{inspect(state.worker_pid)} died (reason=#{inspect(reason)}); initiating swap"
    )

    in_flight_count = Lanes.totals(state).in_flight
    state = Lanes.each(state, &(&1 |> Window.cancel_timers() |> Window.rewind_in_flight(reason)))
    totals = Lanes.totals(state)

    state = %{
      state
      | swap_started_at: state.swap_started_at || state.now_fn.(),
        swap_attempt: 0,
        client_error: {:worker_died, reason},
        pending_swap_telemetry:
          state.pending_swap_telemetry ||
            %{
              measurements: %{
                queue_depth: totals.queue_depth,
                in_flight_records: in_flight_count,
                queue_bytes: totals.queue_bytes
              },
              metadata:
                Map.merge(worker_meta(state), %{
                  reason: reason,
                  producer_id_before: fixed_producer_id(state),
                  old_worker_pid: state.worker_pid
                })
            }
    }

    attempt_swap(state, reason)
  end

  @doc "A scheduled retry of a swap that failed."
  def retry_swap(state, attempt),
    do: attempt_swap(%{state | swap_attempt: attempt}, swap_last_reason(state))

  defp attempt_swap(state, _reason) do
    case do_swap_or_acquire(state) do
      {:ok, new_pid, new_ref} ->
        pending =
          case state.pending_swap_telemetry do
            nil -> nil
            payload -> %{payload | metadata: Map.put(payload.metadata, :new_worker_pid, new_pid)}
          end

        state =
          Lanes.each(
            %{
              state
              | worker_pid: new_pid,
                registry_ref: new_ref,
                swap_started_at: nil,
                swap_attempt: 0,
                client_error: nil,
                pending_swap_telemetry: pending
            },
            &%{&1 | producer_id: nil, producer_epoch: nil, sequence: 0, in_flight: []}
          )

        {:noreply, state, {:continue, :maybe_init_producer_id}}

      {:error, swap_reason} ->
        now = state.now_fn.()
        elapsed = now - (state.swap_started_at || now)

        if elapsed >= state.delivery_timeout_ms do
          fail_all_during_swap(state, swap_reason)
        else
          attempt = (state.swap_attempt || 0) + 1
          backoff = retry_backoff_ms(state, attempt)
          Process.send_after(self(), {:retry_swap, state.registry_ref, attempt}, backoff)
          {:noreply, %{state | swap_attempt: attempt, client_error: {:swap_failed, swap_reason}}}
        end
    end
  end

  # The first acquire has no seat to swap.
  defp do_swap_or_acquire(%{registry_ref: nil} = state),
    do: WorkerRegistry.acquire(state.connection_identity)

  defp do_swap_or_acquire(state) do
    case WorkerRegistry.swap(state.registry_ref) do
      {:ok, _pid, _ref} = ok ->
        ok

      {:error, :no_pending_swap} when not is_nil(state.connection_identity) ->
        WorkerRegistry.acquire(state.connection_identity)

      {:error, _reason} = err ->
        err
    end
  end

  defp swap_last_reason(%{client_error: {:worker_died, reason}}), do: reason
  defp swap_last_reason(%{client_error: {:swap_failed, reason}}), do: reason
  defp swap_last_reason(_), do: :unknown

  # On swap exhaustion we stay alive: every queued record is replied to
  # with `{:error, {:delivery_failed, :worker_swap_exhausted}}` and a
  # `[:produce, :failed]` event fires per record (per task 2.4). The
  # producer's `stats/1` `last_error` carries the terminal reason so callers can
  # surface it. Subsequent enqueues fail through the standard send-path
  # `delivery_timeout_ms` retry against the dead `worker_pid`, so no
  # tight restart loop and no `:swap_exhausted` crash report.
  defp fail_all_during_swap(state, swap_reason) do
    state =
      Lanes.each(state, &fail_queued(&1, swap_reason))

    {:noreply,
     %{
       state
       | swap_started_at: nil,
         swap_attempt: 0,
         client_error: {:swap_exhausted, swap_reason},
         pending_swap_telemetry: nil
     }}
  end

  defp fail_queued(state, swap_reason) do
    queued = Backlog.to_list(state.queue)

    Enum.each(queued, fn record ->
      reply_to_record(record, {:error, {:delivery_failed, :worker_swap_exhausted}})

      emit_produce_failed(
        state.swap_attempt + 1,
        state.delivery_timeout_ms,
        Map.merge(partition_meta(state), %{
          last_reason: {:worker_swap_exhausted, swap_reason},
          caller: caller_tag(record)
        })
      )
    end)

    %{state | queue: Backlog.clear(state.queue), in_flight: []}
  end

  @doc "The one `worker_swapped` event of a swap, once the sessions are back."
  def emit_pending_swap_telemetry(%{pending_swap_telemetry: nil} = state), do: state

  def emit_pending_swap_telemetry(%{pending_swap_telemetry: payload} = state) do
    metadata = Map.put(payload.metadata, :producer_id_after, fixed_producer_id(state))
    emit_worker_swapped(payload.measurements, metadata)
    %{state | pending_swap_telemetry: nil}
  end

  # A swap is the worker's, not one lane's: its event names the fixed partition
  # and that lane's producer ids, or none when the worker chooses partitions.
  defp worker_meta(state), do: %{partition_meta(state) | partition: state.fixed_partition}

  defp fixed_producer_id(%{fixed_partition: nil}), do: nil
  defp fixed_producer_id(state), do: Lanes.all(state)[state.fixed_partition][:producer_id]
end
