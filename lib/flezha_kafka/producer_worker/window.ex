defmodule FlezhaKafka.ProducerWorker.Window do
  @moduledoc """
  One partition lane's queue and in-flight window: admission against the bounded
  queue, linger batching, the window of Produce requests, retries, fences, the
  give-up path and the outbox settlement of every batch.

  Every function takes the producer worker's state with the lane held
  (`FlezhaKafka.ProducerWorker.Lanes`), so `state.queue`, `state.in_flight` and
  `state.partition` are that lane's and everything else is the worker's. A timer
  or a spawned helper names the partition it belongs to, so its message finds the
  lane again.
  """

  require Logger

  alias Flezha.SDK.Knob
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.State.Outbox
  alias FlezhaKafka.ProducerWorker.Client
  alias FlezhaKafka.ProducerWorker.Wire
  alias KafkaEx.Client.NodeSelector

  # The produce retry ladder; `retry_backoff_ms` and `retry_max_backoff_ms`
  # bound it per plugin.
  @produce_ladder %{base: 100, cap: 5_000, jitter: true, reset: :per_call}

  # A full queue never discards a record unless the user chose a drop
  # strategy: the default is `:drop_nothing` for every record, guaranteed or not.
  def admit_record(state, record, from_or_async) do
    if state.queue_size < state.queue_max do
      {:admitted, push(state, record)}
    else
      overflow(state, record, from_or_async, state.queue_overflow)
    end
  end

  defp overflow(state, record, from_or_async, :drop_newest) do
    state = record_drop(state, record, :drop_newest)

    case from_or_async do
      :async -> {:dropped_async, state}
      _from -> {:replied, {:error, {:queue_full, :overflow}}, state}
    end
  end

  defp overflow(state, record, _from_or_async, :drop_oldest) do
    {state, evicted} = pop(state)
    reply_to_record(evicted, {:error, {:queue_full, :evicted}})
    {:admitted, state |> record_drop(evicted, :drop_oldest) |> push(record)}
  end

  # Room is made by a drained batch, never by evicting an admitted record. A
  # synchronous caller waits out its own timeout; a cast-admitted record simply
  # waits — the enqueueing process is not blocked either way.
  defp overflow(state, record, :async, :drop_nothing),
    do: {:blocked, %{state | blocked: :queue.in({:async, record}, state.blocked)}}

  defp overflow(state, record, from, :drop_nothing) do
    Process.send_after(
      self(),
      {:block_timeout, state.partition, from},
      state.queue_block_timeout_ms
    )

    {:blocked, %{state | blocked: :queue.in({from, record}, state.blocked)}}
  end

  # Every drop is counted for `stats/1` — telemetry alone is invisible to a
  # runner polling the worker. A dropped delivery-guaranteed record is
  # alarm-grade on top: its outbox cursor stays where it is, so the loss is
  # visible as an outbox that stops draining rather than as a counter nobody
  # reads.
  defp record_drop(state, record, strategy) do
    guaranteed? = record != nil and Outbox.guaranteed?(record.metadata)

    if guaranteed? do
      Logger.error(
        "#{tag(state)} " <>
          "dropped a delivery-guaranteed record under :queue_overflow = #{strategy}; " <>
          "its outbox cursor will not advance until the entry is re-driven"
      )
    end

    emit_produce_dropped(
      1,
      Map.merge(partition_meta(state), %{
        queue_depth: state.queue_size,
        overflow_strategy: strategy,
        guaranteed: guaranteed?,
        reason: :overflow
      })
    )

    %{state | dropped_total: state.dropped_total + 1}
  end

  # The outbox confirmation seam: an acked batch confirms every guaranteed
  # record it carried in ONE report, and a permanently failed one parks them
  # with their own identity — a recorded, replayable loss instead of a silent
  # one, and a park carries a payload, so it stays per record. Records carrying
  # no token belong to a plain chain and settle nothing.
  defp settle_outbox(batch, :ok), do: Outbox.confirm_many(Enum.map(batch.records, & &1.metadata))

  defp settle_outbox(batch, {:error, {_kind, reason}}),
    do: Enum.each(batch.records, &Outbox.park(&1.metadata, &1.value, reason, batch.attempt + 1))

  def reply_to_record(nil, _reply), do: :ok
  def reply_to_record(%{from: :async}, _reply), do: :ok
  def reply_to_record(%{from: from}, reply), do: GenServer.reply(from, reply)

  defp push(state, record) do
    %{
      state
      | queue: :queue.in(record, state.queue),
        queue_size: state.queue_size + 1,
        queue_bytes: state.queue_bytes + record.bytes
    }
  end

  defp pop(state) do
    case :queue.out(state.queue) do
      {{:value, record}, q} ->
        {%{
           state
           | queue: q,
             queue_size: state.queue_size - 1,
             queue_bytes: state.queue_bytes - record.bytes
         }, record}

      {:empty, _} ->
        {state, nil}
    end
  end

  @doc "A synchronous caller's wait for room ran out: it is answered `:queue_full`."
  def block_timeout(state, from) do
    {dropped, blocked} = drop_blocked(state.blocked, from)

    if dropped do
      GenServer.reply(from, {:error, {:queue_full, :timeout}})
    end

    %{state | blocked: blocked}
  end

  defp drop_blocked(queue, from) do
    list = :queue.to_list(queue)
    {dropped, kept} = Enum.split_with(list, fn {f, _} -> f == from end)
    {dropped != [], :queue.from_list(kept)}
  end

  # A batch is cut against a free window slot: while the window is full — or
  # while it is draining for an ordered resend — arrivals accumulate into the
  # next one instead.
  def maybe_kick_batch(state) do
    cond do
      not slot_free?(state) or state.queue_size == 0 ->
        state

      state.queue_bytes >= state.max_batch_bytes ->
        send(self(), {:flush_batch, state.partition})
        state

      state.linger_timer == nil ->
        timer =
          if state.linger_ms == 0 do
            send(self(), {:flush_batch, state.partition})
            :immediate
          else
            Process.send_after(self(), {:flush_batch, state.partition}, state.linger_ms)
          end

        %{state | linger_timer: timer}

      true ->
        state
    end
  end

  def maybe_send_batch(state) do
    if slot_free?(state), do: send_drained(state), else: state
  end

  defp send_drained(state) do
    case drain_batch(state, state.max_batch_bytes) do
      {[], state} ->
        state

      {records, state} ->
        batch = new_batch(state, records)
        next_seq = if state.idempotent, do: state.sequence + length(records), else: 0

        %{state | sequence: next_seq, in_flight: state.in_flight ++ [batch]}
        |> do_send(batch)
        |> rotate_sticky()
        |> maybe_kick_batch()
    end
  end

  # The sticky partition moves to another one, chosen at random, once a batch
  # has left from it, so unkeyed records spread over the topic batch by batch.
  defp rotate_sticky(%{partition: p, sticky: p, partitions: n} = state) when n > 1,
    do: %{state | sticky: rem(p + :rand.uniform(n - 1), n)}

  defp rotate_sticky(state), do: state

  # ------------------------------------------------------------------
  # The window: the batches outstanding for this partition, oldest first. A
  # batch carries two identities — `id` is stable across retries and names it
  # to everything the worker sends itself, `ref` is the client's handle for the
  # current dispatch and names it to that dispatch's outcome. Clearing `ref` is
  # how a dispatch is abandoned: a late outcome then names no batch at all.
  # `status` is about outcomes, not the wire: `:sent` means one is outstanding
  # (a response, or a topic creation / metadata refresh issued for this batch),
  # `:pending` means the batch is owed a resend.
  # ------------------------------------------------------------------

  defp new_batch(state, records) do
    %{
      id: make_ref(),
      ref: nil,
      records: records,
      base_sequence: state.sequence,
      attempt: 0,
      first_attempt_at: state.now_fn.(),
      last_error: nil,
      status: :sent,
      dispatch_timer: nil
    }
  end

  # Nothing new leaves while a batch is owed a resend: the broker must see the
  # window in its original order, so k goes out again before anything newer.
  defp slot_free?(state) do
    length(state.in_flight) < state.max_in_flight and
      not Enum.any?(state.in_flight, &(&1.status == :pending))
  end

  defp oldest?(%{in_flight: [%{id: id} | _]}, %{id: id}), do: true
  defp oldest?(_state, _batch), do: false

  # A resend waits until every batch sent after the failed one has come back:
  # the wire order must be k, k+1, … again, so nothing goes out mid-window.
  def maybe_resend(state) do
    if Enum.any?(state.in_flight, &(&1.status == :sent)),
      do: state,
      else: schedule_retry(state)
  end

  @doc "The resend timer of `batch_id` fired: the window goes out again if that batch is owed."
  def retry_window(state, batch_id) do
    state = %{state | retry_timer: nil}

    if Enum.any?(state.in_flight, &(&1.id == batch_id and &1.status == :pending)),
      do: resend_window(state),
      else: state
  end

  # Everything still owed goes out again oldest first, under its original
  # sequence numbers, before anything newer is cut.
  defp resend_window(state) do
    state.in_flight
    |> Enum.filter(&(&1.status == :pending))
    |> Enum.reduce(state, fn batch, acc -> do_send(acc, batch) end)
  end

  # A fresh producer session numbers from zero, so every batch still owed is
  # renumbered in window order and owes another attempt. Whatever was on the
  # wire under the old session is abandoned: its dispatch handle is dropped, so
  # a late outcome names no batch.
  defp realign_window(state) do
    {batches, sequence} =
      Enum.map_reduce(state.in_flight, state.sequence, fn batch, seq ->
        batch = %{
          cancel_dispatch_timer(batch)
          | base_sequence: seq,
            attempt: batch.attempt + 1,
            status: :pending,
            ref: nil
        }

        {batch, seq + length(batch.records)}
      end)

    %{state | in_flight: batches, sequence: if(state.idempotent, do: sequence, else: 0)}
  end

  # Runs `fun` against the batch a dispatch handle names, having first taken the
  # dispatch off it — the outcome is in hand, so its timer and handle are spent.
  def with_dispatch(state, ref, fun) do
    case Enum.find(state.in_flight, &(&1.ref == ref)) do
      nil ->
        state

      batch ->
        batch = %{cancel_dispatch_timer(batch) | ref: nil}
        fun.(put_batch(state, batch), batch)
    end
  end

  def with_batch(state, batch_id, fun) do
    case Enum.find(state.in_flight, &(&1.id == batch_id)) do
      nil -> state
      batch -> fun.(state, batch)
    end
  end

  defp put_batch(state, batch) do
    %{state | in_flight: Enum.map(state.in_flight, &if(&1.id == batch.id, do: batch, else: &1))}
  end

  defp drop_batch(state, batch) do
    %{state | in_flight: Enum.reject(state.in_flight, &(&1.id == batch.id))}
  end

  # The whole window returns to the head of the queue, oldest batch first, so
  # the fresh producer session re-sends it in admission order.
  def rewind_in_flight(state, reason) do
    records = Enum.flat_map(state.in_flight, & &1.records)
    bytes_back = Enum.reduce(records, 0, fn r, n -> n + r.bytes end)

    %{
      state
      | queue: :queue.join(:queue.from_list(records), state.queue),
        queue_size: state.queue_size + length(records),
        queue_bytes: state.queue_bytes + bytes_back,
        in_flight: [],
        last_error: {:worker_died, reason}
    }
  end

  defp drain_batch(state, max_bytes), do: drain(state, max_bytes, [], 0)

  # `acc_bytes` rides along: re-summing the accumulator per record made a
  # batch of n records cost n² additions.
  defp drain(state, max_bytes, acc, acc_bytes) do
    case :queue.peek(state.queue) do
      :empty ->
        {Enum.reverse(acc), state}

      {:value, record} ->
        if acc != [] and acc_bytes + record.bytes > max_bytes do
          {Enum.reverse(acc), state}
        else
          {state, _} = pop(state)
          drain(state, max_bytes, [record | acc], acc_bytes + record.bytes)
        end
    end
  end

  # The client answers by message, so the worker sends from its own process and
  # the dispatch handle it hands back identifies the batch. A handle is fresh
  # per dispatch: a late outcome from a prior hung send does not name a batch
  # in the window and is ignored, so a slow send can't double-apply.
  defp do_send(state, batch) do
    request = Wire.build_produce_request(state, batch.records, batch.base_sequence)
    selector = NodeSelector.topic_partition(state.topic, state.partition)

    # A crash or call timeout in the shared client (e.g. a wedged socket after a
    # broker restart) must never silently strand the worker: turn it into a
    # retriable outcome that flows through the one result path, so every batch
    # in the window resolves and nothing resolves re-entrantly mid-resend.
    outcome =
      try do
        state.send_fn.(state.worker_pid, request, selector)
      catch
        :exit, reason -> {:error, {:exit, reason}}
        kind, reason -> {:error, {:send_crashed, kind, reason}}
      end

    {ref, timer} =
      case outcome do
        {:ok, ref} ->
          {ref,
           Process.send_after(
             self(),
             {:dispatch_timeout, state.partition, ref},
             state.dispatch_timeout_ms
           )}

        {:error, _reason} = error ->
          ref = make_ref()
          send(self(), {:kafka_ex_response, ref, error})
          {ref, nil}
      end

    put_batch(state, %{
      cancel_dispatch_timer(batch)
      | ref: ref,
        status: :sent,
        dispatch_timer: timer
    })
  end

  def handle_batch_result(state, batch, result) do
    case Wire.classify_result(result, state) do
      {:ok, base_offset} ->
        deliver_batch(state, batch, base_offset)

      {:already_delivered, reason} ->
        Logger.info("#{tag(state)} broker reports #{inspect(reason)} — treating as delivered")
        deliver_batch(state, batch, -1)

      {:retry, reason} ->
        retry_batch(state, batch, reason)

      {:fenced, reason} ->
        fence_batch(state, batch, reason)

      {:permanent, reason} ->
        emit_failed_per_record(state, batch, reason)
        complete_batch(state, batch, {:error, {:permanent, reason}})
    end
  end

  defp deliver_batch(state, batch, base_offset) do
    emit_produce_ok(
      length(batch.records),
      elapsed_ms(state, batch),
      Map.merge(partition_meta(state), %{base_offset: base_offset, attempt: batch.attempt + 1})
    )

    complete_batch(state, batch, :ok)
  end

  defp retry_batch(state, batch, reason) do
    cond do
      exhausted?(state, batch) ->
        fail_batch(state, batch, reason)

      reason == :unknown_topic_or_partition and not state.auto_create_topic ->
        fail_batch(state, batch, :unknown_topic_or_partition)

      # The recovery action is this batch's outstanding outcome, so the window
      # keeps waiting on it rather than resending around it.
      reason == :unknown_topic_or_partition ->
        {state, batch} = retrying(state, batch, reason, :sent)
        issue_create_topic(state, batch)
        state

      reason in [:not_leader_for_partition, :no_broker] ->
        {state, batch} = retrying(state, batch, reason, :sent)
        issue_metadata_refresh(state, batch)
        state

      true ->
        {state, _batch} = retrying(state, batch, reason, :pending)
        maybe_resend(state)
    end
  end

  # The producer session is fenced and nothing in the window was stored. Re-init
  # the session on the same (healthy) broker client, renumber every batch still
  # owed in window order, and resend them in that order with backoff — never ack
  # a fenced batch. Bounded by retries / delivery_timeout so a re-fencing broker
  # can't spin; on exhaustion fail_batch/3 increments consecutive_failures and
  # maybe_refresh_broker/2 forces a fresh broker client.
  defp fence_batch(state, batch, reason) do
    cond do
      # An earlier batch is still unresolved, so the broker rejected this one
      # for the sequence gap that batch left, not for a fenced session.
      not oldest?(state, batch) ->
        {state, _batch} = retrying(state, batch, reason, :pending)
        maybe_resend(state)

      exhausted?(state, batch) ->
        fail_batch(state, batch, reason)

      true ->
        emit_retry(state, batch, reason)

        %{state | last_error: reason}
        |> restart_producer_session()
        |> schedule_retry()
    end
  end

  defp exhausted?(state, batch) do
    batch.attempt + 1 > state.retries or elapsed_ms(state, batch) >= state.delivery_timeout_ms
  end

  # One more attempt is owed. `:sent` parks the batch behind a recovery action
  # that is still running; `:pending` puts it in the resend queue.
  defp retrying(state, batch, reason, status) do
    emit_retry(state, batch, reason)

    batch = %{
      cancel_dispatch_timer(batch)
      | attempt: batch.attempt + 1,
        last_error: reason,
        status: status,
        ref: nil
    }

    {put_batch(%{state | last_error: reason}, batch), batch}
  end

  defp emit_retry(state, batch, reason) do
    emit_produce_retry(
      batch.attempt + 1,
      retry_backoff_ms(state, batch.attempt + 1),
      Map.put(partition_meta(state), :reason, reason)
    )
  end

  # The recovery action this batch was waiting on is done; it re-joins the
  # resend queue.
  def resume_owed(state, batch) do
    state |> put_batch(%{batch | status: :pending}) |> maybe_resend()
  end

  def handle_topic_create_result(state, batch, result) do
    case result do
      :ok ->
        emit_topic_created(
          state.auto_create_partitions,
          state.auto_create_replication_factor,
          %{
            plugin_id: state.plugin_id,
            topic: state.topic
          }
        )

        resume_owed(state, batch)

      {:error, :topic_already_exists} ->
        resume_owed(state, batch)

      {:error, reason} ->
        fail_batch(state, batch, reason)
    end
  end

  defp issue_create_topic(state, batch) do
    parent = self()
    partition = state.partition
    id = batch.id
    create_fn = state.create_topic_fn
    count_fn = partition_count_reader(state)
    worker = state.worker_pid
    topic = state.topic

    opts = %{
      num_partitions: state.auto_create_partitions,
      replication_factor: state.auto_create_replication_factor,
      config_entries: state.topic_configs,
      timeout_ms: state.metadata_refresh_timeout_ms
    }

    timeout = state.metadata_refresh_timeout_ms

    spawn(fn ->
      # A crash here would leave the batch — and, behind it, the whole window —
      # waiting on an outcome that never arrives.
      result =
        try do
          create_fn.(worker, topic, opts, timeout)
        catch
          :exit, reason -> {:error, {:exit, reason}}
          kind, reason -> {:error, {kind, reason}}
        end

      # The topic now exists with the count it was made with, or another.
      send(parent, {:topic_create_result, partition, id, result, count_fn.(worker)})
    end)

    :ok
  end

  defp issue_metadata_refresh(state, batch) do
    parent = self()
    partition = state.partition
    id = batch.id
    refresh_fn = state.metadata_refresh_fn
    count_fn = partition_count_reader(state)
    worker = state.worker_pid
    started = state.now_fn.()
    timeout = state.metadata_refresh_timeout_ms
    now_fn = state.now_fn

    spawn(fn ->
      err =
        try do
          case refresh_fn.(worker, timeout) do
            :ok -> nil
            {:ok, _} -> nil
            {:error, reason} -> reason
            other -> other
          end
        catch
          :exit, reason -> reason
        end

      latency = now_fn.() - started
      send(parent, {:metadata_refresh_done, partition, id, latency, err, count_fn.(worker)})
    end)

    :ok
  end

  # A refreshed topic may have grown: a worker that chooses partitions reads the
  # count again on the refresh's own process.
  defp partition_count_reader(%{partition_count_fn: count_fn} = state)
       when is_nil(state.fixed_partition) do
    topic = state.topic
    timeout = state.metadata_refresh_timeout_ms
    &count_fn.(&1, topic, timeout)
  end

  defp partition_count_reader(_state), do: fn _worker -> :fixed end

  # One timer for the whole resend run: the oldest owed batch sets the backoff
  # and its arrival resends everything owed behind it.
  defp schedule_retry(state) do
    state = cancel_retry_timer(state)

    case Enum.find(state.in_flight, &(&1.status == :pending)) do
      nil ->
        state

      batch ->
        backoff = retry_backoff_ms(state, batch.attempt)
        message = {:retry_window, state.partition, batch.id}
        %{state | retry_timer: Process.send_after(self(), message, backoff)}
    end
  end

  # `attempt` reaches here as 0 for a batch that has never been retried, which
  # is the ladder's first rung.
  def retry_backoff_ms(state, attempt) do
    row = %{@produce_ladder | base: state.retry_backoff_ms, cap: state.retry_max_backoff_ms}
    delay = Knob.delay(row, max(attempt, 1))
    Knob.announce(:kafka_produce, max(attempt, 1), {state.topic, state.partition}, delay)
  end

  defp fail_batch(state, batch, reason) do
    emit_failed_per_record(state, batch, reason)

    state = %{state | consecutive_failures: state.consecutive_failures + 1}
    state = Client.maybe_refresh_broker(state, reason)

    complete_batch(state, batch, {:error, {:delivery_failed, reason}})
  end

  # (Re-)initialise the idempotent producer session: fetch a fresh
  # producer-id/epoch, reset the sequence, and renumber the window against it.
  # Used both on a producer fence and after a fresh broker client (the old id is
  # invalid there). Non-idempotent producers carry no id, but their window is
  # renumbered all the same — every batch still owed has to go out again.
  def restart_producer_session(%{idempotent: false} = state), do: realign_window(state)

  def restart_producer_session(state) do
    case state.init_producer_id_fn.(state.worker_pid, state.metadata_refresh_timeout_ms) do
      {:ok, %{producer_id: pid, producer_epoch: epoch}} ->
        realign_window(%{state | producer_id: pid, producer_epoch: epoch, sequence: 0})

      _ ->
        realign_window(%{state | producer_id: nil, producer_epoch: nil, sequence: 0})
    end
  end

  defp emit_failed_per_record(state, batch, reason) do
    total_ms = elapsed_ms(state, batch)

    Enum.each(batch.records, fn r ->
      emit_produce_failed(
        batch.attempt + 1,
        total_ms,
        Map.merge(partition_meta(state), %{last_reason: reason, caller: caller_tag(r)})
      )
    end)
  end

  def caller_tag(%{from: :async}), do: :async
  def caller_tag(_), do: :sync

  defp complete_batch(state, batch, reply) do
    Enum.each(batch.records, &reply_to_record(&1, reply))
    settle_outbox(batch, reply)

    state = %{
      drop_batch(state, batch)
      | consecutive_failures: if(reply == :ok, do: 0, else: state.consecutive_failures),
        last_error:
          case reply do
            :ok -> nil
            {:error, {_, reason}} -> reason
          end
    }

    state
    |> process_blocked()
    |> maybe_resend()
    |> maybe_kick_batch()
  end

  defp process_blocked(state) do
    case :queue.out(state.blocked) do
      {:empty, _} ->
        state

      {{:value, {from, record}}, rest} ->
        if state.queue_size < state.queue_max do
          state = %{state | blocked: rest}
          state = push(state, %{record | from: from})
          process_blocked(state)
        else
          state
        end
    end
  end

  defp elapsed_ms(state, batch), do: state.now_fn.() - batch.first_attempt_at

  # The worker's identity, in a log line and in every telemetry event.
  def tag(state),
    do: "[Kafka.ProducerWorker #{state.plugin_id}/#{state.topic}/#{state.partition}]"

  def partition_meta(state) do
    %{plugin_id: state.plugin_id, topic: state.topic, partition: state.partition}
  end

  def cancel_timers(state) do
    if is_reference(state.linger_timer), do: Process.cancel_timer(state.linger_timer)
    state = cancel_retry_timer(state)
    %{state | in_flight: Enum.map(state.in_flight, &cancel_dispatch_timer/1), linger_timer: nil}
  end

  defp cancel_retry_timer(%{retry_timer: t} = state) do
    if is_reference(t), do: Process.cancel_timer(t)
    %{state | retry_timer: nil}
  end

  defp cancel_dispatch_timer(%{dispatch_timer: t} = batch) do
    if is_reference(t), do: Process.cancel_timer(t)
    %{batch | dispatch_timer: nil}
  end

  # This family's telemetry vocabulary. The prefix is derived from the
  # provider's own id by `ProviderBehaviour.emit/4`, so there is no prefix
  # attribute here to drift. `:telemetry.execute/3` runs handlers synchronously
  # in the caller's process, so a handler attached to these events must itself
  # be cheap.
  @provider :kafka

  defp emit(verb, measurements, metadata),
    do: ProviderBehaviour.emit(@provider, verb, measurements, metadata)

  defp emit_produce_ok(batch_size, latency_ms, metadata) do
    emit(:shipped, %{batch_size: batch_size, latency_ms: latency_ms}, metadata)
  end

  defp emit_produce_retry(attempt, backoff_ms, metadata) do
    emit(:retried, %{attempt: attempt, backoff_ms: backoff_ms}, metadata)
  end

  defp emit_produce_dropped(count, metadata) do
    emit(:overflowed, %{count: count}, metadata)
  end

  def emit_produce_failed(attempts, total_ms, metadata) do
    emit(:failed, %{attempts: attempts, total_ms: total_ms}, metadata)
  end

  # Kafka-only facts: no other provider has a broker metadata refresh, a topic
  # to create or a worker to swap, so these stay this provider's own verbs
  # beside the four shared ones.
  def emit_metadata_refresh(latency_ms, metadata) do
    emit(:metadata_refresh, %{latency_ms: latency_ms}, metadata)
  end

  defp emit_topic_created(partitions, replication_factor, metadata) do
    emit(
      :topic_created,
      %{partitions: partitions, replication_factor: replication_factor},
      metadata
    )
  end

  def emit_worker_swapped(measurements, metadata) do
    emit(:worker_swapped, measurements, metadata)
  end
end
