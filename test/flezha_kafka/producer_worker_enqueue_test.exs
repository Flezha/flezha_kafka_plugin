defmodule FlezhaKafka.ProducerWorkerEnqueueTest do
  alias Flezha.Process.Backlog
  alias FlezhaKafka.ProducerWorker
  alias FlezhaKafka.WorkerRegistry
  alias FlezhaKafka.Test.Fakes, as: Fakes
  alias FlezhaTest.Helper.NodeTestHelper
  # async: false: writes Application env; Mimic global mode; Mimic stubs.

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 3]

  import FlezhaKafka.Test.Fakes,
    only: [
      start_worker: 1,
      attach: 1,
      dispatched?: 1,
      in_flight?: 2,
      queued?: 2,
      window_records: 1
    ]

  setup :set_mimic_global_until_exit

  @topic Fakes.topic()
  @partition Fakes.partition()

  test "successful enqueue replies :ok and emits produce.ok telemetry" do
    {pid, _script} =
      start_worker(script: [{:return, Fakes.ok_response(@topic, @partition, 42)}])

    attach(:produce_ok)
    assert :ok = ProducerWorker.enqueue(pid, "hello", %{})
    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :shipped], measurements, metadata}
    assert measurements.batch_size == 1
    assert metadata.topic == @topic
  end

  describe "cast_enqueue/3" do
    test "admission below queue_max returns :ok and queue depth increases" do
      {pid, script_pid} =
        start_worker(
          queue_max: 100,
          linger_ms: 1
        )

      Agent.update(script_pid, fn _ ->
        List.duplicate({:return, {:delayed, Fakes.ok_response(@topic, @partition), 150}}, 5)
      end)

      assert :ok = ProducerWorker.cast_enqueue(pid, "first", %{})
      # The linger fires and the first record goes out alone.
      assert in_flight?(pid, 1)

      assert :ok = ProducerWorker.cast_enqueue(pid, "msg-1", %{})
      assert :ok = ProducerWorker.cast_enqueue(pid, "msg-2", %{})
      assert :ok = ProducerWorker.cast_enqueue(pid, "msg-3", %{})
      assert queued?(pid, 3)

      stats = ProducerWorker.stats(pid)
      assert stats.queue_depth == 3
      assert stats.in_flight == 1
    end

    test "sustained casts above drain rate trigger drop-oldest telemetry; mailbox stays bounded" do
      {pid, script_pid} =
        start_worker(
          queue_max: 5,
          queue_overflow: :drop_oldest,
          linger_ms: 1
        )

      Fakes.script_slow_first(script_pid, 150, 0)

      attach(:dropped)

      :ok = ProducerWorker.cast_enqueue(pid, "first", %{})
      assert in_flight?(pid, 1)

      for i <- 1..200 do
        :ok = ProducerWorker.cast_enqueue(pid, "msg-#{i}", %{})
      end

      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :overflowed], %{count: 1},
                      %{overflow_strategy: :drop_oldest}},
                     500

      # Every cast has been handled once a call queued behind them answers.
      _ = ProducerWorker.stats(pid)
      {:message_queue_len, qlen} = Process.info(pid, :message_queue_len)
      assert qlen <= 5

      # One in flight, five queued, the other 195 evicted — and readable.
      stats = ProducerWorker.stats(pid)
      assert stats.queue_depth == 5
      assert stats.dropped_total == 195
    end

    test "drop-newest under cast emits dropped telemetry without replying" do
      {pid, script_pid} =
        start_worker(
          queue_max: 1,
          queue_overflow: :drop_newest,
          linger_ms: 1
        )

      Fakes.script_slow_first(script_pid, 150, 0)

      attach(:dropped)

      :ok = ProducerWorker.cast_enqueue(pid, "first", %{})
      assert in_flight?(pid, 1)
      :ok = ProducerWorker.cast_enqueue(pid, "filler", %{})
      assert queued?(pid, 1)
      :ok = ProducerWorker.cast_enqueue(pid, "rejected", %{})

      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :overflowed], %{count: 1},
                      %{overflow_strategy: :drop_newest}}

      assert Process.alive?(pid)
      assert ProducerWorker.stats(pid).dropped_total == 1
    end

    test ":drop_nothing admits a cast on its way past the bound instead of dropping" do
      cast_path_keeps_every_record(:drop_nothing)
    end

    test "under the declared default a plain record is kept instead of being dropped" do
      cast_path_keeps_every_record(:drop_nothing)
    end

    test "permanent broker error fires :failed with caller: :async; no GenServer.reply" do
      {pid, _} =
        start_worker(script: [{:return, Fakes.err_response(@topic, @partition, 18)}])

      attach(:produce_failed)

      assert :ok = ProducerWorker.cast_enqueue(pid, "x", %{})

      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :failed], _,
                      %{last_reason: :record_list_too_large, caller: :async}},
                     1_000

      assert Process.alive?(pid)
      stats = ProducerWorker.stats(pid)
      assert stats.queue_depth == 0
      assert stats.last_error == :record_list_too_large
    end

    test "delivery_timeout exhaustion on cast-admitted record fires :failed with caller: :async" do
      {pid, _} =
        Fakes.start_exhausting_worker()

      attach(:produce_failed)

      assert :ok = ProducerWorker.cast_enqueue(pid, "x", %{})

      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :failed], _,
                      %{last_reason: :no_broker, caller: :async}},
                     1_000

      assert Process.alive?(pid)
    end

    test "mixed sync + async records: sync gets reply, async gets telemetry" do
      {pid, _} =
        start_worker(
          linger_ms: 50,
          script: [{:return, Fakes.err_response(@topic, @partition, 18)}]
        )

      attach(:produce_failed)

      sync_caller =
        Task.async(fn ->
          ProducerWorker.enqueue(pid, "sync-msg", %{})
        end)

      # The sync record is queued (the linger is still open) when the async
      # one joins it: both ride one batch, one failure.
      assert queued?(pid, 1)
      :ok = ProducerWorker.cast_enqueue(pid, "async-msg", %{})

      assert {:error, {:permanent, :record_list_too_large}} = Task.await(sync_caller, 1_000)

      events = collect_failed_events(2, 500)
      callers = Enum.map(events, fn {_, _, _, md} -> md.caller end) |> Enum.sort()
      assert callers == [:async, :sync]
    end
  end

  defp collect_failed_events(0, _timeout), do: []

  defp collect_failed_events(n, timeout) do
    receive do
      {:telemetry, [:flezha, :plugin, :kafka, :failed], _, _} = ev ->
        [ev | collect_failed_events(n - 1, timeout)]
    after
      timeout -> []
    end
  end

  # Four plain records cast straight to a `queue_max: 1` worker whose first batch
  # is slow: a cast never blocks its caller and nothing is evicted. The records
  # past the bound were on their way, so they are admitted and produced.
  defp cast_path_keeps_every_record(queue_overflow) do
    {pid, script_pid} =
      start_worker(queue_max: 1, queue_overflow: queue_overflow, linger_ms: 1)

    Fakes.script_slow_first(script_pid)

    attach(:dropped)

    for payload <- ["first", "filler", "waits-1", "waits-2"] do
      :ok = ProducerWorker.cast_enqueue(pid, payload, %{})
    end

    # Every cast has been admitted (or evicted) once a call queued behind them
    # answers; a drop would have emitted before that.
    _ = ProducerWorker.stats(pid)
    refute_received {:telemetry, [:flezha, :plugin, :kafka, :overflowed], _, _}

    assert sent_values(MapSet.new(), 4) == MapSet.new(["first", "filler", "waits-1", "waits-2"])
    assert ProducerWorker.stats(pid).dropped_total == 0
  end

  # The values of every batch sent, until `n` were seen.
  defp sent_values(seen, n) do
    if MapSet.size(seen) >= n do
      seen
    else
      assert_receive {:send_called, request, _ns}, 2_000
      [%{data: [%{record_set: rb}]}] = request.topic_data
      sent_values(MapSet.union(seen, MapSet.new(rb.records, & &1.value)), n)
    end
  end

  describe "sustained produce stall forces a fresh broker connection" do
    test "resumes producing after K give-ups without the shared worker dying" do
      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      identity = %{hosts: [{"k", 9092}]}
      fresh = spawn(&NodeTestHelper.park/0)

      Mimic.stub(WorkerRegistry, :force_replace, fn ^identity -> {:ok, fresh} end)

      # Two full batch give-ups (each exhausts retries fast), then the broker
      # is reachable again and the third batch succeeds — recovery rebinds onto
      # a freshly replaced client (no reliance on the swap fan-out).
      {pid, _} =
        start_worker(
          retry_backoff_ms: 1,
          retry_max_backoff_ms: 3,
          delivery_timeout_ms: 30,
          retries: 1,
          linger_ms: 1,
          script:
            List.duplicate({:return, {:error, :no_broker}}, 4) ++
              List.duplicate({:return, Fakes.ok_response(@topic, @partition, 500)}, 4),
          extra: [stall_threshold: 2, connection_identity: identity]
        )

      Fakes.seed_registry_ref(pid, registry_ref)

      assert {:error, {:delivery_failed, :no_broker}} = ProducerWorker.enqueue(pid, "m1", %{})

      # A no-broker error is authoritative: the FIRST such batch forces a fresh
      # client immediately (throttled), resetting the counter — recovery no
      # longer waits for the stall threshold a low-traffic producer may never
      # reach.
      assert :sys.get_state(pid).consecutive_failures == 0

      assert {:error, {:delivery_failed, :no_broker}} = ProducerWorker.enqueue(pid, "m2", %{})

      # The throttle window blocks a second immediate refresh, so this give-up
      # only increments the counter (still below the stall threshold) — the
      # worker keeps draining on the same pid.
      assert :sys.get_state(pid).consecutive_failures == 1
      assert Process.alive?(pid)

      attach(:produce_ok)
      assert :ok = ProducerWorker.enqueue(pid, "m3", %{})

      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :shipped], _, _}
      assert :sys.get_state(pid).consecutive_failures == 0
    end

    test "refresh_broker/1 cast forces a fresh connection on demand" do
      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      identity = %{hosts: [{"k", 9092}]}
      test_pid = self()
      fresh = spawn(&NodeTestHelper.park/0)

      Mimic.stub(WorkerRegistry, :force_replace, fn ^identity ->
        send(test_pid, :refresh_called)
        {:ok, fresh}
      end)

      {pid, _} =
        start_worker(
          script: [{:return, Fakes.ok_response(@topic, @partition)}],
          extra: [connection_identity: identity]
        )

      Fakes.seed_registry_ref(pid, registry_ref)
      :sys.replace_state(pid, fn s -> %{s | consecutive_failures: 5} end)

      ProducerWorker.refresh_broker(pid)

      assert_receive :refresh_called, 500
      assert :sys.get_state(pid).consecutive_failures == 0
    end
  end

  describe "worker swap on {:kafka_worker_dead, ...}" do
    test "swap preserves queue and re-issues InitProducerId on the new worker" do
      call_count = :counters.new(1, [])

      init_producer_id_fn = fn _w, _t ->
        :counters.add(call_count, 1, 1)

        case :counters.get(call_count, 1) do
          1 -> {:ok, %{producer_id: 4242, producer_epoch: 5}}
          _ -> {:ok, %{producer_id: 9999, producer_epoch: 7}}
        end
      end

      {pid, script_pid} =
        start_worker(
          init_producer_id_fn: init_producer_id_fn,
          queue_max: 100,
          linger_ms: 5
        )

      Agent.update(script_pid, fn _ -> List.duplicate({:return, :never}, 10) end)

      :ok = ProducerWorker.cast_enqueue(pid, "a", %{})
      :ok = ProducerWorker.cast_enqueue(pid, "b", %{})
      :ok = ProducerWorker.cast_enqueue(pid, "c", %{})
      assert dispatched?(pid)

      pre = :sys.get_state(pid)
      assert pre.producer_id == 4242
      assert pre.producer_epoch == 5

      {registry_ref, new_worker_pid} = Fakes.expect_worker_swap(pid)

      send(pid, {:kafka_worker_dead, registry_ref, :killed})

      post = :sys.get_state(pid)
      assert post.worker_pid == new_worker_pid
      assert post.producer_id == 9999
      assert post.producer_epoch == 7
      assert post.pending_swap_telemetry == nil

      # All three records still in-system (queue + in_flight).
      assert post.queue.frames + window_records(post) == 3
    end

    test "re-admitted in_flight records retain from/metadata/timestamp/bytes/key" do
      {pid, script_pid} =
        start_worker(
          init_producer_id_fn: Fakes.init_producer_id_ok(),
          linger_ms: 1
        )

      Agent.update(script_pid, fn _ -> List.duplicate({:return, :never}, 5) end)

      :ok = ProducerWorker.cast_enqueue(pid, "payload-1", %{"k" => "v"})
      :ok = ProducerWorker.cast_enqueue(pid, "payload-2", %{"flezha.dedup_key" => "abc"})
      assert dispatched?(pid)

      pre = :sys.get_state(pid)
      in_flight_records = Enum.flat_map(pre.in_flight, & &1.records)
      assert in_flight_records != []

      {registry_ref, _new_worker_pid} = Fakes.expect_worker_swap(pid)

      send(pid, {:kafka_worker_dead, registry_ref, :killed})

      post = :sys.get_state(pid)

      # After the swap, the rewound records may still be in queue OR have
      # been kicked into the new in_flight batch — assert they remain in
      # the producer's bookkeeping with their original fields intact.
      in_post = Backlog.to_list(post.queue) ++ Enum.flat_map(post.in_flight, & &1.records)

      preserved = Enum.take(in_post, length(in_flight_records))

      assert Enum.map(preserved, & &1.value) == Enum.map(in_flight_records, & &1.value)
      assert Enum.map(preserved, & &1.metadata) == Enum.map(in_flight_records, & &1.metadata)
      assert Enum.map(preserved, & &1.timestamp) == Enum.map(in_flight_records, & &1.timestamp)
      assert Enum.map(preserved, & &1.bytes) == Enum.map(in_flight_records, & &1.bytes)
      assert Enum.map(preserved, & &1.key) == Enum.map(in_flight_records, & &1.key)
    end

    test "telemetry [:worker, :swapped] fires once with measurements + metadata" do
      {pid, script_pid} =
        start_worker(
          init_producer_id_fn: Fakes.init_producer_id_ok(1234, 0),
          linger_ms: 1
        )

      Agent.update(script_pid, fn _ -> List.duplicate({:return, :never}, 5) end)

      :ok = ProducerWorker.cast_enqueue(pid, "x", %{})
      :ok = ProducerWorker.cast_enqueue(pid, "y", %{})
      assert dispatched?(pid)

      {registry_ref, new_worker_pid} = Fakes.expect_worker_swap(pid)

      attach(:worker_swapped)
      send(pid, {:kafka_worker_dead, registry_ref, :killed})

      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :worker_swapped], measurements,
                      metadata},
                     500

      assert Map.has_key?(measurements, :queue_depth)
      assert Map.has_key?(measurements, :in_flight_records)
      assert Map.has_key?(measurements, :queue_bytes)
      assert metadata.topic == @topic
      assert metadata.partition == @partition
      _ = :sys.get_state(pid)
      assert metadata.reason == :killed
      assert metadata.producer_id_before == 1234
      assert metadata.new_worker_pid == new_worker_pid
      assert is_pid(metadata.old_worker_pid)
      assert is_integer(metadata.producer_id_after) or is_nil(metadata.producer_id_after)

      refute_received {:telemetry, [:flezha, :plugin, :kafka, :worker_swapped], _, _}
    end

    test "no :produce :failed events fire for queued records during the swap window" do
      {pid, script_pid} =
        start_worker(
          init_producer_id_fn: Fakes.init_producer_id_ok(),
          linger_ms: 5
        )

      Agent.update(script_pid, fn _ -> List.duplicate({:return, :never}, 5) end)

      :ok = ProducerWorker.cast_enqueue(pid, "x", %{})
      :ok = ProducerWorker.cast_enqueue(pid, "y", %{})
      assert dispatched?(pid)

      {registry_ref, _new_worker_pid} = Fakes.expect_worker_swap(pid)

      attach(:produce_failed)
      send(pid, {:kafka_worker_dead, registry_ref, :killed})

      # The swap is handled ahead of this call; a failure would have fired inside it.
      _ = :sys.get_state(pid)
      refute_received {:telemetry, [:flezha, :plugin, :kafka, :failed], _, _}
    end

    test "defensive :EXIT from unrelated pid does not crash the producer" do
      {pid, _script} =
        start_worker(script: [{:return, Fakes.ok_response(@topic, @partition)}])

      assert Process.alive?(pid)

      # NOTE: send from a non-parent pid — gen_server.erl turns
      # `{:EXIT, parent_pid, :normal}` into a real terminate call.
      other = spawn(&NodeTestHelper.park/0)
      send(pid, {:EXIT, other, :killed})
      _ = :sys.get_state(pid)

      assert Process.alive?(pid),
             ":EXIT from unrelated pid must not crash the producer"
    end

    test "stale {:kafka_worker_dead, old_ref, _} is dropped without altering queue" do
      {pid, _script} =
        start_worker(script: [{:return, Fakes.ok_response(@topic, @partition)}])

      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      Fakes.seed_registry_ref(pid, registry_ref)

      pre = :sys.get_state(pid)

      send(pid, {:kafka_worker_dead, {{[{"k", 9092}], 0, 0}, make_ref()}, :killed})

      post = :sys.get_state(pid)
      assert post.queue == pre.queue
      assert post.registry_ref == registry_ref
      assert Process.alive?(pid)
    end

    test "swap failure schedules retry within delivery_timeout_ms (happy retry)" do
      {pid, script_pid} =
        start_worker(
          init_producer_id_fn: Fakes.init_producer_id_ok(),
          linger_ms: 1,
          retry_backoff_ms: 1,
          delivery_timeout_ms: 10_000
        )

      Agent.update(script_pid, fn _ -> List.duplicate({:return, :never}, 5) end)

      :ok = ProducerWorker.cast_enqueue(pid, "x", %{})
      assert dispatched?(pid)

      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      Fakes.seed_registry_ref(pid, registry_ref)
      new_worker_pid = spawn(&NodeTestHelper.park/0)

      counter = :counters.new(1, [])

      Mimic.stub(WorkerRegistry, :swap, fn ^registry_ref ->
        :counters.add(counter, 1, 1)

        if :counters.get(counter, 1) < 3 do
          {:error, :no_broker}
        else
          {:ok, new_worker_pid, {elem(registry_ref, 0), make_ref()}}
        end
      end)

      send(pid, {:kafka_worker_dead, registry_ref, :killed})

      # Two refused swaps back off before the third succeeds.
      assert wait_until(fn -> :sys.get_state(pid).worker_pid == new_worker_pid end, 2_000, 5)

      post = :sys.get_state(pid)
      assert post.worker_pid == new_worker_pid
      assert post.swap_attempt == 0
      assert post.swap_started_at == nil
    end

    test "swap exhaustion empties the queue, marks :swap_exhausted, keeps the producer alive (exhaustion)" do
      {pid, script_pid} =
        start_worker(
          init_producer_id_fn: Fakes.init_producer_id_ok(),
          linger_ms: 1,
          retry_backoff_ms: 1,
          retry_max_backoff_ms: 3,
          delivery_timeout_ms: 50
        )

      Agent.update(script_pid, fn _ -> List.duplicate({:return, :never}, 5) end)

      :ok = ProducerWorker.cast_enqueue(pid, "x", %{})
      :ok = ProducerWorker.cast_enqueue(pid, "y", %{})
      assert dispatched?(pid)

      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      Fakes.seed_registry_ref(pid, registry_ref)

      # Backdate swap_started_at so the first swap-attempt is already past
      # delivery_timeout_ms; the first {:error, _} should exhaust immediately.
      now = System.monotonic_time(:millisecond)

      :sys.replace_state(pid, fn state ->
        %{state | swap_started_at: now - 1_000}
      end)

      Mimic.stub(WorkerRegistry, :swap, fn ^registry_ref -> {:error, :no_broker} end)

      attach(:produce_failed)

      send(pid, {:kafka_worker_dead, registry_ref, :killed})

      # The exhaustion bookkeeping empties the queue and the window.
      assert wait_until(
               fn ->
                 state = :sys.get_state(pid)
                 state.queue.frames == 0 and state.in_flight == []
               end,
               2_000,
               5
             )

      assert Process.alive?(pid),
             "producer must stay alive after exhaustion (no supervisor crash loop)"

      state = :sys.get_state(pid)
      assert state.queue.frames == 0
      assert state.in_flight == []
      assert state.swap_attempt == 0
      assert state.swap_started_at == nil
      assert ProducerWorker.stats(pid).last_error == {:swap_exhausted, :no_broker}

      # Every queued record SHOULD have produced a :failed telemetry event.
      events = collect_failed_events(2, 200)
      assert length(events) == 2
    end

    test "swap falls back to acquire/2 when registry returns :no_pending_swap" do
      identity = %{hosts: [{"k", 9092}]}

      {pid, script_pid} =
        start_worker(
          init_producer_id_fn: Fakes.init_producer_id_ok(),
          linger_ms: 1,
          extra: [connection_identity: identity]
        )

      Agent.update(script_pid, fn _ -> List.duplicate({:return, :never}, 5) end)

      :ok = ProducerWorker.cast_enqueue(pid, "x", %{})
      assert dispatched?(pid)

      {registry_ref, new_worker_pid} =
        Fakes.expect_worker_swap(pid, {:refused, :no_pending_swap})

      Mimic.expect(WorkerRegistry, :acquire, fn ^identity ->
        {:ok, new_worker_pid, {elem(registry_ref, 0), make_ref()}}
      end)

      send(pid, {:kafka_worker_dead, registry_ref, :killed})

      post = :sys.get_state(pid)
      assert post.worker_pid == new_worker_pid
    end
  end

  # ---------- outbox delivery confirmation ----------
end
