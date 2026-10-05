defmodule FlezhaKafka.ProducerWorkerTierRecordsTest do
  alias FlezhaKafka.Producer
  alias FlezhaKafka.ProducerWorker
  alias FlezhaKafka.Test.Fakes, as: Fakes
  # async: false: writes Application env; Mimic global mode; Mimic stubs.

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaKafka.Test.Helpers, only: [set_mimic_global_until_exit: 1]

  import FlezhaKafka.Test.Fakes,
    only: [start_worker: 1, attach: 1, in_flight?: 2, queued?: 2]

  setup :set_mimic_global_until_exit

  @topic Fakes.topic()
  @partition Fakes.partition()

  describe "effectively-once tier records" do
    @tier_metadata %{
      "channel" => "out",
      "flz.cell" => "MarketRelay/s1/k1",
      "flz.seq" => "42.0",
      "flz.epoch" => "1",
      "flz.key" => "BTC/1m/1720000000",
      "flz.ack" => "held"
    }

    test "flz.key is the message key and the identity header rides along" do
      {pid, _} = start_worker(script: [{:return, Fakes.ok_response(@topic, @partition)}])

      assert :ok = ProducerWorker.enqueue(pid, "candle", @tier_metadata)
      assert_receive {:send_called, request, _selector}
      [%{data: [%{record_set: rb}]}] = request.topic_data
      [record] = rb.records

      assert record.key == "BTC/1m/1720000000"

      assert [
               {"flz.key", "BTC/1m/1720000000"},
               {"flz.cell", "MarketRelay/s1/k1"},
               {"flz.seq", "42.0"},
               {"flz.epoch", "1"},
               {"flezha.dedup_key", dedup}
             ] = Enum.map(record.headers, &{&1.key, &1.value})

      assert dedup == "MarketRelay/s1/k1/42.0/1"
      refute dedup =~ "processor_"
      refute dedup =~ "compute_engine_"
    end

    test "a re-driven record carries identical key, header, and partition" do
      {pid, _} =
        start_worker(
          script: [
            {:return, Fakes.ok_response(@topic, @partition)},
            {:return, Fakes.ok_response(@topic, @partition)}
          ]
        )

      assert :ok = ProducerWorker.enqueue(pid, "candle", @tier_metadata)
      assert_receive {:send_called, req1, _}
      # Restart re-drive re-publishes the same durable identity.
      assert :ok = ProducerWorker.enqueue(pid, "candle", @tier_metadata)
      assert_receive {:send_called, req2, _}

      [%{data: [%{partition: p1, record_set: rb1}]}] = req1.topic_data
      [%{data: [%{partition: p2, record_set: rb2}]}] = req2.topic_data
      [r1] = rb1.records
      [r2] = rb2.records

      assert p1 == p2
      assert r1.key == r2.key
      assert r1.headers == r2.headers
      assert r1.value == r2.value
    end

    test "at-least-once records keep the caller key and carry no headers" do
      {pid, _} = start_worker(script: [{:return, Fakes.ok_response(@topic, @partition)}])

      assert :ok = ProducerWorker.enqueue(pid, "v", %{"kafka_msg_key" => "user-key"})
      assert_receive {:send_called, request, _}
      [%{data: [%{record_set: rb}]}] = request.topic_data
      [record] = rb.records

      assert record.key == "user-key"
      assert record.headers == []
    end

    test "a keyed channel outside the tier still partitions by its natural key" do
      {pid, _} = start_worker(script: [{:return, Fakes.ok_response(@topic, @partition)}])

      # What an ordinary `.keyed()` hybrid channel stamps: cell + seq + key,
      # but no `flz.epoch` — only the effectively-once tier stamps that. A nil
      # key here scatters one cell's records across partitions, losing the
      # ordering `.keyed()` was declared for.
      assert :ok =
               ProducerWorker.enqueue(pid, "candle", %{
                 "channel" => "ohlc_out",
                 "flz.cell" => "MarketRelay/ohlc/BTC-USD",
                 "flz.seq" => "42.0",
                 "flz.key" => "BTC/USD"
               })

      assert_receive {:send_called, request, _}
      [%{data: [%{record_set: rb}]}] = request.topic_data
      [record] = rb.records

      assert record.key == "BTC/USD"

      # Not held by the outbox, so nothing downstream deduplicates it: no headers.
      assert record.headers == []
    end
  end

  test "InitProducerId result is cached and used in subsequent batches" do
    pid_id = 4242
    epoch = 5

    {pid, _} =
      start_worker(
        init_producer_id_fn: Fakes.init_producer_id_ok(pid_id, epoch),
        script: [{:return, Fakes.ok_response(@topic, @partition)}]
      )

    assert :ok = ProducerWorker.enqueue(pid, "x", %{})
    assert_receive {:send_called, request, _selector}
    [%{data: [%{record_set: rb}]}] = request.topic_data
    assert rb.producer_id == pid_id
    assert rb.producer_epoch == epoch
    assert rb.base_sequence == 0
  end

  test "linger_ms batches enqueues into one request" do
    {pid, _} =
      start_worker(
        linger_ms: 50,
        script: [{:return, Fakes.ok_response(@topic, @partition)}]
      )

    callers =
      for i <- 1..10 do
        Task.async(fn -> ProducerWorker.enqueue(pid, "msg-#{i}", %{}) end)
      end

    Task.await_many(callers, 2_000)

    assert_received {:send_called, request, _}
    refute_received {:send_called, _, _}
    [%{data: [%{record_set: rb}]}] = request.topic_data
    assert length(rb.records) == 10
  end

  test "retriable error is retried with same base_sequence under idempotence" do
    {pid, _} =
      start_worker(
        retry_backoff_ms: 1,
        script: [
          {:return, Fakes.err_response(@topic, @partition, 6)},
          {:return, Fakes.ok_response(@topic, @partition)}
        ]
      )

    attach(:produce_retry)
    assert :ok = ProducerWorker.enqueue(pid, "hello", %{})

    assert_received {:send_called, req1, _}
    assert_received {:send_called, req2, _}
    [%{data: [%{record_set: rb1}]}] = req1.topic_data
    [%{data: [%{record_set: rb2}]}] = req2.topic_data
    assert rb1.base_sequence == rb2.base_sequence

    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :retried], _,
                    %{reason: :not_leader_for_partition}}
  end

  test "permanent error returns {:error, {:permanent, _}}" do
    {pid, _} =
      start_worker(script: [{:return, Fakes.err_response(@topic, @partition, 18)}])

    attach(:produce_failed)
    assert {:error, {:permanent, :record_list_too_large}} = ProducerWorker.enqueue(pid, "x", %{})

    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :failed], _,
                    %{last_reason: :record_list_too_large}}
  end

  test "delivery_timeout exhaustion returns {:error, {:delivery_failed, _}}" do
    {pid, _} =
      Fakes.start_exhausting_worker()

    assert {:error, {:delivery_failed, :no_broker}} = ProducerWorker.enqueue(pid, "x", %{})
  end

  test "drop-oldest evicts head and admits new tail under saturation" do
    {pid, script_pid} =
      start_worker(
        queue_max: 2,
        queue_overflow: :drop_oldest,
        linger_ms: 1
      )

    Fakes.script_slow_first(script_pid)

    attach(:dropped)

    # Saturate: kick off the in-flight slow batch
    {first_caller, _} = spawn_monitor(fn -> ProducerWorker.enqueue(pid, "first", %{}) end)
    assert in_flight?(pid, 1)

    # Fill queue to capacity
    spawn(fn -> ProducerWorker.enqueue(pid, "filler-1", %{}) end)
    spawn(fn -> ProducerWorker.enqueue(pid, "filler-2", %{}) end)
    assert queued?(pid, 2)

    # This one will trigger drop_oldest
    spawn(fn -> ProducerWorker.enqueue(pid, "filler-3", %{}) end)

    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :overflowed], %{count: 1},
                    %{overflow_strategy: :drop_oldest}},
                   1000

    Process.exit(first_caller, :kill)
  end

  test "drop-newest rejects new and replies overflow" do
    {pid, script_pid} =
      start_worker(
        queue_max: 1,
        queue_overflow: :drop_newest,
        linger_ms: 1
      )

    Fakes.script_slow_first(script_pid)

    spawn(fn -> ProducerWorker.enqueue(pid, "first", %{}) end)
    assert in_flight?(pid, 1)

    spawn(fn -> ProducerWorker.enqueue(pid, "filler", %{}) end)
    assert queued?(pid, 1)

    assert {:error, {:queue_full, :overflow}} = ProducerWorker.enqueue(pid, "rejected", %{})
  end

  test "auto_create_topic on :unknown_topic_or_partition issues CreateTopics and retries" do
    create_called = self()

    create_fn = fn _worker, topic, _opts, _timeout ->
      send(create_called, {:create_called, topic})
      :ok
    end

    {pid, _} =
      start_worker(
        retry_backoff_ms: 1,
        create_topic_fn: create_fn,
        auto_create_topic: true,
        script: [
          {:return, Fakes.err_response(@topic, @partition, 3)},
          {:return, Fakes.ok_response(@topic, @partition)}
        ]
      )

    attach(:topic_created)
    assert :ok = ProducerWorker.enqueue(pid, "hi", %{})
    assert_receive {:create_called, @topic}
    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :topic_created], _, _}
  end

  test "auto_create_topic = false drops :unknown_topic_or_partition immediately" do
    {pid, _} =
      start_worker(
        auto_create_topic: false,
        script: [{:return, Fakes.err_response(@topic, @partition, 3)}]
      )

    assert {:error, {:delivery_failed, :unknown_topic_or_partition}} =
             ProducerWorker.enqueue(pid, "hi", %{})
  end

  test ":not_leader_for_partition triggers metadata refresh before retry" do
    refreshed_pid = self()

    refresh_fn = fn _worker, _timeout ->
      send(refreshed_pid, :refresh_called)
      :ok
    end

    {pid, _} =
      start_worker(
        retry_backoff_ms: 1,
        metadata_refresh_fn: refresh_fn,
        script: [
          {:return, Fakes.err_response(@topic, @partition, 6)},
          {:return, Fakes.ok_response(@topic, @partition)}
        ]
      )

    attach(:metadata_refresh)
    assert :ok = ProducerWorker.enqueue(pid, "x", %{})

    assert_receive :refresh_called

    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :metadata_refresh], _,
                    %{trigger: :error_recovery, error: nil}}
  end

  test "stats/1 reports counters and last_error" do
    {pid, _} =
      start_worker(
        script: [
          {:return, Fakes.err_response(@topic, @partition, 18)}
        ]
      )

    {:error, {:permanent, _}} = ProducerWorker.enqueue(pid, "x", %{})

    stats = ProducerWorker.stats(pid)
    assert stats.queue_depth == 0
    assert stats.in_flight == 0
    assert stats.last_error == :record_list_too_large
  end

  test "non-idempotent mode skips InitProducerId and uses producer_id = -1" do
    test_pid = self()

    init_fn = fn _w, _t ->
      send(test_pid, :init_called)
      {:ok, %{producer_id: 1, producer_epoch: 1}}
    end

    {pid, _} =
      start_worker(
        idempotent: false,
        init_producer_id_fn: init_fn,
        script: [{:return, Fakes.ok_response(@topic, @partition)}]
      )

    refute_received :init_called

    assert :ok = ProducerWorker.enqueue(pid, "x", %{})
    assert_received {:send_called, request, _}
    [%{data: [%{record_set: rb}]}] = request.topic_data
    assert rb.producer_id == -1
    assert rb.base_sequence == -1
  end

  test "InitProducerId failure falls back to non-idempotent mode" do
    {pid, _} =
      start_worker(
        idempotent: true,
        init_producer_id_fn: Fakes.init_producer_id_fail(:no_coordinator),
        script: [{:return, Fakes.ok_response(@topic, @partition)}]
      )

    assert :ok = ProducerWorker.enqueue(pid, "x", %{})
    assert_received {:send_called, request, _}
    [%{data: [%{record_set: rb}]}] = request.topic_data
    assert rb.producer_id == -1
    assert rb.base_sequence == -1
  end

  test "linger_ms = 0 disables batching — one Produce per enqueue" do
    {pid, _} =
      start_worker(
        linger_ms: 0,
        script: [
          {:return, Fakes.ok_response(@topic, @partition)},
          {:return, Fakes.ok_response(@topic, @partition)},
          {:return, Fakes.ok_response(@topic, @partition)}
        ]
      )

    assert :ok = ProducerWorker.enqueue(pid, "a", %{})
    assert :ok = ProducerWorker.enqueue(pid, "b", %{})
    assert :ok = ProducerWorker.enqueue(pid, "c", %{})

    # Three separate sends.
    assert_received {:send_called, _, _}
    assert_received {:send_called, _, _}
    assert_received {:send_called, _, _}
    refute_received {:send_called, _, _}
  end

  test ":no_broker triggers metadata refresh before retry" do
    refreshed = self()

    refresh_fn = fn _w, _t ->
      send(refreshed, :refresh_called)
      :ok
    end

    {pid, _} =
      start_worker(
        retry_backoff_ms: 1,
        metadata_refresh_fn: refresh_fn,
        script: [
          {:return, {:error, :no_broker}},
          {:return, Fakes.ok_response(@topic, @partition)}
        ]
      )

    attach(:metadata_refresh)
    assert :ok = ProducerWorker.enqueue(pid, "x", %{})

    assert_receive :refresh_called

    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :metadata_refresh], _,
                    %{trigger: :error_recovery}}
  end

  test "topic create failure fails the batch" do
    create_fn = fn _w, _t, _o, _to -> {:error, :invalid_replication_factor} end

    {pid, _} =
      start_worker(
        retry_backoff_ms: 1,
        create_topic_fn: create_fn,
        script: [{:return, Fakes.err_response(@topic, @partition, 3)}]
      )

    assert {:error, {:delivery_failed, :invalid_replication_factor}} =
             ProducerWorker.enqueue(pid, "hi", %{})
  end

  test "duplicate_sequence_number is treated as already delivered (no re-init, no resend)" do
    # error_code 46 = duplicate_sequence_number — the batch is genuinely stored.
    test_pid = self()

    {pid, _} =
      start_worker(
        idempotent: true,
        init_producer_id_fn: fn _w, _t ->
          send(test_pid, :init_called)
          {:ok, %{producer_id: 7777, producer_epoch: 0}}
        end,
        script: [{:return, Fakes.err_response(@topic, @partition, 46)}]
      )

    # The boot-time InitProducerId fires once; drain it before asserting no re-init.
    assert_receive :init_called

    attach(:produce_ok)
    assert :ok = ProducerWorker.enqueue(pid, "x", %{})
    assert_receive {:telemetry, [:flezha, :plugin, :kafka, :shipped], _, _}

    # Exactly one send (the original) — a duplicate must not re-init or resend.
    assert_received {:send_called, _, _}
    refute_received {:send_called, _, _}
    refute_received :init_called
  end

  for {label, code} <- [out_of_order_sequence_number: 45, invalid_producer_epoch: 47] do
    test "#{label} fence re-inits the producer session and resends without acking the fence" do
      test_pid = self()
      {:ok, epoch_counter} = Agent.start_link(fn -> 0 end)

      # Boot init returns epoch 0; the post-fence re-init returns epoch 1 so we
      # can prove the resend rode a fresh session.
      init_fn = fn _w, _t ->
        epoch = Agent.get_and_update(epoch_counter, fn n -> {n, n + 1} end)
        send(test_pid, {:init_called, epoch})
        {:ok, %{producer_id: 7777, producer_epoch: epoch}}
      end

      {pid, _} =
        start_worker(
          idempotent: true,
          retry_backoff_ms: 1,
          init_producer_id_fn: init_fn,
          script: [
            {:return, Fakes.err_response(@topic, @partition, unquote(code))},
            {:return, Fakes.ok_response(@topic, @partition)}
          ]
        )

      assert_receive {:init_called, 0}

      attach(:produce_ok)
      assert :ok = ProducerWorker.enqueue(pid, "hello", %{})

      # The fence drove a second InitProducerId (session re-init).
      assert_receive {:init_called, 1}

      # Two sends: the fenced original, then the resend on the fresh session
      # with sequence realigned to 0.
      assert_received {:send_called, req1, _}
      assert_received {:send_called, req2, _}
      [%{data: [%{record_set: rb1}]}] = req1.topic_data
      [%{data: [%{record_set: rb2}]}] = req2.topic_data
      assert rb1.producer_epoch == 0
      assert rb2.producer_epoch == 1
      assert rb2.base_sequence == 0

      # Delivered on the resend — never a fake ack on the fence itself.
      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :shipped], _, _}
    end
  end

  test "non-idempotent producer (the default) treats a transient hiccup as a plain retry" do
    test_pid = self()

    {pid, _} =
      start_worker(
        idempotent: false,
        retry_backoff_ms: 1,
        init_producer_id_fn: fn _w, _t ->
          send(test_pid, :init_called)
          {:ok, %{producer_id: 1, producer_epoch: 1}}
        end,
        script: [
          # 6 = not_leader_for_partition (transient), then ok.
          {:return, Fakes.err_response(@topic, @partition, 6)},
          {:return, Fakes.ok_response(@topic, @partition)}
        ]
      )

    refute_received :init_called

    assert :ok = ProducerWorker.enqueue(pid, "x", %{})

    assert_received {:send_called, req1, _}
    [%{data: [%{record_set: rb1}]}] = req1.topic_data
    assert rb1.producer_id == -1
    assert rb1.base_sequence == -1
  end

  test "a fence the worker cannot recover surfaces :produce_stalled health after exhaustion" do
    {pid, _} =
      start_worker(
        idempotent: true,
        retries: 1,
        retry_backoff_ms: 1,
        delivery_timeout_ms: 1_000,
        # Every send fences — each batch exhausts its retry and fails.
        script: List.duplicate({:return, Fakes.err_response(@topic, @partition, 45)}, 30)
      )

    attach(:produce_failed)

    # Each synchronous enqueue is its own batch; three failed batches drive
    # consecutive_failures past the health threshold.
    for _ <- 1..3 do
      assert {:error, {:delivery_failed, :out_of_order_sequence_number}} =
               ProducerWorker.enqueue(pid, "x", %{})
    end

    assert_received {:telemetry, [:flezha, :plugin, :kafka, :failed], _, _}

    # The producer now reports :produce_stalled on its health surface; the
    # in-process maybe_refresh_broker/2 path drives recovery on batch failure.
    stats = ProducerWorker.stats(pid)
    assert stats.consecutive_failures >= 3

    assert {:unhealthy, :produce_stalled} =
             Producer.health(%{owned_pid: pid})
  end

  test "max_batch_bytes triggers immediate flush even within linger window" do
    {pid, _} =
      start_worker(
        linger_ms: 5_000,
        max_batch_bytes: 30,
        script: [
          {:return, Fakes.ok_response(@topic, @partition)},
          {:return, Fakes.ok_response(@topic, @partition)}
        ]
      )

    # Enqueue messages totalling > 30 bytes — should flush mid-window without
    # waiting the full 5 seconds.
    Task.async(fn -> ProducerWorker.enqueue(pid, String.duplicate("a", 20), %{}) end)
    Task.async(fn -> ProducerWorker.enqueue(pid, String.duplicate("b", 20), %{}) end)

    assert_receive {:send_called, _, _}, 1_000
  end

  test "drop-nothing mode applies back-pressure with timeout" do
    # The dispatch is never answered — the batch is guaranteed to still be in
    # flight when the block-timeout fires for the queued enqueue.
    {pid, script_pid} =
      start_worker(
        queue_max: 1,
        queue_overflow: :drop_nothing,
        queue_block_timeout_ms: 50,
        linger_ms: 1
      )

    Agent.update(script_pid, fn _ ->
      List.duplicate({:return, :never}, 4)
    end)

    # Establish in-flight (first) and a queued message (filler).
    spawn(fn -> ProducerWorker.enqueue(pid, "first", %{}) end)
    assert in_flight?(pid, 1)
    spawn(fn -> ProducerWorker.enqueue(pid, "filler", %{}) end)
    assert queued?(pid, 1)

    # The queue is now full (filler) and in-flight (first) never completes;
    # this third enqueue must timeout via the block path.
    assert {:error, {:queue_full, :timeout}} =
             ProducerWorker.enqueue(pid, "blocked", %{}, timeout: 1_000)
  end

  test "an outcome for an unknown dispatch is ignored after its batch completes" do
    {pid, _} =
      start_worker(script: [{:return, Fakes.ok_response(@topic, @partition)}])

    assert :ok = ProducerWorker.enqueue(pid, "x", %{})

    # Inject an outcome for a dispatch the window never issued — a no-op.
    send(pid, {:kafka_ex_response, make_ref(), Fakes.ok_response(@topic, @partition)})

    # Worker still alive and responsive (the call queues behind the message).
    assert ProducerWorker.stats(pid).queue_depth == 0
  end
end
