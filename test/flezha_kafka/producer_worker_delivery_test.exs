defmodule FlezhaKafka.ProducerWorkerDeliveryTest do
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Plugin.Provider.WorkerSup
  alias FlezhaKafka.Producer
  alias FlezhaKafka.ProducerWorker
  alias FlezhaKafka.Test.Fakes
  alias FlezhaKafka.WorkerRegistry
  alias FlezhaTest.Helper.ProviderPrepare
  # async: false: writes Application env; Mimic global mode; Mimic stubs.

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 3]
  import ExUnit.CaptureLog

  import FlezhaKafka.Test.Fakes,
    only: [start_worker: 1, attach: 1, in_flight?: 2, queued?: 2, window_records: 1]

  setup :set_mimic_global_until_exit

  @topic Fakes.topic()
  @partition Fakes.partition()

  describe "delivery-guaranteed output" do
    setup do
      rc = "kw_" <> Integer.to_string(System.unique_integer([:positive]))
      cell_id = {rc, "task", "k1"}
      :ok = ProviderPrepare.stub_outbox_owner(cell_id)
      %{rc: rc, cell_id: cell_id}
    end

    test "an acked batch confirms its outbox range", %{cell_id: cell_id} do
      {pid, _} = start_worker(script: [{:return, Fakes.ok_response(@topic, @partition)}])

      :ok =
        ProducerWorker.cast_enqueue(
          pid,
          "v",
          ProviderPrepare.guaranteed_metadata(cell_id, 7)
        )

      assert_receive {:send_called, _request, _ns}, 1_000
      assert :ok = ProviderPrepare.assert_confirmed([{:cursor, 7}], 1_000)
    end

    @tag :tmp_dir
    test "a permanently failed batch parks the record instead of dropping it", %{
      rc: rc,
      cell_id: cell_id,
      tmp_dir: dir
    } do
      original = Application.get_env(:flezha, :dlq, [])
      Application.put_env(:flezha, :dlq, Keyword.put(original, :dir, dir))
      on_exit(fn -> Application.put_env(:flezha, :dlq, original) end)
      start_supervised!(DlqStore)

      {pid, _} = start_worker(script: [{:return, Fakes.err_response(@topic, @partition, 18)}])

      # The park logs at :error by design. The capture has to span the enqueue as
      # well as the receive: the worker parks asynchronously, so a capture opened
      # only around the receive can miss the line it is meant to assert.
      log =
        capture_log(fn ->
          :ok =
            ProducerWorker.cast_enqueue(
              pid,
              "poison",
              ProviderPrepare.guaranteed_metadata(cell_id, 3)
            )

          # Parked, then the hold is answered so the cursor may pass the parked range.
          assert :ok = ProviderPrepare.assert_confirmed([{:cursor, 3}], 1_000)
        end)

      assert log =~ "parked undeliverable output"
      assert log =~ "record_list_too_large"

      assert [entry] = DlqStore.list(rc)
      assert entry["cell_key"] == "k1"
      assert entry["channel"] == "out"
      assert entry["error"] =~ "record_list_too_large"
    end

    test "with no configured overflow policy a guaranteed record waits instead of evicting", %{
      cell_id: cell_id
    } do
      {pid, script_pid} =
        start_worker(queue_max: 1, queue_overflow: :drop_nothing, linger_ms: 1)

      Fakes.script_slow_first(script_pid)

      attach(:dropped)

      for seq <- 0..2 do
        :ok =
          ProducerWorker.cast_enqueue(
            pid,
            "v#{seq}",
            ProviderPrepare.guaranteed_metadata(cell_id, seq)
          )
      end

      # Every record is delivered — under `:drop_oldest` the middle one would
      # have been evicted while its cursor claimed it delivered.
      assert :ok =
               ProviderPrepare.assert_confirmed(
                 Enum.map(0..2, &{:cursor, &1}),
                 2_000
               )

      refute_received {:telemetry, [:flezha, :plugin, :kafka, :overflowed], _, _}
    end

    test "an explicitly configured drop of a guaranteed record is alarm-grade", %{
      cell_id: cell_id
    } do
      {pid, script_pid} =
        start_worker(queue_max: 1, queue_overflow: :drop_oldest, linger_ms: 1)

      Fakes.script_slow_first(script_pid)

      attach(:dropped)

      log =
        capture_log(fn ->
          for seq <- 0..2 do
            :ok =
              ProducerWorker.cast_enqueue(
                pid,
                "v#{seq}",
                ProviderPrepare.guaranteed_metadata(cell_id, seq)
              )
          end

          assert_receive {:telemetry, [:flezha, :plugin, :kafka, :overflowed], %{count: 1},
                          %{overflow_strategy: :drop_oldest, guaranteed: true}},
                         1_000
        end)

      assert log =~ "dropped a delivery-guaranteed record"
    end

    @tag :tmp_dir
    test "swap exhaustion parks every guaranteed record, queued or waiting for room", %{
      rc: rc,
      cell_id: cell_id,
      tmp_dir: dir
    } do
      original = Application.get_env(:flezha, :dlq, [])
      Application.put_env(:flezha, :dlq, Keyword.put(original, :dir, dir))
      on_exit(fn -> Application.put_env(:flezha, :dlq, original) end)
      start_supervised!(DlqStore)

      producer = WorkerSup.worker(Producer, {"default", rc, @topic})

      {pid, script_pid} =
        start_worker(
          name: producer,
          linger_ms: 1,
          delivery_timeout_ms: 50,
          queue_max: 1,
          queue_overflow: :drop_nothing
        )

      Agent.update(script_pid, fn _ -> List.duplicate({:return, :never}, 5) end)

      dispatch =
        &ProducerWorker.cast_enqueue(
          producer,
          "v#{&1}",
          ProviderPrepare.guaranteed_metadata(cell_id, &1)
        )

      # 1 is dispatched and never answered, 2 fills the queue and 3, on its way,
      # is admitted past the bound and raises the full flag, so 4 waits for room.
      # The client's death rewinds 1 into the queue.
      :ok = dispatch.(1)
      assert in_flight?(pid, 1)
      :ok = dispatch.(2)
      :ok = dispatch.(3)
      assert queued?(pid, 2)
      waiting = Task.async(fn -> dispatch.(4) end)
      assert wait_until(fn -> ProducerWorker.stats(pid).waiting == 1 end, 1_000, 5)

      registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
      Fakes.seed_registry_ref(pid, registry_ref)

      :sys.replace_state(
        pid,
        &%{&1 | swap_started_at: System.monotonic_time(:millisecond) - 1_000}
      )

      Mimic.stub(WorkerRegistry, :swap, fn ^registry_ref -> {:error, :no_broker} end)

      log =
        capture_log(fn ->
          send(pid, {:kafka_worker_dead, registry_ref, :killed})

          assert :ok = ProviderPrepare.assert_confirmed(Enum.map(1..4, &{:cursor, &1}), 1_000)
        end)

      assert :ok = Task.await(waiting, 1_000)
      assert length(String.split(log, "parked undeliverable output")) == 5
      assert log =~ "worker_swap_exhausted"
      assert length(DlqStore.list(rc)) == 4
      assert %{last_error: {:swap_exhausted, :no_broker}, waiting: 0} = ProducerWorker.stats(pid)
    end
  end

  describe "in-flight window" do
    # One record per dispatch: the next cast only goes in once the previous
    # batch is on the wire, so the window fills a batch at a time.
    defp fill_window(pid, n) do
      for i <- 1..n do
        :ok = ProducerWorker.cast_enqueue(pid, "m#{i}", %{})
        assert_receive {:dispatched, ref, request}, 1_000
        {ref, base_sequence(request)}
      end
    end

    defp base_sequence(request) do
      [%{data: [%{record_set: record_set} | _]} | _] = request.topic_data
      record_set.base_sequence
    end

    defp producer_id(request) do
      [%{data: [%{record_set: record_set} | _]} | _] = request.topic_data
      record_set.producer_id
    end

    defp start_forward_worker(opts) do
      {pid, _script} =
        start_worker(
          opts ++ [send_fn: Fakes.forward_send_fn(self()), linger_ms: 0, max_in_flight: 5]
        )

      pid
    end

    test "five batches are outstanding at once with consecutive sequences" do
      attach(:produce_ok)
      pid = start_forward_worker([])

      window = fill_window(pid, 5)
      assert Enum.map(window, &elem(&1, 1)) == [0, 1, 2, 3, 4]
      assert window_records(:sys.get_state(pid)) == 5

      for {ref, _seq} <- window do
        Fakes.answer(pid, ref, Fakes.ok_response(@topic, @partition))
      end

      for _ <- 1..5 do
        assert_receive {:telemetry, [:flezha, :plugin, :kafka, :shipped], m, _}, 1_000
        assert m.batch_size == 1
      end

      assert ProducerWorker.stats(pid).in_flight == 0
    end

    test "without idempotence exactly one batch is outstanding" do
      pid = start_forward_worker(idempotent: false, max_in_flight: 1)

      :ok = ProducerWorker.cast_enqueue(pid, "a", %{})
      assert_receive {:dispatched, ref, request}, 1_000
      assert base_sequence(request) == -1

      :ok = ProducerWorker.cast_enqueue(pid, "b", %{})
      _ = :sys.get_state(pid)
      refute_received {:dispatched, _, _}

      Fakes.answer(pid, ref, Fakes.ok_response(@topic, @partition))
      assert_receive {:dispatched, _ref, _request}, 1_000
    end

    test "a head failure resends the whole tail behind it, in order" do
      pid = start_forward_worker([])

      [{ref1, 0}, {ref2, 1}, {ref3, 2}] = fill_window(pid, 3)

      Fakes.answer(pid, ref1, Fakes.err_response(@topic, @partition, 7))
      # k+1 and k+2 are still outstanding, so nothing goes back on the wire yet.
      _ = :sys.get_state(pid)
      refute_received {:dispatched, _, _}

      Fakes.answer(pid, ref2, Fakes.err_response(@topic, @partition, 45))
      Fakes.answer(pid, ref3, Fakes.err_response(@topic, @partition, 45))

      assert_receive {:dispatched, _, resent_1}, 1_000
      assert_receive {:dispatched, _, resent_2}, 1_000
      assert_receive {:dispatched, _, resent_3}, 1_000
      assert Enum.map([resent_1, resent_2, resent_3], &base_sequence/1) == [0, 1, 2]
    end

    test "a tail failure does not resend the head" do
      pid = start_forward_worker([])

      [{ref1, 0}, {ref2, 1}] = fill_window(pid, 2)

      Fakes.answer(pid, ref1, Fakes.ok_response(@topic, @partition))
      Fakes.answer(pid, ref2, Fakes.err_response(@topic, @partition, 7))

      assert_receive {:dispatched, _, resent}, 1_000
      assert base_sequence(resent) == 1
      _ = :sys.get_state(pid)
      refute_received {:dispatched, _, _}
    end

    test "a timed-out head waits for the tail's outcome, then both go out in order" do
      pid = start_forward_worker([])

      [{ref1, 0}, {ref2, 1}] = fill_window(pid, 2)

      send(pid, {:dispatch_timeout, @partition, ref1})
      _ = :sys.get_state(pid)
      refute_received {:dispatched, _, _}

      Fakes.answer(pid, ref2, Fakes.err_response(@topic, @partition, 45))

      assert_receive {:dispatched, _, resent_1}, 1_000
      assert_receive {:dispatched, _, resent_2}, 1_000
      assert Enum.map([resent_1, resent_2], &base_sequence/1) == [0, 1]
    end

    test "a fence on the oldest batch renumbers the whole window and resends it in order" do
      ids = :counters.new(1, [])

      init_producer_id_fn = fn _w, _t ->
        :counters.add(ids, 1, 1)
        {:ok, %{producer_id: 1000 + :counters.get(ids, 1), producer_epoch: 0}}
      end

      pid = start_forward_worker(init_producer_id_fn: init_producer_id_fn)

      [{ref1, 0}, {_ref2, 1}, {_ref3, 2}] = fill_window(pid, 3)

      Fakes.answer(pid, ref1, Fakes.err_response(@topic, @partition, 45))

      resent =
        for _ <- 1..3 do
          assert_receive {:dispatched, _, request}, 1_000
          request
        end

      assert Enum.map(resent, &base_sequence/1) == [0, 1, 2]
      assert Enum.map(resent, &producer_id/1) == [1002, 1002, 1002]
    end

    test "worker death rewinds the whole window to the head of the queue, oldest first" do
      attach(:worker_swapped)
      pid = start_forward_worker([])

      _window = fill_window(pid, 3)

      {registry_ref, _new_worker_pid} = Fakes.expect_worker_swap(pid)

      send(pid, {:kafka_worker_dead, registry_ref, :killed})

      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :worker_swapped], m, _}, 1_000
      assert m.in_flight_records == 3

      # The rewound records are back at the head of the queue in admission
      # order, and leave again under the new session.
      assert_receive {:dispatched, _, resent}, 1_000
      assert record_values(resent) == ["m1", "m2", "m3"]
      assert :sys.get_state(pid).queue.frames == 0
    end

    test "a completed batch confirms each of its records exactly once" do
      rc = "kw_" <> Integer.to_string(System.unique_integer([:positive]))
      cell_id = {rc, "task", "k1"}
      :ok = ProviderPrepare.stub_outbox_owner(cell_id)

      pid = start_forward_worker([])

      for seq <- 1..3 do
        :ok =
          ProducerWorker.cast_enqueue(
            pid,
            "v#{seq}",
            ProviderPrepare.guaranteed_metadata(cell_id, seq)
          )

        assert_receive {:dispatched, ref, _request}, 1_000
        Fakes.answer(pid, ref, Fakes.ok_response(@topic, @partition))
        assert :ok = ProviderPrepare.assert_confirmed([{:cursor, seq}], 1_000)
      end
    end

    defp record_values(request) do
      [%{data: [%{record_set: record_set} | _]} | _] = request.topic_data
      Enum.map(record_set.records, & &1.value)
    end
  end
end
