defmodule FlezhaKafka.LanesTest do
  @moduledoc """
  What one partition's trouble may and may not do to the worker's other
  partitions, and where a record goes when it has no key.
  """

  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureLog
  import FlezhaKafka.Test.Helpers, only: [set_mimic_global_until_exit: 1]

  alias FlezhaKafka.Producer
  alias FlezhaKafka.ProducerWorker
  alias FlezhaKafka.Test.Fakes
  alias FlezhaKafka.Test.Helpers
  alias FlezhaKafka.WorkerRegistry
  alias KafkaEx.Producer.Partitioner

  setup :set_mimic_global_until_exit

  # The suite logs at `:error`; the warnings these tests provoke are captured.
  setup do
    for module <- [FlezhaKafka.ProducerWorker, FlezhaKafka.ProducerWorker.Client],
        do: Logger.put_module_level(module, :warning)

    on_exit(fn ->
      for module <- [FlezhaKafka.ProducerWorker, FlezhaKafka.ProducerWorker.Client],
          do: Logger.delete_module_level(module)
    end)
  end

  @topic Fakes.topic()
  # Partitions 0 and 6 of 12.
  @key_a "21"
  @key_b "foobar"

  defp start_choosing_worker(opts) do
    {count_fn, opts} = Keyword.pop(opts, :count_fn, fn _w, _t, _o -> {:ok, 12} end)
    {extra, opts} = Keyword.pop(opts, :extra, [])

    {pid, _script} =
      Fakes.start_worker(
        Keyword.merge(
          [
            linger_ms: 0,
            max_in_flight: 5,
            send_fn: Fakes.relay_send_fn(self()),
            extra: [partition: nil, partition_count_fn: count_fn] ++ extra
          ],
          opts
        )
      )

    pid
  end

  defp dispatched do
    assert_receive {:dispatched, ref, request}, 1_000
    [%{data: [%{partition: partition}]}] = request.topic_data
    {ref, partition}
  end

  test "a forced fresh client resends every partition's window, not only the failing one's" do
    pid = start_choosing_worker(retries: 0)
    Fakes.seed_registry_ref(pid, {{[{"k", 9092}], 0, 0}, make_ref()})
    stub(WorkerRegistry, :force_replace, fn _identity -> {:ok, spawn(&Helpers.park/0)} end)

    delivered = Task.async(fn -> ProducerWorker.enqueue(pid, "a1", %{"flz.key" => @key_a}) end)
    {_ref_a, 0} = dispatched()

    :ok = ProducerWorker.cast_enqueue(pid, "b1", %{"flz.key" => @key_b})
    {ref_b, 6} = dispatched()

    log =
      capture_log(fn ->
        Fakes.answer(pid, ref_b, {:error, :no_broker})
        assert {resent, 0} = dispatched()
        Fakes.answer(pid, resent, Fakes.ok_response(@topic, 0))
      end)

    assert log =~ "forcing fresh broker connection"
    assert Task.await(delivered, 1_000) == :ok
  end

  test "one partition stalling makes the worker unhealthy while another delivers" do
    pid = start_choosing_worker(retries: 0, idempotent: false)

    for _round <- 1..3 do
      :ok = ProducerWorker.cast_enqueue(pid, "b", %{"flz.key" => @key_b})
      {ref_b, 6} = dispatched()
      Fakes.answer(pid, ref_b, {:error, :request_timed_out})

      :ok = ProducerWorker.cast_enqueue(pid, "a", %{"flz.key" => @key_a})
      {ref_a, 0} = dispatched()
      Fakes.answer(pid, ref_a, Fakes.ok_response(@topic, 0))
    end

    assert Helpers.wait_until(
             fn -> ProducerWorker.stats(pid).partitions[0].in_flight == 0 end,
             1_000
           )

    assert Producer.health(%{owned_pid: pid}) == {:unhealthy, :produce_stalled}
    assert %{consecutive_failures: 3, last_error: :request_timed_out} = ProducerWorker.stats(pid)
  end

  test "a producer session that failed to open is opened again at the next swap" do
    {:ok, attempts} = Agent.start_link(fn -> [{:error, :coordinator_load_in_progress}] end)

    init_fn = fn _worker, _timeout ->
      Agent.get_and_update(attempts, fn
        [first | rest] -> {first, rest}
        [] -> {{:ok, %{producer_id: 4242, producer_epoch: 0}}, []}
      end)
    end

    log =
      capture_log(fn ->
        {pid, _script} = Fakes.start_worker(init_producer_id_fn: init_fn, max_in_flight: 5)
        assert %{idempotent: false, max_in_flight: 1} = :sys.get_state(pid)

        {registry_ref, _new_worker} = Fakes.expect_worker_swap(pid)
        send(pid, {:kafka_worker_dead, registry_ref, :killed})

        assert Helpers.wait_until(fn -> :sys.get_state(pid).producer_id == 4242 end, 1_000)
        assert %{idempotent: true, max_in_flight: 5} = :sys.get_state(pid)
      end)

    assert log =~ "InitProducerId failed: :coordinator_load_in_progress"
  end

  test "records sent to partitions the created topic does not have go to ones it has" do
    {:ok, count} = Agent.start_link(fn -> {:ok, :absent} end)

    pid =
      start_choosing_worker(
        idempotent: false,
        count_fn: fn _w, _t, _o -> Agent.get(count, & &1) end,
        create_topic_fn: Fakes.create_topic_already_exists(),
        extra: [auto_create_partitions: 6]
      )

    delivered = Task.async(fn -> ProducerWorker.enqueue(pid, "x", %{"flz.key" => "abc"}) end)
    assert Partitioner.Default.assign_partition(@topic, "abc", nil, 6) == 3
    {ref, 3} = dispatched()

    # The broker made the topic itself, with one partition, before the worker could.
    Agent.update(count, fn _ -> {:ok, 1} end)
    Fakes.answer(pid, ref, Fakes.err_response(@topic, 3, 3))

    assert {ref, 0} = dispatched()
    Fakes.answer(pid, ref, Fakes.ok_response(@topic, 0))
    assert Task.await(delivered, 1_000) == :ok
  end

  test "a metadata refresh that finds fewer partitions sends the lane's records where they exist" do
    {:ok, count} = Agent.start_link(fn -> {:ok, :absent} end)

    pid =
      start_choosing_worker(
        idempotent: false,
        count_fn: fn _w, _t, _o -> Agent.get(count, & &1) end,
        extra: [auto_create_partitions: 6]
      )

    delivered = Task.async(fn -> ProducerWorker.enqueue(pid, "x", %{"flz.key" => "abc"}) end)
    {ref, 3} = dispatched()

    # A broker that creates a topic when a metadata read names it made one partition.
    Agent.update(count, fn _ -> {:ok, 1} end)
    Fakes.answer(pid, ref, {:error, :no_broker})

    assert {ref, 0} = dispatched()
    Fakes.answer(pid, ref, Fakes.ok_response(@topic, 0))
    assert Task.await(delivered, 1_000) == :ok
  end

  test "an unkeyed effectively-once record lands where its redrive lands" do
    pid = start_choosing_worker(idempotent: false)

    metadata = %{"flz.cell" => "orders/tally/k1", "flz.seq" => "9.0", "flz.epoch" => "1"}
    expected = Partitioner.Default.assign_partition(@topic, "orders/tally/k1/9.0/1", nil, 12)

    for _send_and_redrive <- 1..2 do
      :ok = ProducerWorker.cast_enqueue(pid, "v", metadata)
      assert {ref, ^expected} = dispatched()
      Fakes.answer(pid, ref, Fakes.ok_response(@topic, expected))
    end
  end
end
