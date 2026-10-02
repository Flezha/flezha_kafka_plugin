defmodule FlezhaKafka.PartitioningTest do
  @moduledoc """
  Which partition a record goes to: the Java client's choice for a keyed record,
  the sticky partition for the rest, a fixed `partition` for all, and the lanes
  the one worker per topic keeps for them.
  """

  use ExUnit.Case, async: true

  import Bitwise

  alias FlezhaKafka.ProducerWorker
  alias FlezhaKafka.Test.Fakes
  alias KafkaEx.Producer.Partitioner

  @topic Fakes.topic()

  # `org.apache.kafka.common.utils.UtilsTest#testMurmur2`: the keys and the
  # signed 32-bit hashes the Java client computes for them.
  @java_vectors [
    {"21", -973_932_308},
    {"foobar", -790_332_482},
    {"a-little-bit-long-string", -985_981_536},
    {"a-little-bit-longer-string", -1_486_304_829},
    {"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58_897_971},
    {"abc", 479_470_107}
  ]

  # A worker that chooses partitions over a topic of `count`, handing every
  # dispatch to the test. The count it reads is the agent's, which a test may
  # change.
  defp start_choosing_worker(count, opts \\ []) do
    {:ok, agent} = Agent.start_link(fn -> Keyword.get(opts, :count, {:ok, count}) end)
    count_fn = fn _worker, _topic, _timeout -> Agent.get(agent, & &1) end

    {pid, _script} =
      Fakes.start_worker(
        Keyword.merge(
          [
            linger_ms: Keyword.get(opts, :linger_ms, 0),
            idempotent: false,
            send_fn: Fakes.relay_send_fn(self()),
            extra: [
              partition: nil,
              partitioner: Keyword.get(opts, :partitioner, :murmur2),
              partition_count_fn: count_fn,
              retry_backoff_ms: 5,
              retry_max_backoff_ms: 5,
              auto_create_partitions: Keyword.get(opts, :auto_create_partitions, 1)
            ]
          ],
          Keyword.take(opts, [:queue_max, :queue_overflow])
        )
      )

    {pid, agent}
  end

  defp dispatched do
    assert_receive {:dispatched, ref, request}, 1_000
    [%{data: [%{partition: partition, record_set: batch}]}] = request.topic_data
    {ref, partition, Enum.map(batch.records, & &1.value)}
  end

  test "a keyed record goes to the partition the Java client chooses" do
    {pid, _agent} = start_choosing_worker(12)

    for {key, java} <- @java_vectors do
      :ok = ProducerWorker.cast_enqueue(pid, key, %{"flz.key" => key})
      {_ref, partition, [^key]} = dispatched()

      assert partition == rem(java &&& 0x7FFFFFFF, 12), "partition of #{inspect(key)}"
      assert partition == Partitioner.Default.assign_partition(@topic, key, nil, 12)
    end
  end

  test "fifty outputs of one key land on one partition in emit order" do
    {pid, _agent} = start_choosing_worker(12, linger_ms: 50)
    expected = for i <- 1..50, do: "m#{i}"

    for value <- expected,
        do: :ok = ProducerWorker.cast_enqueue(pid, value, %{"flz.key" => "EUR-USD"})

    {_ref, partition, values} = dispatched()
    assert partition == Partitioner.Default.assign_partition(@topic, "EUR-USD", nil, 12)
    assert values == expected
  end

  test "a fixed partition takes every record, whatever its key" do
    {pid, _script} =
      Fakes.start_worker(
        linger_ms: 0,
        idempotent: false,
        send_fn: Fakes.relay_send_fn(self()),
        extra: [partition: 3]
      )

    for key <- ["a", "b", "c", "d"] do
      :ok = ProducerWorker.cast_enqueue(pid, key, %{"flz.key" => key})
      assert {ref, 3, [^key]} = dispatched()
      Fakes.answer(pid, ref, Fakes.ok_response(@topic, 3))
    end

    assert Map.keys(ProducerWorker.stats(pid).partitions) == [3]
  end

  test "unkeyed records go one batch to a partition, the next batch to another" do
    {pid, _agent} = start_choosing_worker(4, partitioner: :sticky)

    partitions =
      for i <- 1..6 do
        value = "u#{i}"
        :ok = ProducerWorker.cast_enqueue(pid, value, %{})
        {ref, partition, [^value]} = dispatched()
        Fakes.answer(pid, ref, Fakes.ok_response(@topic, partition))
        partition
      end

    assert Enum.all?(partitions, &(&1 in 0..3))

    for [a, b] <- Enum.chunk_every(partitions, 2, 1, :discard),
        do: assert(a != b, "consecutive batches on #{a}: #{inspect(partitions)}")
  end

  test "records wait for the partition count, then join their lanes in arrival order" do
    {pid, agent} = start_choosing_worker(2, count: {:error, :no_broker})

    for i <- 1..3, do: :ok = ProducerWorker.cast_enqueue(pid, "w#{i}", %{"flz.key" => "k"})

    assert %{queue_depth: 3, partitions: lanes, last_error: {:partitions_unknown, :no_broker}} =
             ProducerWorker.stats(pid)

    assert lanes == %{}
    refute_received {:dispatched, _, _}

    Agent.update(agent, fn _ -> {:ok, 2} end)

    {_ref, partition, values} = dispatched()
    assert partition == Partitioner.Default.assign_partition(@topic, "k", nil, 2)
    assert values == ["w1", "w2", "w3"]
    assert ProducerWorker.stats(pid).consecutive_failures == 0
  end

  test "a topic the broker does not know counts the partitions it will be created with" do
    {pid, _agent} = start_choosing_worker(0, count: {:ok, :absent}, auto_create_partitions: 4)
    :ok = ProducerWorker.cast_enqueue(pid, "x", %{"flz.key" => "foobar"})

    assert {_ref, partition, ["x"]} = dispatched()
    assert partition == Partitioner.Default.assign_partition(@topic, "foobar", nil, 4)
    assert partition == 2
  end

  test "each partition keeps its own window and idempotent sequence" do
    {pid, _script} =
      Fakes.start_worker(
        linger_ms: 0,
        idempotent: true,
        max_in_flight: 5,
        send_fn: Fakes.relay_send_fn(self()),
        extra: [partition: nil, partition_count_fn: fn _w, _t, _o -> {:ok, 12} end]
      )

    # One at a time, so each record is a batch of its own.
    sequences =
      for key <- ["21", "foobar", "21", "foobar"] do
        :ok = ProducerWorker.cast_enqueue(pid, key, %{"flz.key" => key})
        assert_receive {:dispatched, _ref, request}, 1_000
        [%{data: [%{partition: p, record_set: batch}]}] = request.topic_data
        {p, batch.base_sequence}
      end

    p21 = Partitioner.Default.assign_partition(@topic, "21", nil, 12)
    pfoo = Partitioner.Default.assign_partition(@topic, "foobar", nil, 12)
    refute p21 == pfoo
    assert sequences == [{p21, 0}, {pfoo, 0}, {p21, 1}, {pfoo, 1}]

    assert ProducerWorker.stats(pid).partitions == %{
             p21 => %{queue_depth: 0, in_flight: 2},
             pfoo => %{queue_depth: 0, in_flight: 2}
           }
  end
end
