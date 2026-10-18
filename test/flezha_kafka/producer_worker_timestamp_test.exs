defmodule FlezhaKafka.ProducerWorkerTimestampTest do
  @moduledoc """
  Regression: Kafka record `timestamp` must be a wall-clock Unix-epoch
  millisecond, not BEAM monotonic time. Monotonic time is a large negative
  integer that decodes to ~1951 once Kafka stores it as CreateTime.
  """

  alias FlezhaKafka.ProducerWorker
  alias FlezhaKafka.Test.Fakes, as: Fakes
  alias FlezhaTest.Helper.NodeTestHelper

  # async: false: registered singletons.

  use ExUnit.Case, async: false
  use Mimic

  @topic "ts_topic"
  @partition 0

  defp start_worker do
    test_pid = self()

    {:ok, script_pid} =
      Agent.start_link(fn -> [{:return, Fakes.ok_response(@topic, @partition, 0)}] end)

    {:ok, pid} =
      GenServer.start_link(
        ProducerWorker,
        Map.merge(Fakes.worker_spec("ts-test-#{System.unique_integer([:positive])}"), %{
          worker_pid: spawn(&NodeTestHelper.park/0),
          topic: @topic,
          partition: @partition,
          linger_ms: 5,
          queue_max: 100,
          queue_overflow: :drop_oldest,
          retries: 0,
          idempotent: true,
          auto_create_topic: false,
          send_fn: Fakes.script_send_fn(test_pid, script_pid),
          init_producer_id_fn: Fakes.init_producer_id_ok(1, 0),
          metadata_refresh_fn: Fakes.metadata_refresh_ok(),
          create_topic_fn: Fakes.create_topic_ok(),
          name: nil
        })
      )

    pid
  end

  test "record timestamp on the wire is a wall-clock Unix-epoch millisecond" do
    pid = start_worker()
    before_ms = System.system_time(:millisecond)

    assert :ok = ProducerWorker.enqueue(pid, "hello", %{})

    assert_receive {:send_called, request, _selector}, 1_000
    [%{data: [%{record_set: rb}]}] = request.topic_data
    [record] = rb.records

    after_ms = System.system_time(:millisecond)

    assert record.timestamp >= before_ms - 60_000,
           "record timestamp #{record.timestamp} is far below current wall clock " <>
             "#{before_ms} — looks like BEAM monotonic time, not Unix-epoch ms"

    assert record.timestamp <= after_ms + 60_000,
           "record timestamp #{record.timestamp} is far above current wall clock " <>
             "#{after_ms} — not a sane Unix-epoch ms"
  end
end
