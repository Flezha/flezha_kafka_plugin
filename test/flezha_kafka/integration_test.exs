defmodule FlezhaKafka.IntegrationTest do
  @moduledoc """
  End-to-end integration tests against a real Kafka in
  `docker/dev/docker-compose.yml`. Tagged `:kafka_integration` so the
  default `mix test` skips them — run with:

      KAFKA_INTEGRATION=1 mix test --include kafka_integration test/flezha_kafka/integration_test.exs

  Pre-reqs:
    1. `docker compose -f docker/dev/docker-compose.yml up -d kafka` in the core
    2. Kafka is reachable at `localhost:19092`, the listener the dev broker
       advertises to the host (`KAFKA_BROKER_HOST` and `KAFKA_BROKER_PORT` name
       another).

  Failure mode under chaos: the test pauses Kafka mid-stream via
  `docker compose pause kafka`, holds for ~10 s, unpauses, and asserts
  every produced message survives. The producer SHALL retry transparently
  during the pause and succeed after unpause.
  """

  alias FlezhaKafka.Producer
  alias FlezhaKafka.ProducerWorker
  alias Flezha.Plugin.Provider.WorkerSup
  alias FlezhaKafka.Test.Helpers
  alias Flezha.SDK.SafeCall

  # async: false: a shared Kafka broker in Docker.

  use ExUnit.Case, async: false

  @moduletag :kafka_integration

  @broker_host System.get_env("KAFKA_BROKER_HOST", "localhost")
  @broker_port String.to_integer(System.get_env("KAFKA_BROKER_PORT", "19092"))
  @compose_file System.get_env(
                  "KAFKA_COMPOSE_FILE",
                  Path.join(Mix.Project.deps_paths()[:flezha], "docker/dev/docker-compose.yml")
                )
  @compose_service System.get_env("KAFKA_COMPOSE_SERVICE", "kafka")

  # Run only when explicitly included: `mix test --include kafka_integration`
  # AND the env var is set, so accidental local invocation does not destroy
  # someone's running Kafka container.
  setup_all do
    if System.get_env("KAFKA_INTEGRATION") != "1" do
      raise "Refusing to run kafka_integration tests without KAFKA_INTEGRATION=1"
    end

    :ok
  end

  test "100 messages survive a 10-second broker pause" do
    plugin_id = "integ-#{System.unique_integer([:positive])}"
    topic = "flezha_integ_#{System.unique_integer([:positive])}"

    {:ok, _opts, pid} =
      Helpers.prepare_and_activate(Producer, %{
        hosts: [{@broker_host, @broker_port}],
        topic: topic,
        partition: 0,
        plugin_id: plugin_id,
        linger_ms: 5,
        retries: 100,
        retry_backoff_ms: 100,
        retry_max_backoff_ms: 1_000,
        delivery_timeout_ms: 60_000,
        idempotent: true,
        auto_create_topic: true,
        auto_create_partitions: 1,
        auto_create_replication_factor: 1
      })

    # Send 50 messages, pause Kafka, send 50 more during the pause,
    # then unpause and verify all 100 succeed.
    first_half = produce_in_parallel(pid, 1..50)

    {pause_out, 0} =
      System.cmd("docker", ["compose", "-f", @compose_file, "pause", @compose_service])

    # A failure past this point must not leave the shared broker frozen.
    on_exit(fn ->
      System.cmd("docker", ["compose", "-f", @compose_file, "unpause", @compose_service],
        stderr_to_stdout: true
      )
    end)

    IO.puts(pause_out)

    # `docker compose pause` returns once the container is frozen.
    second_half = produce_in_parallel(pid, 51..100)

    # The paused broker answers nothing: the worker has to have met that —
    # a failed attempt on the wire — before the broker comes back. While it
    # waits on its client, for up to the library's `sync_timeout`, its stats
    # answer comes that much late; an unanswered read is read again.
    stats_budget = Application.fetch_env!(:kafka_ex, :sync_timeout)

    assert Helpers.wait_until(
             fn ->
               stats = SafeCall.bounded(pid, :stats, stats_budget, on_error: %{}, log: :debug)
               Map.get(stats, :consecutive_failures, 0) >= 1 or Map.get(stats, :last_error) != nil
             end,
             30_000
           ),
           "the worker never observed the paused broker"

    {unpause_out, 0} =
      System.cmd("docker", ["compose", "-f", @compose_file, "unpause", @compose_service])

    IO.puts(unpause_out)

    results = Task.await_many(first_half ++ second_half, 60_000)

    failures = Enum.reject(results, &(&1 == :ok))

    assert failures == [],
           "expected all 100 messages to deliver, got failures: #{inspect(failures)}"

    stats = ProducerWorker.stats(pid)
    assert stats.dropped_total == 0
    assert stats.queue_depth == 0

    WorkerSup.stop_worker(WorkerSup.family(Producer), {:default, plugin_id, topic})
  end

  for codec <- [:gzip, :zstd] do
    test "a #{codec} batch is read back by a consumer, headers and all" do
      topic = "flezha_integ_#{unquote(codec)}_#{System.unique_integer([:positive])}"

      pid =
        start_producer(topic,
          compression: unquote(codec),
          partition: 0,
          headers: ~s({"source": "flezha"})
        )

      for i <- 1..100 do
        :ok =
          ProducerWorker.cast_enqueue(pid, "v#{i}", %{"flz.key" => "k#{i}", "flz.seq" => "#{i}.0"})
      end

      records = read_back(topic, 0, 100)
      assert Enum.map(records, & &1.value) == Enum.map(1..100, &"v#{&1}")

      assert Enum.map(hd(records).headers, &{&1.key, &1.value}) == [{"source", "flezha"}]
    end
  end

  test "keyed outputs land on the partitions the Java client chooses" do
    topic = "flezha_integ_keys_#{System.unique_integer([:positive])}"

    # Created first: the dev broker creates a topic a metadata read names with
    # its own default of one partition.
    {:ok, client} = KafkaEx.start_link_worker(:no_name, uris: [{@broker_host, @broker_port}])
    {:ok, _} = KafkaEx.API.create_topic(client, topic, num_partitions: 3, replication_factor: 1)

    pid = start_producer(topic, [])
    keys = for i <- 1..30, do: "key-#{i}"

    for key <- keys, do: :ok = ProducerWorker.cast_enqueue(pid, key, %{"flz.key" => key})

    by_partition =
      Enum.group_by(
        keys,
        &KafkaEx.Producer.Partitioner.Default.assign_partition(topic, &1, nil, 3)
      )

    for {partition, expected} <- by_partition do
      assert Enum.map(read_back(topic, partition, length(expected)), & &1.value) == expected
    end
  end

  defp start_producer(topic, extra) do
    plugin_id = "integ-#{System.unique_integer([:positive])}"

    {:ok, _opts, pid} =
      Helpers.prepare_and_activate(
        Producer,
        Map.merge(
          %{
            hosts: [{@broker_host, @broker_port}],
            topic: topic,
            plugin_id: plugin_id,
            linger_ms: 5,
            auto_create_topic: true,
            auto_create_replication_factor: 1
          },
          Map.new(extra)
        )
      )

    on_exit(fn ->
      WorkerSup.stop_worker(WorkerSup.family(Producer), {:default, plugin_id, topic})
    end)

    pid
  end

  # Every record of one partition, once `count` of them are there.
  defp read_back(topic, partition, count) do
    {:ok, client} = KafkaEx.start_link_worker(:no_name, uris: [{@broker_host, @broker_port}])

    assert Helpers.wait_until(
             fn ->
               match?(
                 {:ok, %{records: records}} when length(records) >= count,
                 KafkaEx.API.fetch_all(client, topic, partition, api_version: 4)
               )
             end,
             30_000,
             200
           ),
           "#{count} records never reached #{topic}/#{partition}"

    {:ok, %{records: records}} = KafkaEx.API.fetch_all(client, topic, partition, api_version: 4)
    records
  end

  defp produce_in_parallel(pid, range) do
    for i <- range do
      Task.async(fn ->
        ProducerWorker.enqueue(pid, "msg-#{i}", %{"kafka_msg_key" => "k-#{i}"}, timeout: 65_000)
      end)
    end
  end
end
