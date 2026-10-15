defmodule FlezhaKafka.ConsumerIntegrationTest do
  @moduledoc """
  The consumer against the dev broker (`docker/dev/docker-compose.yml`, the host
  listener `localhost:19092`). Tagged `:kafka_integration`, run with:

      KAFKA_INTEGRATION=1 mix test --include kafka_integration test/flezha_kafka/consumer_integration_test.exs
  """

  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias FlezhaKafka.Consumer
  alias FlezhaKafka.Consumer.Member
  alias FlezhaKafka.Consumer.Partition
  alias FlezhaKafka.Test.Helpers
  alias KafkaEx.API
  alias KafkaEx.Client.NodeSelector
  alias KafkaEx.Consumer.ConsumerGroup

  @moduletag :kafka_integration

  @hosts [
    {System.get_env("KAFKA_BROKER_HOST", "localhost"),
     String.to_integer(System.get_env("KAFKA_BROKER_PORT", "19092"))}
  ]
  @api_versions %{fetch: 4, offset_fetch: 1, offset_commit: 1}
  # The record-batch attribute bit of a transaction's records.
  @transactional 0x10
  @compose_file Path.join(Mix.Project.deps_paths()[:flezha], "docker/dev/docker-compose.yml")

  # A GenConsumer that hands every message set to the test.
  defmodule Forward do
    use KafkaEx.Consumer.GenConsumer

    @impl true
    def init(topic, partition, %{test: test}), do: {:ok, %{test: test, tp: {topic, partition}}}

    @impl true
    def handle_message_set(records, state) do
      send(state.test, {:records, state.tp, self(), records})
      {:sync_commit, state}
    end
  end

  setup_all do
    if System.get_env("KAFKA_INTEGRATION") != "1",
      do: raise("Refusing to run kafka_integration tests without KAFKA_INTEGRATION=1")

    :ok
  end

  # A client of each test's own: one that lived through a broker pause may not
  # see a topic created after it.
  setup do
    {:ok, client} =
      KafkaEx.Client.start_link([uris: @hosts, consumer_group: :no_consumer_group], :no_name)

    Process.unlink(client)
    on_exit(fn -> Process.exit(client, :shutdown) end)
    %{client: client}
  end

  describe "the fork's group consumer" do
    test "a header arrives and a sync commit moves the committed offset", %{client: client} do
      topic = new_topic(client, 1)
      group = "proof-#{uid()}"

      {:ok, _} =
        API.produce(
          client,
          topic,
          0,
          [%{key: "EUR-USD", value: "v", headers: [{"source", "feed"}]}],
          api_version: 3
        )

      group_pid = start_group!(group, [topic])

      assert_receive {:records, {^topic, 0}, _pid, [record]}, 30_000
      assert record.key == "EUR-USD"
      assert [%KafkaEx.Messages.Header{key: "source", value: "feed"}] = record.headers

      assert wait_committed(client, group, topic, 0, 1)
      Supervisor.stop(group_pid)
    end

    test "two members split a 4-partition topic", %{client: client} do
      topic = new_topic(client, 4)
      group = "proof-#{uid()}"
      a = start_group!(group, [topic])
      b = start_group!(group, [topic])

      assert wait(fn ->
               as = assignments(a)
               bs = assignments(b)

               is_list(as) and is_list(bs) and as != [] and bs != [] and
                 Enum.sort(as ++ bs) == for(p <- 0..3, do: {topic, p})
             end)

      Supervisor.stop(a)
      Supervisor.stop(b)
    end
  end

  describe "kafka_consumer" do
    setup do
      :ok = :logger.set_module_level([Member, Partition], :all)
      on_exit(fn -> :logger.unset_module_level([Member, Partition]) end)
      :ok
    end

    test "records arrive in order with their key and headers, and durable input commits each batch",
         %{client: client} do
      topic = new_topic(client, 1)

      {:ok, _} =
        API.produce(
          client,
          topic,
          0,
          [%{key: "EUR-USD", value: "0", headers: [{"source", "feed"}]}],
          api_version: 3
        )

      {:ok, _} = API.produce(client, topic, 0, for(i <- 1..99, do: %{value: "#{i}"}))
      {opts, _member} = start_consumer(topic, %{durable_input: true})

      [{"0", first} | rest] = take(100)
      assert Enum.map(rest, &elem(&1, 0)) == Enum.map(1..99, &to_string/1)

      assert %{
               "kafka.key" => "EUR-USD",
               "kafka.header.source" => "feed",
               "kafka.topic" => ^topic,
               "kafka.partition" => "0",
               "kafka.offset" => "0"
             } = first

      assert first["flz.msg_id"] == "#{opts.plugin_id}/#{topic}/0/0"
      assert wait_committed(client, opts.group_id, topic, 0, 100)
    end

    test "two members of one group split a 6-partition topic, each record to one of them",
         %{client: client} do
      topic = new_topic(client, 6)
      group = "split-#{uid()}"

      {a_opts, a} =
        start_consumer(topic, %{group_id: group, overwatcher_pid: Helpers.stand_in_host(:a)})

      {b_opts, b} =
        start_consumer(topic, %{group_id: group, overwatcher_pid: Helpers.stand_in_host(:b)})

      assert wait(fn ->
               with {:ok, %{assignment: [_ | _] = as}} <- Member.probe(a),
                    {:ok, %{assignment: [_ | _] = bs}} <- Member.probe(b) do
                 length(as) + length(bs) == 6
               end
             end)

      for p <- 0..5,
          do: {:ok, _} = API.produce(client, topic, p, for(i <- 0..99, do: %{value: "#{p}-#{i}"}))

      seen = collect_tagged(600)
      assert length(seen) == 600
      assert seen |> Enum.map(&elem(&1, 1)) |> Enum.uniq() |> length() == 600
      assert seen |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> Enum.sort() == [:a, :b]
      refute_receive {:ingest, _, _, _}, 1_000
      for opts <- [a_opts, b_opts], do: assert(opts.group_id == group)
    end

    test "without durable input the offsets wait for the timer", %{client: client} do
      topic = new_topic(client, 1)
      # One record batch per produce call: a fetch returns whole batches.
      for b <- 0..49,
          do: {:ok, _} = API.produce(client, topic, 0, for(i <- 1..10, do: %{value: "#{b}-#{i}"}))

      {opts, member} =
        start_consumer(topic, %{durable_input: false, commit_interval_ms: 5_000, max_bytes: 512})

      _ = take(1)
      deadline = System.monotonic_time(:millisecond) + 2_000

      samples =
        Stream.repeatedly(fn ->
          taken = drain()
          Process.sleep(100)
          {taken, committed_offset(client, opts.group_id, topic)}
        end)
        |> Enum.take_while(fn _ -> System.monotonic_time(:millisecond) < deadline end)

      # Many batches were taken in the window, and the offset moved once at most.
      assert samples |> Enum.map(&elem(&1, 0)) |> Enum.sum() >= 100
      assert samples |> Enum.map(&elem(&1, 1)) |> Enum.dedup() |> length() <= 2
      # Taken to the end before the stop, so no batch waits on this test as it exits.
      _ = take_any(500, 1_000)
      stop_member(member)
    end

    test "a refused batch commits nothing and is fetched again", %{client: client} do
      topic = new_topic(client, 1)
      {:ok, _} = API.produce(client, topic, 0, for(i <- 1..5, do: %{value: "#{i}"}))

      log =
        capture_log(fn ->
          {opts, _member} = start_consumer(topic, %{durable_input: true})
          assert [{"1", _}] = take(1, {:error, :parked})
          # The four after it may already have been sent in the same window.
          _ = take_any(4)
          assert committed_offset(client, opts.group_id, topic) == -1
          assert Enum.map(take(5), &elem(&1, 0)) == ~w(1 2 3 4 5)
          assert wait_committed(client, opts.group_id, topic, 0, 5)
        end)

      assert log =~ "partition=0 offset=0 records=5 ingest refused: {:error, :parked}"
    end

    test "lag rises while the relay does not take a batch", %{client: client} do
      topic = new_topic(client, 1)
      {:ok, _} = API.produce(client, topic, 0, for(i <- 1..1_000, do: %{value: "#{i}"}))
      lag_event = [:flezha, :plugin, :kafka_consumer, :lag]
      FlezhaKafka.Test.Helpers.capture(lag_event)

      capture_log(fn ->
        {_opts, member} = start_consumer(topic, %{overwatcher_pid: spawn_link(&Helpers.park/0)})

        assert_receive {:telemetry, ^lag_event, %{lag: lag}, %{topic: ^topic, partition: 0}},
                       30_000

        assert lag >= 900
        assert {:ok, %{lag: %{{^topic, 0} => _}}} = Member.probe(member)
        # Its consumer stops once the batch it waits on ends at the deadline.
        stop_member(member)
      end)
      |> then(&assert(&1 =~ "records=1000 ingest refused: {:error, {:unreachable, :timeout}}"))
    end

    test "read_committed hands over a committed transaction and leaves out an aborted one",
         %{client: client} do
      topic = new_topic(client, 1)
      transaction!(client, topic, ["aborted-1", "aborted-2"], false)
      transaction!(client, topic, ["committed"], true)

      start_consumer(topic, %{isolation_level: "read_committed"})
      # Written first, an aborted record or marker handed over would come first.
      assert [{"committed", _}] = take(1)
    end

    test "a 10 s broker pause reads as idle, then healthy, and loses nothing", %{client: client} do
      topic = new_topic(client, 1)
      {:ok, _} = API.produce(client, topic, 0, for(i <- 1..50, do: %{value: "#{i}"}))

      log =
        capture_log(fn ->
          {opts, member} =
            start_consumer(topic, %{
              durable_input: true,
              idle_after_ms: 5_000,
              lag_interval_ms: 500
            })

          opts = Map.put(opts, :owned_pid, member)
          assert length(take(50)) == 50
          assert wait(fn -> Consumer.health(opts) == :healthy end)

          pause!()
          assert wait(fn -> match?({:unhealthy, {:poll_idle, _}}, Consumer.health(opts)) end)
          Process.sleep(10_000 - 5_000)
          unpause!()

          {:ok, _} = API.produce(client, topic, 0, for(i <- 51..100, do: %{value: "#{i}"}))
          values = Enum.map(take_any(50, 60_000), &elem(&1, 0))
          assert Enum.uniq(values) -- Enum.map(51..100, &to_string/1) == []
          assert wait(fn -> Consumer.health(opts) == :healthy end)
          # Stopped here, so what the group logs as it leaves is captured too.
          stop_member(member)
        end)

      # The fork's calls to its own client time out while the broker is away.
      assert log =~ "(EXIT) time out"
    end
  end

  defp start_consumer(topic, extra) do
    plugin_id = "kc-#{uid()}"

    opts =
      Map.merge(
        %{
          hosts: @hosts,
          topics: topic,
          group_id: "g-#{plugin_id}",
          channel: "in",
          plugin_id: plugin_id,
          start_at: "beginning",
          max_wait_ms: 200,
          lag_interval_ms: 500
        },
        extra
      )

    {:ok, prepared, member} = Helpers.prepare_and_activate(Consumer, opts)
    on_exit(fn -> stop_member(member) end)
    {prepared, member}
  end

  # The member and the group it stops on its own, so what the group logs as it
  # leaves lands inside the caller's capture.
  defp stop_member(member) do
    with %{group: group} when is_pid(group) <- member_state(member) do
      ref = Process.monitor(group)
      Helpers.ensure_stopped(member)
      assert_receive {:DOWN, ^ref, _, _, _}, 10_000
    else
      _stopped_or_joining -> Helpers.ensure_stopped(member)
    end
  end

  defp member_state(member) do
    :sys.get_state(member)
  catch
    :exit, _stopped -> nil
  end

  # Answer the next `n` ingests with `verdict`, in order.
  defp take(n, verdict \\ :ok), do: for(_ <- 1..n, do: answer(verdict, 30_000))

  # Up to `n` more ingests, answered `:ok`, as many as arrive in time.
  defp take_any(n, timeout \\ 1_000) do
    Enum.reduce_while(1..n, [], fn _, acc ->
      receive do
        {:"$gen_call", from, {:ingest, body, md}} ->
          GenServer.reply(from, :ok)
          {:cont, [{body, md} | acc]}
      after
        timeout -> {:halt, acc}
      end
    end)
    |> Enum.reverse()
  end

  defp answer(verdict, timeout) do
    receive do
      {:"$gen_call", from, {:ingest, body, md}} ->
        GenServer.reply(from, verdict)
        {body, md}
    after
      timeout -> flunk("no ingest within #{timeout} ms")
    end
  end

  defp drain(), do: length(take_any(1_000, 0))

  defp collect_tagged(n),
    do:
      for(
        _ <- 1..n,
        do:
          receive do
            {:ingest, tag, body, _metadata} -> {tag, body}
          after
            30_000 -> flunk("fewer than #{n} records")
          end
      )

  # One transaction of a producer of its own writing `values` to partition 0,
  # then committed or aborted. The client library has no transactional
  # producer, so its requests are sent as they are.
  defp transaction!(client, topic, values, commit?) do
    id = "txn-#{uid()}"

    init = %Kayrock.InitProducerId.V0.Request{
      transactional_id: id,
      transaction_timeout_ms: 60_000
    }

    # Asking for the coordinator makes the broker create its transaction log.
    assert wait(fn -> match?({:ok, _}, API.find_coordinator(client, id, coordinator_type: 1)) end)
    assert wait(fn -> match?({:ok, %{error_code: 0}}, send_txn(client, init)) end)
    {:ok, %{producer_id: producer, producer_epoch: epoch}} = send_txn(client, init)

    txn = %{transactional_id: id, producer_id: producer, producer_epoch: epoch}

    added =
      struct(
        Kayrock.AddPartitionsToTxn.V0.Request,
        Map.put(txn, :topics, [%{topic: topic, partitions: [0]}])
      )

    assert {:ok, %{errors: [%{partition_errors: [%{error_code: 0}]}]}} = send_txn(client, added)

    batch = %Kayrock.RecordBatch{
      attributes: @transactional,
      producer_id: producer,
      producer_epoch: epoch,
      base_sequence: 0,
      records:
        for(
          v <- values,
          do: %Kayrock.RecordBatch.Record{value: v, timestamp: System.os_time(:millisecond)}
        )
    }

    produce = %Kayrock.Produce.V3.Request{
      transactional_id: id,
      acks: -1,
      timeout: 5_000,
      topic_data: [%{topic: topic, data: [%{partition: 0, record_set: batch}]}]
    }

    assert {:ok, %{responses: [%{partition_responses: [%{error_code: 0}]}]}} =
             send_txn(client, produce)

    ended = struct(Kayrock.EndTxn.V0.Request, Map.put(txn, :transaction_result, commit?))
    assert {:ok, %{error_code: 0}} = send_txn(client, ended)
  end

  defp send_txn(client, request),
    do: KafkaEx.Client.send_request(client, request, NodeSelector.first_available(), 10_000)

  defp committed_offset(client, group, topic) do
    {:ok, [%{partition_offsets: [%{offset: offset}]}]} =
      API.fetch_committed_offset(client, group, topic, [%{partition_num: 0}], api_version: 1)

    offset
  end

  defp pause!() do
    {_, 0} = System.cmd("docker", ["compose", "-f", @compose_file, "pause", "kafka"])
    on_exit(&unpause!/0)
  end

  defp unpause!(), do: System.cmd("docker", ["compose", "-f", @compose_file, "unpause", "kafka"])

  defp start_group!(group, topics) do
    {:ok, pid} =
      ConsumerGroup.start_link(Forward, group, topics,
        uris: @hosts,
        api_versions: @api_versions,
        auto_offset_reset: :earliest,
        heartbeat_interval: 500,
        session_timeout: 6_000,
        # A join is held for the rebalance, which must end within the client's
        # `sync_timeout`.
        rebalance_timeout: 4_000,
        fetch_options: [max_wait_time: 500],
        extra_consumer_args: %{test: self()}
      )

    Process.unlink(pid)
    pid
  end

  defp new_topic(client, partitions) do
    topic = "flz_cons_#{uid()}"

    {:ok, _} =
      API.create_topic(client, topic, num_partitions: partitions, replication_factor: 1)

    assert wait(fn -> match?({:ok, _}, API.latest_offset(client, topic, partitions - 1)) end)
    topic
  end

  defp wait_committed(client, group, topic, partition, offset) do
    wait(fn ->
      match?(
        {:ok, [%{partition_offsets: [%{offset: ^offset}]}]},
        API.fetch_committed_offset(client, group, topic, [%{partition_num: partition}],
          api_version: 1
        )
      )
    end)
  end

  # A manager answers nothing while it joins.
  defp assignments(group) do
    ConsumerGroup.assignments(group, 1_000)
  catch
    :exit, _busy -> nil
  end

  # Topics and groups outlive a run, so a name is unique across runs too.
  defp uid(), do: "#{System.os_time(:millisecond)}_#{System.unique_integer([:positive])}"

  defp wait(fun), do: Helpers.wait_until(fun, 30_000, 100)
end
