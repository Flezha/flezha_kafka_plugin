defmodule FlezhaKafka.ConsumerTest do
  @moduledoc """
  The consumer without a broker: its options and refusals, a fetched record set
  through `Partition.handle_message_set/2` against a stand-in relay, and the
  verdicts its health draws from the member's facts.
  """

  # async: false: the primary Logger.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Flezha.Plugin.ProviderBehaviour.Options
  alias FlezhaKafka.Consumer
  alias FlezhaKafka.Consumer.Member
  alias FlezhaKafka.Consumer.Partition
  alias FlezhaKafka.Test.Helpers
  alias KafkaEx.Messages.Fetch.Record
  alias KafkaEx.Messages.Header

  @base %{hosts: "kafka1:9092,kafka2:9093", topics: "trades", group_id: "g", channel: "in"}

  defp prepare(opts) do
    host = %{
      overwatcher_pid: self(),
      source_namespace: :default,
      source_relay_id: "op",
      source_plugin_id: "kin",
      subscribed_extensions: []
    }

    with {:ok, normalised} <- Options.normalise_opts(Consumer, Map.merge(host, opts)),
         do: Consumer.init_provider(normalised)
  end

  describe "options" do
    test "a binding is prepared with its hosts parsed and its credential redacted" do
      assert {:ok, opts} =
               prepare(Map.put(@base, :auth, %{"username" => "u", "password" => "hunter2"}))

      assert opts.hosts == [{"kafka1", 9092}, {"kafka2", 9093}]
      assert opts.topics == ["trades"]
      assert opts.plugin_id == "kin"
      assert opts.start_at == "end"
      assert opts.password == "**redacted**"
      refute opts.auth.password

      # The secret reaches the client only through the member's closure.
      args = Member.args(opts)
      refute inspect(Map.delete(args, :connect)) =~ "hunter2"
      auth = Keyword.fetch!(args.connect.(), :auth)
      assert %KafkaEx.Auth.Config{password: "hunter2", mechanism: :plain} = auth
      refute inspect(auth) =~ "hunter2"
    end

    test "isolation_level reads uncommitted unless told, and takes only a level Kafka has" do
      assert {:ok, %{isolation_level: :read_uncommitted}} = prepare(@base)

      assert {:ok, opts} = prepare(Map.put(@base, :isolation_level, "read_committed"))
      assert Member.args(opts).isolation_level == :read_committed

      assert {:error, {:invalid_option_value, :isolation_level, "serializable"}} =
               prepare(Map.put(@base, :isolation_level, "serializable"))
    end

    test "a binding without a group is refused" do
      assert {:error, reason} = prepare(Map.delete(@base, :group_id))

      assert %Flezha.Error{code: "plugin.options_required"} =
               Flezha.Error.from_reason(reason, nil)
    end

    test "one name in two namespaces is two group members" do
      keys =
        for ns <- ["ns_a", "ns_b"] do
          {:ok, opts} = prepare(Map.put(@base, :source_namespace, ns))
          [{key, _args}] = Consumer.worker_specs(opts)
          key
        end

      assert keys == [{"ns_a", "kin"}, {"ns_b", "kin"}]
    end

    test "it feeds, is not shareable and declares its own verbs" do
      decl = Consumer.__provider__()
      assert {decl.name, decl.feeds, decl.shareable} == {"kafka_consumer", true, false}
      assert decl.telemetry == [rebalanced: :event, lag: :metric]

      assert Application.get_env(:flezha_kafka, :flezha_providers)["kafka_consumer"] ==
               Consumer
    end
  end

  describe "a record set" do
    setup do
      :ok = :logger.set_module_level(Partition, :all)
      on_exit(fn -> :logger.unset_module_level(Partition) end)
      :ok
    end

    test "every record becomes one input carrying where it came from" do
      records = [
        record(7, "v1", key: "EUR-USD", headers: [{"source", "feed"}, {"source", "late"}]),
        record(8, nil, key: <<255, 0>>, headers: [{"raw", <<200>>}])
      ]

      host = Helpers.stand_in_host(:host, [:ok, :ok])
      assert {:sync_commit, _state} = Partition.handle_message_set(records, state(host, true))

      assert_receive {:ingest, :host, "v1", first}
      assert_receive {:ingest, :host, "", second}

      assert first == %{
               "channel" => "in",
               "flz.msg_id" => "kin/trades/3/7",
               "kafka.topic" => "trades",
               "kafka.partition" => "3",
               "kafka.offset" => "7",
               "kafka.timestamp" => "1700000000000",
               "kafka.key" => "EUR-USD",
               "kafka.header.source" => "late"
             }

      assert second["kafka.key.b64"] == Base.encode64(<<255, 0>>)
      assert second["kafka.header.raw.b64"] == Base.encode64(<<200>>)
      refute Map.has_key?(second, "kafka.key")
      assert_receive :batch_taken
    end

    test "without durable input a taken set is left to the timer" do
      host = Helpers.stand_in_host(:host, [:ok])

      assert {:async_commit, _} =
               Partition.handle_message_set([record(1, "a")], state(host, false))

      assert_receive :batch_taken
    end

    # Fails when: an absorbed or suspended verdict reads as a refusal, which
    # would refetch a duplicate the relay already holds forever.
    test "a set answered absorbed and suspended is taken and committed" do
      host = Helpers.stand_in_host(:host, [{:ok, :absorbed}, {:ok, :suspended}])
      records = [record(1, "a"), record(2, "b")]

      assert {:sync_commit, _} = Partition.handle_message_set(records, state(host, true))
      assert_receive :batch_taken
    end

    test "a refused set exits uncommitted and tells the member" do
      host = Helpers.stand_in_host(:host, [:ok, {:error, :parked}])

      log =
        capture_log(fn ->
          assert {:shutdown, {:ingest_refused, {:error, :parked}}} =
                   catch_exit(
                     Partition.handle_message_set(
                       [record(4, "a"), record(5, "b")],
                       state(host, true)
                     )
                   )
        end)

      assert log =~
               "topic=trades partition=3 offset=4 records=2 ingest refused: {:error, :parked}"

      assert_receive {:batch_refused, "ingest refused: {:error, :parked}"}
      refute_received :batch_taken
    end
  end

  describe "health" do
    test "refusals reach the threshold" do
      opts = %{failure_threshold: 3, idle_after_ms: 1_000, lag_interval_ms: 5_000}
      now = :erlang.monotonic_time(:millisecond)

      assert health(opts, %{consecutive_failures: 2, last_error: "x", last_settled_at: now}) ==
               :healthy

      assert health(opts, %{consecutive_failures: 3, last_error: "x", last_settled_at: now}) ==
               {:unhealthy, {:read_failed, "x"}}
    end

    test "what relay info shows is text the CLI can print" do
      opts = %{
        failure_threshold: 3,
        idle_after_ms: 0,
        lag_interval_ms: 5_000,
        group_id: "g",
        topics: ["trades"]
      }

      facts = %{
        consecutive_failures: 0,
        last_error: nil,
        last_settled_at: :erlang.monotonic_time(:millisecond),
        assignment: [{"trades", 0}, {"trades", 3}],
        lag: %{{"trades", 0} => 5, {"trades", 3} => 0}
      }

      member = spawn_link(fn -> answer_probes(facts) end)

      assert %{
               group_id: "g",
               topics: ["trades"],
               health: :healthy,
               assignment: ["trades/0", "trades/3"],
               lag: %{"trades/0" => 5, "trades/3" => 0}
             } = Consumer.inspect_state(Map.put(opts, :owned_pid, member))
    end

    test "no member, and a dead one, are named" do
      opts = %{failure_threshold: 3, idle_after_ms: 0, lag_interval_ms: 5_000}
      assert Consumer.health(opts) == {:unhealthy, :no_member}
      dead = spawn(fn -> :ok end)
      ref = Process.monitor(dead)
      assert_receive {:DOWN, ^ref, _, _, _}
      assert Consumer.health(Map.put(opts, :owned_pid, dead)) == {:unhealthy, :member_dead}
    end
  end

  # A member that answers its probe with `facts`.
  defp health(opts, facts) do
    member =
      spawn_link(fn ->
        receive do
          {:health_probe, from, ref} -> send(from, {ref, facts})
        end
      end)

    Consumer.health(Map.put(opts, :owned_pid, member))
  end

  defp answer_probes(facts) do
    receive do
      {:health_probe, from, ref} -> send(from, {ref, facts})
    end

    answer_probes(facts)
  end

  defp state(host, durable) do
    %{
      host: host,
      channel: "in",
      plugin_id: "kin",
      durable: durable,
      member: self(),
      topic: "trades",
      partition: 3
    }
  end

  defp record(offset, value, opts \\ []) do
    %Record{
      offset: offset,
      value: value,
      key: opts[:key],
      timestamp: 1_700_000_000_000,
      headers: for({k, v} <- opts[:headers] || [], do: %Header{key: k, value: v})
    }
  end
end
