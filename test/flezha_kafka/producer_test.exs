defmodule FlezhaKafka.ProducerTest do
  @moduledoc """
  Facade-level tests for `FlezhaKafka.Producer`.

  Exercises the full path: `init_provider/1` → ensures a `ProducerWorker`
  is running, `on_input/2` / `on_output/2` → enqueues and gets back
  `{:ok, ctx}` once the broker round-trip completes.

  Per-component coverage (idempotence, retry, batching, drop semantics,
  auto-create, metadata refresh, telemetry, registry) lives in the more
  focused files beside this one.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions
  alias FlezhaKafka.Producer
  alias FlezhaKafka.ProducerWorker
  alias FlezhaKafka.WorkerRegistry
  alias FlezhaTest.Helper.NodeTestHelper

  # async: false: Mimic global mode; Mimic stubs; the primary Logger.

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  # Some cases start a real ProducerWorker with no broker reachable, so KafkaEx
  # logs an :nxdomain connect error + crash report by design; capture so a green
  # run stays quiet (logs still print on failure).
  @moduletag :capture_log

  @topic "flezha_test_topic"
  @broker {"kafka", 9092}
  # overflow-naming-record:begin
  @retired_policy "block"
  # overflow-naming-record:end

  setup :set_mimic_global_until_exit

  setup do
    test_pid = self()

    fake_worker = fn ->
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)
    end

    stub(WorkerRegistry, :acquire, fn _identity ->
      pid = fake_worker.()
      send(test_pid, {:registry_acquired, pid})
      {:ok, pid, make_ref()}
    end)

    stub(WorkerRegistry, :release, fn _ref -> :ok end)

    :ok
  end

  # Preparation as the extension table performs it: the declaration normalises
  # the options, then the provider interprets them. Nothing is started.
  defp prepare(opts) do
    with {:ok, normalised} <- ProviderOptions.normalise_opts(Producer, opts),
         do: Producer.init_provider(normalised)
  end

  # Activation as the table performs it: the declared worker, under the
  # provider's own family, keyed as `worker_specs/1` says.
  defp activate(opts) do
    family = WorkerSup.family(Producer)
    [{key, spec}] = Producer.worker_specs(opts)

    WorkerSup.ensure_worker(
      family,
      key,
      {ProducerWorker, spec},
      &ProducerWorker.reusable?/1,
      :permanent
    )
  end

  defp prepare_and_activate(opts) do
    with {:ok, prepared} <- prepare(opts),
         {:ok, pid} <- activate(prepared),
         do: {:ok, prepared, pid}
  end

  # The one worker a plugin has per topic, in a test that names no namespace.
  defp key(plugin_id), do: {nil, plugin_id, @topic}

  defp registered(plugin_id) do
    Registry.lookup(WorkerSup.registry_name(WorkerSup.family(Producer)), key(plugin_id))
  end

  # `subscribed_extensions` is the host's, and the producer is a chain plugin:
  # every test here drives `on_input`, and a `channel` option on a plugin
  # subscribed to nothing is refused because nothing would read it.
  defp build_opts(plugin_id, partition, extras \\ %{}) do
    Map.merge(
      %{
        hosts: [@broker],
        topic: @topic,
        partition: partition,
        plugin_id: plugin_id,
        idempotent: false,
        subscribed_extensions: [:on_input]
      },
      extras
    )
  end

  test "init_provider starts a producer worker and returns its pid" do
    plugin_id = "init-test-#{System.unique_integer([:positive])}"

    {:ok, opts, producer_pid} =
      prepare_and_activate(build_opts(plugin_id, 0))

    assert is_pid(producer_pid)
    assert Process.alive?(producer_pid)
    # The pid is the table's, not an option: the worker is addressed by the
    # name it is registered under.
    refute Map.has_key?(opts, :producer_pid)

    assert [{^producer_pid, _}] =
             Registry.lookup(
               WorkerSup.registry_name(WorkerSup.family(Producer)),
               key(plugin_id)
             )

    assert opts[:plugin_id] == plugin_id

    [{registered_pid, _}] =
      Registry.lookup(
        WorkerSup.registry_name(WorkerSup.family(Producer)),
        key(plugin_id)
      )

    assert registered_pid == producer_pid

    WorkerSup.stop_worker(WorkerSup.family(Producer), key(plugin_id))
  end

  test "stats/1 returns producer counters keyed on plugin_id" do
    plugin_id = "stats-test-#{System.unique_integer([:positive])}"

    {:ok, _opts, _pid} = prepare_and_activate(build_opts(plugin_id, 3))

    assert {:ok, stats} = Producer.stats(plugin_id)
    assert stats.queue_depth == 0
    assert stats.topic == @topic
    assert Map.keys(stats.partitions) == [3]

    WorkerSup.stop_worker(WorkerSup.family(Producer), key(plugin_id))

    # The Registry clears the entry from the worker's exit, which lands after
    # `stop_worker/2` returns; settle it so this asserts the no-entry answer
    # rather than whichever answer won the race.
    assert NodeTestHelper.wait_until(fn -> registered(plugin_id) == [] end, 1_000)
    assert {:error, :no_producer} = Producer.stats(plugin_id)
  end

  test "stats/1 refuses a registry entry the stopped worker has not yet cleared" do
    plugin_id = "stale-stats-#{System.unique_integer([:positive])}"
    family = WorkerSup.family(Producer)

    {:ok, _opts, producer_pid} = prepare_and_activate(build_opts(plugin_id, 0))

    # The Registry partition clears an entry from the exit it is linked by, and
    # a suspended process handles none: the entry provably still names the
    # worker after it is stopped, which is the window this reads in. A `:noproc`
    # never surfaces as an exit here because `ProducerWorker.stats/1` declares
    # the empty map as its refusal.
    partition =
      Process.whereis(Module.concat(WorkerSup.registry_name(family), "PIDPartition0"))

    :sys.suspend(partition)
    on_exit(fn -> :sys.resume(partition) end)

    ref = Process.monitor(producer_pid)
    :ok = WorkerSup.stop_worker(family, key(plugin_id))
    assert_receive {:DOWN, ^ref, :process, ^producer_pid, _}, 1_000
    assert [{^producer_pid, _}] = registered(plugin_id)

    assert {:error, :no_producer} = Producer.stats(plugin_id)
  end

  test "channel filter passes through when metadata does not match" do
    plugin_id = "group-test-#{System.unique_integer([:positive])}"

    {:ok, opts, _pid} =
      prepare_and_activate(build_opts(plugin_id, 0, %{channel: "2"}))

    ctx = %Ctx.IO{
      relay_id: "p",
      namespace: "n",
      message: "trade_payload",
      metadata: %{"channel" => "4"}
    }

    assert {:ok, ^ctx} = Producer.on_input(ctx, opts)

    WorkerSup.stop_worker(WorkerSup.family(Producer), key(plugin_id))
  end

  test "channel filter passes through when metadata is missing" do
    plugin_id = "group-test-#{System.unique_integer([:positive])}"

    {:ok, opts, _pid} =
      prepare_and_activate(build_opts(plugin_id, 0, %{channel: "2"}))

    ctx = %Ctx.IO{
      relay_id: "p",
      namespace: "n",
      message: "untagged",
      metadata: %{}
    }

    assert {:ok, ^ctx} = Producer.on_input(ctx, opts)

    WorkerSup.stop_worker(WorkerSup.family(Producer), key(plugin_id))
  end

  test "a window wider than one without idempotence is refused at preparation" do
    plugin_id = "no-idem-#{System.unique_integer([:positive])}"

    assert {:error, {:window_needs_idempotence, 3}} =
             prepare(build_opts(plugin_id, 0, %{idempotent: false, max_in_flight: 3}))

    assert Registry.lookup(
             WorkerSup.registry_name(WorkerSup.family(Producer)),
             key(plugin_id)
           ) == []
  end

  describe ":queue_overflow validation" do
    test "a value that is not a policy is refused at preparation" do
      for value <- [:drop_odlest, "drop_odlest", "DROP_OLDEST", 5] do
        plugin_id = "overflow-bad-#{System.unique_integer([:positive])}"

        assert {:error, {:invalid_option_value, :queue_overflow, ^value}} =
                 prepare(build_opts(plugin_id, 0, %{queue_overflow: value})),
               "expected #{inspect(value)} to be refused"

        assert Registry.lookup(
                 WorkerSup.registry_name(WorkerSup.family(Producer)),
                 key(plugin_id)
               ) == []
      end
    end

    test "the retired policy spelling is refused on the enable path too" do
      plugin_id = "overflow-retired-#{System.unique_integer([:positive])}"

      assert {:error, {:invalid_option_value, :queue_overflow, @retired_policy}} =
               prepare(build_opts(plugin_id, 0, %{queue_overflow: @retired_policy}))

      assert Registry.lookup(
               WorkerSup.registry_name(WorkerSup.family(Producer)),
               key(plugin_id)
             ) == []
    end

    test "every policy is accepted, as an atom and as its CLI string" do
      cases = [
        {:drop_nothing, :drop_nothing},
        {:drop_oldest, :drop_oldest},
        {:drop_newest, :drop_newest},
        {"drop_nothing", :drop_nothing},
        {"drop_oldest", :drop_oldest},
        {"drop_newest", :drop_newest},
        {nil, :drop_nothing}
      ]

      for {value, expected} <- cases do
        plugin_id = "overflow-ok-#{System.unique_integer([:positive])}"

        assert {:ok, _opts, pid} =
                 prepare_and_activate(build_opts(plugin_id, 0, %{queue_overflow: value})),
               "expected #{inspect(value)} to be accepted"

        assert :sys.get_state(pid).queue_overflow == expected
        WorkerSup.stop_worker(WorkerSup.family(Producer), key(plugin_id))
      end
    end
  end

  describe ":auth validation" do
    test "a mechanism the broker client does not support is refused at preparation" do
      for value <- ["plian", :plian, "PLAIN", 5] do
        plugin_id = "auth-mech-#{System.unique_integer([:positive])}"

        assert {:error, {:invalid_option_value, "auth mechanism", ^value}} =
                 prepare(
                   build_opts(plugin_id, 0, %{
                     auth: %{"mechanism" => value, "username" => "u", "password" => "p"}
                   })
                 ),
               "expected mechanism #{inspect(value)} to be refused"

        assert Registry.lookup(
                 WorkerSup.registry_name(WorkerSup.family(Producer)),
                 key(plugin_id)
               ) == []
      end
    end

    test "an auth key the broker client does not accept is refused, not dropped" do
      for key <- ["user", "usernmae", "timeout"] do
        plugin_id = "auth-key-#{System.unique_integer([:positive])}"

        assert {:error, {:invalid_option_value, "auth key", ^key}} =
                 prepare(
                   build_opts(plugin_id, 0, %{
                     auth: %{"username" => "u", "password" => "p", key => "x"}
                   })
                 ),
               "expected auth key #{key} to be refused"

        assert Registry.lookup(
                 WorkerSup.registry_name(WorkerSup.family(Producer)),
                 key(plugin_id)
               ) == []
      end
    end

    test "every supported mechanism is accepted, as an atom and as its string" do
      for {value, expected} <- [
            {:plain, :plain},
            {"plain", :plain},
            {:scram, :scram},
            {"scram", :scram}
          ] do
        plugin_id = "auth-ok-#{System.unique_integer([:positive])}"

        assert {:ok, opts, _pid} =
                 prepare_and_activate(
                   build_opts(plugin_id, 0, %{
                     auth: %{"mechanism" => value, "username" => "u", "password" => "hunter2"}
                   })
                 ),
               "expected mechanism #{inspect(value)} to be accepted"

        assert opts.auth[:mechanism] == expected
        assert opts.auth[:username] == "u"
        assert opts.auth[:password] == "**redacted**"
        WorkerSup.stop_worker(WorkerSup.family(Producer), key(plugin_id))
      end
    end

    test "an auth config supplied as the broker client's own struct is accepted" do
      plugin_id = "auth-struct-#{System.unique_integer([:positive])}"

      config = %KafkaEx.Auth.Config{mechanism: :plain, username: "u", password: "hunter2"}

      assert {:ok, _opts, _pid} =
               prepare_and_activate(build_opts(plugin_id, 0, %{auth: config}))

      WorkerSup.stop_worker(WorkerSup.family(Producer), key(plugin_id))
    end

    test "no auth at all is still the default" do
      plugin_id = "auth-none-#{System.unique_integer([:positive])}"

      assert {:ok, opts, _pid} = prepare_and_activate(build_opts(plugin_id, 0))
      assert opts.auth == nil

      WorkerSup.stop_worker(WorkerSup.family(Producer), key(plugin_id))
    end
  end

  describe "build/1 conversion" do
    test "parses integer params from binary CLI input" do
      assert {:ok, %Producer{queue_max: 20_000, retries: 7, linger_ms: 25}} =
               Producer.build(%{
                 "hosts" => "kafka:9092",
                 "topic" => "x",
                 "queue_max" => "20000",
                 "retries" => "7",
                 "linger_ms" => "25"
               })
    end

    test "parses max_in_flight" do
      assert {:ok, %Producer{max_in_flight: 5}} =
               Producer.build(%{
                 "hosts" => "kafka:9092",
                 "topic" => "x",
                 "max_in_flight" => "5"
               })
    end

    test "parses boolean params" do
      assert {:ok, %Producer{idempotent: false, auto_create_topic: true}} =
               Producer.build(%{
                 "hosts" => "kafka:9092",
                 "topic" => "x",
                 "idempotent" => "false",
                 "auto_create_topic" => "true"
               })
    end

    test "parses queue_overflow atom" do
      assert {:ok, %Producer{queue_overflow: :drop_newest}} =
               Producer.build(%{
                 "hosts" => "kafka:9092",
                 "topic" => "x",
                 "queue_overflow" => "drop_newest"
               })
    end

    test "queue_overflow accepts drop_nothing and refuses the retired spelling" do
      assert {:ok, %Producer{queue_overflow: :drop_nothing}} =
               Producer.build(%{
                 "hosts" => "kafka:9092",
                 "topic" => "x",
                 "queue_overflow" => "drop_nothing"
               })

      assert {:error, {:invalid_option_value, :queue_overflow, @retired_policy}} =
               Producer.build(%{
                 "hosts" => "kafka:9092",
                 "topic" => "x",
                 "queue_overflow" => @retired_policy
               })
    end

    test "default required_acks is -1 (acks=all) — BREAKING change from previous default of 1" do
      assert {:ok, %Producer{required_acks: -1}} =
               Producer.build(%{
                 "hosts" => "kafka:9092",
                 "topic" => "x"
               })
    end
  end

  describe "host parsing" do
    test "parses comma-separated string hosts" do
      plugin_id = "host-test-#{System.unique_integer([:positive])}"

      {:ok, opts, _pid} =
        prepare_and_activate(%{
          hosts: "kafka1:9092,kafka2:9093",
          topic: @topic,
          partition: 0,
          plugin_id: plugin_id,
          idempotent: false
        })

      assert opts[:hosts] == [{"kafka1", 9092}, {"kafka2", 9093}]
      WorkerSup.stop_worker(WorkerSup.family(Producer), key(plugin_id))
    end

    test "passes through list-form hosts" do
      plugin_id = "host-test-#{System.unique_integer([:positive])}"

      {:ok, opts, _pid} =
        prepare_and_activate(%{
          hosts: [{"k1", 9092}, {"k2", 9093}],
          topic: @topic,
          partition: 0,
          plugin_id: plugin_id,
          idempotent: false
        })

      assert opts[:hosts] == [{"k1", 9092}, {"k2", 9093}]
      WorkerSup.stop_worker(WorkerSup.family(Producer), key(plugin_id))
    end
  end

  describe "on_input / on_output enqueue path" do
    test "on_input forwards to producer worker and gets back :ok ctx" do
      # Drive the producer via the facade and rely on the worker to ack.
      # The worker_pid the registry hands out is a fake that never replies
      # to send_request — but we never enqueue because we only test the
      # channel passthrough branch in the facade-level tests above.
      # Here we exercise the produce attempt with an in-memory ProducerWorker
      # primed for success via send_fn.
      plugin_id = "io-test-#{System.unique_integer([:positive])}"

      {:ok, opts, producer_pid} =
        prepare_and_activate(%{
          hosts: [@broker],
          topic: @topic,
          partition: 0,
          plugin_id: plugin_id,
          idempotent: false
        })

      # Replace the producer worker's send_fn with one that returns OK.
      :sys.replace_state(producer_pid, fn state ->
        send_fn = fn _w, _r, _ns ->
          {:ok,
           %Kayrock.Produce.V5.Response{
             responses: [
               %{
                 topic: @topic,
                 cell_responses: [
                   %{
                     partition: 0,
                     error_code: 0,
                     base_offset: 0,
                     log_append_time: -1,
                     log_start_offset: -1
                   }
                 ]
               }
             ],
             throttle_time_ms: 0
           }}
        end

        %{state | send_fn: send_fn}
      end)

      ctx = %Ctx.IO{relay_id: "p", namespace: "n", message: "hello", metadata: %{}}
      assert {:ok, ^ctx} = Producer.on_input(ctx, opts)
      assert {:ok, ^ctx} = Producer.on_output(ctx, opts)

      WorkerSup.stop_worker(WorkerSup.family(Producer), key(plugin_id))
    end

    test "on_output returns {:ok, ctx} immediately even when broker would block 30 s" do
      plugin_id = "nonblock-test-#{System.unique_integer([:positive])}"

      {:ok, opts, producer_pid} =
        prepare_and_activate(%{
          hosts: [@broker],
          topic: @topic,
          partition: 0,
          plugin_id: plugin_id,
          idempotent: false
        })

      # Send never answers — a broker round-trip that outlives the test — so a
      # chain that waited on it would block for the worker's 30 s
      # `delivery_timeout_ms`. With cast_enqueue the chain returns while the
      # stub is still parked, which is what the report below witnesses.
      test_pid = self()

      :sys.replace_state(producer_pid, fn state ->
        %{
          state
          | send_fn: fn _w, _r, _ns ->
              send(test_pid, :broker_entered)
              NodeTestHelper.park()
              {:ok, make_ref()}
            end
        }
      end)

      ctx = %Ctx.IO{relay_id: "p", namespace: "n", message: "x", metadata: %{}}

      {elapsed_us, result} =
        :timer.tc(fn -> Producer.on_output(ctx, opts) end)

      assert {:ok, ^ctx} = result

      # The property, not a microsecond count: the broker WAS entered and never
      # answered, and the chain had already returned. The bound is sized against
      # the 30 s the return distinguishes itself from — a tighter one measures
      # scheduler jitter, not the enqueue.
      assert_receive :broker_entered, 5_000

      assert elapsed_us < 1_000_000,
             "the enqueue waited #{div(elapsed_us, 1_000)}ms on a broker that never answers"

      # Released, the send hands back a dispatch whose answer never comes, so
      # the worker takes its shutdown instead of being killed after 5 s.
      send(producer_pid, :unpark)
      WorkerSup.stop_worker(WorkerSup.family(Producer), key(plugin_id))
    end

    test "on_output surfaces permanent delivery failure via :failed telemetry, not return value" do
      plugin_id = "telemetry-test-#{System.unique_integer([:positive])}"

      {:ok, opts, producer_pid} =
        prepare_and_activate(%{
          hosts: [@broker],
          topic: @topic,
          partition: 0,
          plugin_id: plugin_id,
          idempotent: false,
          retries: 1,
          retry_backoff_ms: 1,
          retry_max_backoff_ms: 5,
          delivery_timeout_ms: 30,
          metadata_refresh_timeout_ms: 50
        })

      :sys.replace_state(producer_pid, fn state ->
        %{
          state
          | send_fn: fn _w, _r, _ns -> {:error, :network_exception} end
        }
      end)

      handler_id = "kafka-test-failed-#{System.unique_integer([:positive])}"
      test_pid = self()

      :ok =
        :telemetry.attach(
          handler_id,
          [:flezha, :plugin, :kafka, :failed],
          fn name, measurements, metadata, _ ->
            send(test_pid, {:telemetry, name, measurements, metadata})
          end,
          nil
        )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      ctx = %Ctx.IO{relay_id: "p", namespace: "n", message: "x", metadata: %{}}
      assert {:ok, ^ctx} = Producer.on_output(ctx, opts)

      assert_receive {:telemetry, [:flezha, :plugin, :kafka, :failed], _,
                      %{last_reason: :network_exception, caller: :async}},
                     2_000

      WorkerSup.stop_worker(WorkerSup.family(Producer), key(plugin_id))
    end
  end

  describe "registry release coupling" do
    test "stopping the producer worker releases the WorkerRegistry handle" do
      released = self()

      stub(WorkerRegistry, :acquire, fn _identity ->
        pid =
          spawn(fn ->
            receive do
              :stop -> :ok
            end
          end)

        {:ok, pid, {:fake_ref, System.unique_integer([:positive])}}
      end)

      stub(WorkerRegistry, :release, fn ref ->
        send(released, {:released, ref})
        :ok
      end)

      plugin_id = "release-test-#{System.unique_integer([:positive])}"

      {:ok, _opts, _pid} =
        prepare_and_activate(%{
          hosts: [@broker],
          topic: @topic,
          partition: 0,
          plugin_id: plugin_id,
          idempotent: false
        })

      WorkerSup.stop_worker(WorkerSup.family(Producer), key(plugin_id))

      assert_receive {:released, {:fake_ref, _}}, 1_000
    end
  end
end

defmodule FlezhaKafka.KafkaSaslRedactionTest do
  @moduledoc """
  finish-platform-authn-authz §7: Kafka SASL credentials ride a closure into
  the client start (mirroring the Postgres provider) — the live client
  authenticates with the real secret while the returned/persisted opts carry
  only a redacted placeholder.
  """
  # async: false: Mimic global mode; Mimic stubs; OS environment variables.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Plugin
  alias FlezhaKafka.Producer
  alias FlezhaKafka.ProducerWorker
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions
  alias FlezhaTest.Helper.NodeTestHelper

  setup :set_mimic_global_until_exit

  @secret "sup3r-s3cret-sasl-pw"

  # Preparation and activation as the extension table performs them.
  defp prepare_and_activate(opts) do
    with {:ok, normalised} <- ProviderOptions.normalise_opts(Producer, opts),
         {:ok, prepared} <- Producer.init_provider(normalised),
         [{key, spec}] = Producer.worker_specs(prepared),
         {:ok, pid} <-
           WorkerSup.ensure_worker(
             WorkerSup.family(Producer),
             key,
             {ProducerWorker, spec},
             &ProducerWorker.reusable?/1,
             :permanent
           ),
         do: {:ok, prepared, pid}
  end

  defp fake_client_stubs(test_pid) do
    stub(KafkaEx, :start_link_worker, fn :no_name, worker_opts ->
      client = spawn(&NodeTestHelper.park/0)
      send(test_pid, {:client_started, client, worker_opts})
      {:ok, client}
    end)

    stub(KafkaEx, :stop_worker, fn pid ->
      Process.exit(pid, :kill)
      :ok
    end)
  end

  test "the SASL secret reaches the client but not the returned opts" do
    fake_client_stubs(self())
    suffix = System.unique_integer([:positive])

    opts = %{
      hosts: "sasl-test-host-#{suffix}:9092",
      topic: "sasl_topic_#{suffix}",
      plugin_id: "sasl-test-#{suffix}",
      auth: %{"mechanism" => "plain", "username" => "svc", "password" => @secret}
    }

    {:ok, returned, producer_pid} = prepare_and_activate(opts)

    on_exit(fn ->
      _ =
        WorkerSup.stop_worker(
          WorkerSup.family(Producer),
          {"sasl-test-#{suffix}", "sasl_topic_#{suffix}", 0}
        )

      _ = producer_pid
    end)

    # The live client authenticated with the real secret...
    assert_receive {:client_started, _client, worker_opts}, 2_000
    assert %KafkaEx.Auth.Config{username: "svc", password: @secret} = worker_opts[:auth]

    # ...while the returned (and therefore persisted) opts are redacted. The
    # keys are the closed set `KafkaEx.Auth.Config` accepts, whichever spelling
    # the user wrote: validation normalises them at preparation.
    assert returned.auth[:password] == "**redacted**"
    assert returned.auth[:mechanism] == :plain
    # ...and nothing persisted carries it: the resolved credential is a
    # transient row, held in memory and dropped at the persistence boundary.
    persisted = Plugin.strip_transient(%Plugin{id: "p", provider: Producer, opts: returned})
    refute inspect(persisted, limit: :infinity) =~ @secret
  end

  test "sasl_password_env resolves the secret from the environment at client start" do
    fake_client_stubs(self())
    suffix = System.unique_integer([:positive])
    env_var = "FLZ_TEST_SASL_PW_#{suffix}"
    System.put_env(env_var, @secret)
    on_exit(fn -> System.delete_env(env_var) end)

    opts = %{
      hosts: "sasl-env-host-#{suffix}:9092",
      topic: "sasl_topic_#{suffix}",
      plugin_id: "sasl-env-#{suffix}",
      auth: %{"mechanism" => "plain", "username" => "svc", "password" => "placeholder"},
      sasl_password_env: env_var
    }

    {:ok, returned, _pid} = prepare_and_activate(opts)

    on_exit(fn ->
      _ =
        WorkerSup.stop_worker(
          WorkerSup.family(Producer),
          {"sasl-env-#{suffix}", "sasl_topic_#{suffix}", 0}
        )
    end)

    assert_receive {:client_started, _client, worker_opts}, 2_000
    assert %KafkaEx.Auth.Config{password: @secret} = worker_opts[:auth]

    persisted = Plugin.strip_transient(%Plugin{id: "p", provider: Producer, opts: returned})
    refute inspect(persisted, limit: :infinity) =~ @secret
  end
end
