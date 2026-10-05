defmodule FlezhaKafka.ProducerSupReapTest do
  @moduledoc """
  `WorkerSup.ensure_worker/5` dead and parked-pid reaping, in the Kafka
  producer's own family. A recovered relay must start a fresh, healthy
  producer rather than re-attach to a worker that cannot drain.

  The rule the `reusable?` argument carries is the spec's "a provider worker
  handed to a caller SHALL have passed the reusability check", and it is pinned
  here because this family is the one that uses it.
  """
  # async: false: registered singletons.
  use ExUnit.Case, async: false

  # Reaping is what these cases drive, and the family's supervisor reports
  # every abnormal child exit at error level.
  @moduletag :capture_log

  alias FlezhaKafka.Producer
  alias FlezhaKafka.ProducerWorker
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions
  alias FlezhaKafka.Test.Helpers

  @family WorkerSup.family(Producer)

  defp ensure(opts) do
    WorkerSup.ensure_worker(
      @family,
      key(opts),
      {ProducerWorker, opts},
      &ProducerWorker.reusable?/1,
      :permanent
    )
  end

  defp key(opts), do: {opts.plugin_id, opts.topic, opts.partition}
  defp stop(plugin_id), do: WorkerSup.stop_worker(@family, {plugin_id, "t", 0})

  # A stub supervised by the producer dyn-sup and registered under the worker's
  # via-name; it answers :stats with a permanently-parked verdict so
  # ensure_worker treats it as non-reusable.
  defmodule ParkedStub do
    use GenServer

    def start_link(opts) do
      key = {opts.plugin_id, opts.topic, opts.partition}

      GenServer.start_link(__MODULE__, opts, name: WorkerSup.via(WorkerSup.family(Producer), key))
    end

    def child_spec(opts) do
      %{
        id: {__MODULE__, make_ref()},
        start: {__MODULE__, :start_link, [opts]},
        restart: :temporary
      }
    end

    @impl true
    def init(_opts), do: {:ok, %{}}

    @impl true
    def handle_call(:stats, _from, state) do
      {:reply, %{last_error: {:swap_exhausted, :test}, queue_depth: 0, in_flight: 0}, state}
    end
  end

  defp forward_sleeps(pid) do
    receive do
      {:trace, _, :call, {Process, :sleep, [ms]}} -> send(pid, {:slept, ms})
      _other -> :ok
    end

    forward_sleeps(pid)
  end

  defp fake_worker do
    pid = spawn(&Helpers.park/0)
    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
    pid
  end

  # idempotent: false so the fresh ProducerWorker skips InitProducerId against
  # the fake (non-Kafka) worker pid and starts cleanly with no broker.
  # The worker spec the provider itself declares, so this test starts exactly
  # what the table would. `idempotent: false` keeps the fresh worker from
  # sending InitProducerId to the fake (non-Kafka) pid it is given.
  defp opts(plugin_id) do
    {:ok, normalised} =
      ProviderOptions.normalise_opts(Producer, %{
        hosts: [{"localhost", 9092}],
        topic: "t",
        partition: 0,
        plugin_id: plugin_id,
        idempotent: false
      })

    [{_key, spec}] = Producer.worker_specs(normalised)
    Map.put(spec, :worker_pid, fake_worker())
  end

  test "reuses a healthy worker" do
    o = opts("ps_reuse")
    {:ok, p1} = ensure(o)
    on_exit(fn -> stop("ps_reuse") end)

    {:ok, p2} = ensure(o)
    assert p1 == p2
    assert Process.alive?(p1)
  end

  test "reaps a parked worker and starts a fresh one" do
    o = opts("ps_parked")

    {:ok, parked} =
      DynamicSupervisor.start_child(WorkerSup.dyn_sup_name(@family), {ParkedStub, o})

    on_exit(fn -> stop("ps_parked") end)

    {:ok, fresh} = ensure(o)

    assert fresh != parked
    refute Process.alive?(parked)
    assert Process.alive?(fresh)
  end

  test "a registry entry still naming the dead worker does not delay the swap" do
    o = opts("ps_stale")
    key = {"ps_stale", "t", 0}
    on_exit(fn -> stop("ps_stale") end)

    {:ok, dead} = DynamicSupervisor.start_child(WorkerSup.dyn_sup_name(@family), {ParkedStub, o})

    # The family's Registry partition clears an entry from the `:EXIT` it is
    # linked by; a suspended process processes none, so the entry provably
    # still names the worker after it dies. Without this the partition has
    # usually cleared it before the swap looks, and a busy-wait that never
    # spins is a busy-wait no test can see.
    partition = Process.whereis(Module.concat(WorkerSup.registry_name(@family), "PIDPartition0"))
    :sys.suspend(partition)
    on_exit(fn -> :sys.resume(partition) end)

    ref = Process.monitor(dead)
    Process.exit(dead, :kill)
    assert_receive {:DOWN, ^ref, :process, ^dead, _}, 1_000
    assert [{^dead, _}] = Registry.lookup(WorkerSup.registry_name(@family), key)

    # A process cannot be its own call tracer, so the sleeps are collected by a
    # forwarder that dies with the test.
    test_pid = self()
    tracer = spawn_link(fn -> forward_sleeps(test_pid) end)
    :erlang.trace_pattern({Process, :sleep, 1}, true, [:local])
    :erlang.trace(self(), true, [:call, {:tracer, tracer}])
    on_exit(fn -> :erlang.trace_pattern({Process, :sleep, 1}, false, [:local]) end)

    {elapsed_us, {:ok, fresh}} = :timer.tc(fn -> ensure(o) end)
    :erlang.trace(self(), false, [:call])

    refute_receive {:slept, _}, 100
    assert elapsed_us < 50_000, "the swap took #{elapsed_us} us"

    assert fresh != dead
    assert Process.alive?(fresh)
    assert [{^fresh, _}] = Registry.lookup(WorkerSup.registry_name(@family), key)
  end

  test "a worker the reuse policy rejected is never handed back" do
    o = opts("ps_foreign")
    key = {"ps_foreign", "t", 0}

    # Registered under the family's key by a process the family's
    # DynamicSupervisor did not start, so the reap's `terminate_child/2` finds
    # no such child and the parked worker stays alive and registered.
    {:ok, parked} = ParkedStub.start_link(o)
    on_exit(fn -> if Process.alive?(parked), do: Process.exit(parked, :kill) end)

    assert {:error, :not_found} =
             DynamicSupervisor.terminate_child(WorkerSup.dyn_sup_name(@family), parked)

    refute ProducerWorker.reusable?(parked)
    assert [{^parked, _}] = Registry.lookup(WorkerSup.registry_name(@family), key)

    refute match?({:ok, ^parked}, ensure(o))
  end

  test "a forced worker death is answered with a fresh, registered worker" do
    o = opts("ps_killed")
    key = {"ps_killed", "t", 0}
    on_exit(fn -> stop("ps_killed") end)

    fresh_pids =
      for _ <- 1..10 do
        {:ok, victim} =
          DynamicSupervisor.start_child(WorkerSup.dyn_sup_name(@family), {ParkedStub, o})

        ref = Process.monitor(victim)
        Process.exit(victim, :kill)
        assert_receive {:DOWN, ^ref, :process, ^victim, _}, 1_000

        {:ok, fresh} = ensure(o)
        assert fresh != victim
        assert Process.alive?(fresh)
        assert [{^fresh, _}] = Registry.lookup(WorkerSup.registry_name(@family), key)
        _ = stop("ps_killed")
        fresh
      end

    assert length(Enum.uniq(fresh_pids)) == 10
  end
end
