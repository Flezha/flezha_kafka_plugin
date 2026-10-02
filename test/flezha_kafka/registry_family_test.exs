defmodule FlezhaKafka.RegistryFamilyTest do
  @moduledoc """
  The broker-client registry has no application to start it: the first acquire
  starts it under the producer's own family, where no plugin's teardown reaches
  it and its supervisor restarts it.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions
  alias FlezhaKafka.Producer
  alias FlezhaKafka.ProducerWorker
  alias FlezhaKafka.Test.Helpers
  alias FlezhaKafka.WorkerRegistry

  @family WorkerSup.family(Producer)

  defp registry_pid, do: GenServer.whereis(WorkerSup.via(@family, :registry))

  test "it survives a plugin's teardown and is restarted after a crash" do
    server = WorkerRegistry.ensure_started()
    assert server == WorkerSup.via(@family, :registry)
    registry = registry_pid()
    assert is_pid(registry)

    # A plugin's worker under the same family, stopped as a disable stops it.
    {:ok, normalised} =
      ProviderOptions.normalise_opts(Producer, %{
        hosts: "localhost:19092",
        topic: "t",
        partition: 0,
        idempotent: false,
        source_namespace: "default",
        source_plugin_id: "gone"
      })

    {:ok, prepared} = Producer.init_provider(normalised)
    [{key, spec}] = Producer.worker_specs(prepared)
    spec = Map.put(spec, :worker_pid, spawn(&Helpers.park/0))

    {:ok, _worker} =
      WorkerSup.ensure_worker(@family, key, {ProducerWorker, spec}, &Process.alive?/1, :permanent)

    :ok = WorkerSup.stop_worker(@family, key)
    assert registry_pid() == registry

    ref = Process.monitor(registry)
    Process.exit(registry, :kill)
    assert_receive {:DOWN, ^ref, :process, ^registry, :killed}, 1_000

    assert Helpers.wait_until(fn -> registry_pid() not in [nil, registry] end, 1_000, 5)
    assert WorkerRegistry.ensure_started() == server
  end
end
