defmodule FlezhaKafka.ChainMailboxTest do
  @moduledoc """
  The chain in front of a slow broker: an operator's host process runs the
  producer's hook for every input at a steady rate while the worker behind it
  takes 50 ms per Produce. The hook only holds and casts, so the host's mailbox
  stays empty whatever the broker does. The core's `mix bench.extension` ran
  this scenario before the producer left it.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.WorkerSup
  alias FlezhaKafka.Producer
  alias FlezhaKafka.ProducerWorker
  alias FlezhaKafka.Test.Fakes
  alias FlezhaKafka.Test.Helpers

  @rate_per_s 80
  @inputs 160

  test "the host's mailbox stays bounded while the broker takes 50 ms per batch" do
    catalog =
      Host.build_catalog([{:on_input, :transform, Ctx.IO}], %{
        file: __ENV__.file,
        line: __ENV__.line
      })

    plugin = %Plugin{
      id: "chain",
      provider: Producer,
      namespace: "default",
      operator_id: "host",
      opts: %{hosts: "localhost:19092", topic: Fakes.topic(), partition: 0, idempotent: false},
      hooks: [{:on_input, [position: 0]}]
    }

    {:ok, table} = Table.bulk_subscribe(Table.new(catalog), [{"chain", plugin}])
    start_slow_worker(table.plugins["chain"].opts)

    host = spawn_link(fn -> host_loop(table) end)
    peak = drive(host)

    assert peak <= 1, "the host's mailbox reached #{peak}: the dispatch waited on the broker"
    assert Helpers.wait_until(fn -> ProducerWorker.stats(worker()).queue_depth == 0 end, 5_000)
  end

  # The worker the table would start, behind a broker that answers each Produce
  # after 50 ms.
  defp start_slow_worker(opts) do
    send_fn = fn _client, _request, _selector ->
      Process.sleep(50)
      ref = make_ref()
      send(self(), {:kafka_ex_response, ref, Fakes.ok_response(Fakes.topic(), 0)})
      {:ok, ref}
    end

    [{key, spec}] = Producer.worker_specs(opts)
    spec = Map.merge(spec, %{worker_pid: spawn(&Helpers.park/0), send_fn: send_fn})

    {:ok, _pid} =
      WorkerSup.ensure_worker(
        WorkerSup.family(Producer),
        key,
        {ProducerWorker, spec},
        &Process.alive?/1,
        :temporary
      )

    Process.put(:worker_key, key)
    on_exit(fn -> WorkerSup.stop_worker(WorkerSup.family(Producer), key) end)
  end

  defp worker, do: WorkerSup.worker(Producer, Process.get(:worker_key))

  defp host_loop(table) do
    receive do
      {:input, message} ->
        _ =
          Extension.run(table, :on_input, %Ctx.IO{
            operator_id: "host",
            namespace: :default,
            message: message,
            metadata: %{}
          })

        host_loop(table)
    end
  end

  # Inputs at a steady rate, the host's mailbox read before each one; the
  # deepest it was.
  defp drive(host) do
    interval_us = div(1_000_000, @rate_per_s)
    start = System.monotonic_time(:microsecond)

    Enum.reduce(1..@inputs, 0, fn i, peak ->
      {:message_queue_len, depth} = Process.info(host, :message_queue_len)
      send(host, {:input, "msg-#{i}"})
      pause_until(start + i * interval_us)
      max(peak, depth)
    end)
  end

  defp pause_until(target_us) do
    left_ms = div(target_us - System.monotonic_time(:microsecond), 1_000)
    if left_ms > 0, do: Process.sleep(left_ms)
  end
end
