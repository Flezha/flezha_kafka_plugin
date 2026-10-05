defmodule FlezhaKafka.OptionsTest do
  @moduledoc """
  What the package adds to the producer it moved: idempotence by default,
  compression without native code, a held output's identity and the plugin's own
  headers on its records, and a dispatch that only holds and casts.
  """

  use ExUnit.Case, async: true

  import Bitwise

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions
  alias Flezha.State.Outbox
  alias FlezhaKafka.Producer
  alias FlezhaKafka.ProducerWorker
  alias FlezhaKafka.Test.Fakes

  @topic Fakes.topic()

  defp prepare(opts) do
    base = %{hosts: "localhost:19092", topic: @topic, subscribed_extensions: [:on_output]}

    with {:ok, normalised} <- ProviderOptions.normalise_opts(Producer, Map.merge(base, opts)),
         do: Producer.init_provider(normalised)
  end

  defp key(opts) do
    {:ok, prepared} = prepare(opts)
    [{key, _spec}] = Producer.worker_specs(prepared)
    key
  end

  defp request_for(opts, metadata) do
    {pid, _script} =
      Fakes.start_worker(
        Keyword.merge(
          [linger_ms: 0, idempotent: false, send_fn: Fakes.forward_send_fn(self())],
          opts
        )
      )

    :ok = ProducerWorker.cast_enqueue(pid, "v", metadata)
    assert_receive {:dispatched, _ref, request}, 1_000
    [%{data: [%{record_set: batch}]}] = request.topic_data
    {request, batch}
  end

  describe "idempotence" do
    test "is the default, and the worker opens a producer session" do
      {:ok, prepared} = prepare(%{source_plugin_id: "idem", partition: 0})
      [{_key, spec}] = Producer.worker_specs(prepared)
      assert spec.idempotent == true
      assert spec.max_in_flight == 5

      test_pid = self()

      init_fn = fn _client, _timeout ->
        send(test_pid, :init_producer_id)
        {:ok, %{producer_id: 11, producer_epoch: 0}}
      end

      {:ok, _pid} =
        ProducerWorker.start_link(
          Map.merge(spec, %{
            worker_pid: spawn(fn -> :ok end),
            init_producer_id_fn: init_fn,
            name: nil
          })
        )

      assert_receive :init_producer_id, 1_000
    end
  end

  describe "compression" do
    test "snappy, lz4 and zstd are refused at enable, naming the option" do
      for codec <- ["snappy", "lz4", "zstd"] do
        assert {:error, {:invalid_option_value, :compression, ^codec}} =
                 prepare(%{compression: codec})
      end
    end

    test "a gzip batch carries the gzip codec on Produce v5" do
      {request, batch} = request_for([extra: [compression: :gzip]], %{})
      assert %Kayrock.Produce.V5.Request{} = request
      assert (batch.attributes &&& 7) == 1
    end
  end

  describe "headers" do
    @identity %{
      "flz.key" => "k",
      "flz.cell" => "orders/tally/k1",
      "flz.seq" => "7.0",
      "flz.epoch" => "2"
    }

    test "a guaranteed output carries its identity, the dedup key and the plugin's headers" do
      {:ok, prepared} = prepare(%{headers: ~s({"source": "flezha"})})

      metadata =
        Map.put(
          @identity,
          Outbox.ack_key(),
          Outbox.token({"orders", "tally", "k1"}, {:cursor, 7})
        )

      {_request, batch} = request_for([extra: [headers: prepared.headers]], metadata)
      [record] = batch.records

      assert Enum.map(record.headers, &{&1.key, &1.value}) == [
               {"flz.key", "k"},
               {"flz.cell", "orders/tally/k1"},
               {"flz.seq", "7.0"},
               {"flz.epoch", "2"},
               {"flezha.dedup_key", "orders/tally/k1/7.0/2"},
               {"source", "flezha"}
             ]
    end

    test "a plain output carries no identity headers, only the plugin's" do
      {:ok, prepared} = prepare(%{headers: ~s({"source": "flezha"})})
      plain = Map.delete(@identity, "flz.epoch")

      {_request, batch} = request_for([extra: [headers: prepared.headers]], plain)
      [record] = batch.records

      assert record.key == "k"
      assert Enum.map(record.headers, &{&1.key, &1.value}) == [{"source", "flezha"}]
    end

    test "a header name the producer writes itself is refused" do
      for name <- ["flz.key", "flz.anything", "flezha.dedup_key"] do
        assert {:error, {:headers_invalid, "kafka_producer", :headers, ^name}} =
                 prepare(%{headers: %{name => "x"}})
      end

      assert {:ok, %{headers: %{"source" => "flezha"}}} =
               prepare(%{headers: %{"source" => "flezha"}})
    end
  end

  describe "the dispatch and the worker it names" do
    test "on_output holds the output and casts it, once, to the plugin's one worker per topic" do
      {:ok, opts} =
        prepare(%{source_namespace: "default", source_plugin_id: "out", partition: 3})

      assert opts.producer ==
               WorkerSup.worker(Producer, {"default", "out", @topic})

      {:ok, _} =
        Registry.register(
          WorkerSup.registry_name(WorkerSup.family(Producer)),
          {"default", "out", @topic},
          nil
        )

      ctx = %Ctx.IO{
        relay_id: "o",
        namespace: :default,
        message: "m",
        metadata: %{"flz.key" => "k"}
      }

      assert {:ok, ^ctx} = Producer.on_output(ctx, opts)

      assert_receive {:"$gen_cast", {:enqueue_async, "m", %{"flz.key" => "k"}}}
      refute_received _
    end

    test "a shared copy and its relays name one worker; namespaces and members do not" do
      shared = key(%{source_namespace: "default", source_plugin_id: "alerts"})

      assert key(%{
               source_namespace: "default",
               source_plugin_id: "alerts",
               source_relay_id: "op_a"
             }) ==
               shared

      refute key(%{source_namespace: "other", source_plugin_id: "alerts"}) == shared

      refute key(%{source_namespace: "default", source_plugin_id: "alerts#p0"}) ==
               key(%{source_namespace: "default", source_plugin_id: "alerts#p1"})
    end
  end
end
