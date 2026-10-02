defmodule FlezhaKafka.Test.Fakes do
  @moduledoc """
  A `ProducerWorker` under test: how it is started, how its accounting is
  awaited, and a scripted stand-in for the `KafkaEx.Client` it sends through.

  The real client answers by message, so these do too: a send returns a dispatch
  handle at once and the outcome reaches the worker's own mailbox later.
  `script_send_fn/2` pops one entry per send from an Agent-held list —
  `{:return, value}`, `{:return_fn, fun}`, or, once the list runs out,
  `:never` — where a value may be `:never` (a dispatch that is never answered,
  so the batch stays in flight) or `{:delayed, value, ms}` (answered after
  `ms`). `relay_send_fn/1` hands every dispatch to the test as
  `{:dispatched, ref, request}` instead, so a test can answer any batch in any
  order with `answer/3`.
  """

  alias FlezhaKafka.ProducerWorker
  alias FlezhaKafka.WorkerRegistry
  alias FlezhaKafka.Test.Helpers

  @topic "p_topic"
  @partition 0

  @doc "The topic and partition `start_worker/1` produces to."
  def topic, do: @topic
  def partition, do: @partition

  @doc """
  The worker spec the Kafka provider itself declares for `plugin_id`, on this
  helper's topic and partition. Every tunable comes from the declaration, so a
  test that starts a worker starts exactly what the extension table would.
  """
  def worker_spec(plugin_id) do
    {:ok, normalised} =
      Flezha.Plugin.ProviderBehaviour.Options.normalise_opts(
        FlezhaKafka.Producer,
        %{
          hosts: [{"localhost", 9092}],
          topic: @topic,
          partition: @partition,
          plugin_id: plugin_id
        }
      )

    [{_key, spec}] = FlezhaKafka.Producer.worker_specs(normalised)
    spec
  end

  @doc """
  A `ProducerWorker` linked to the caller, plus the Agent holding its send
  script. Every knob the worker takes has a default here; `opts` overrides one
  by name, and `:extra` appends raw init options.
  """
  def start_worker(opts) do
    test_pid = self()

    {:ok, script_pid} = Agent.start_link(fn -> Keyword.get(opts, :script, []) end)

    base = [
      worker_pid: spawn(&Helpers.park/0),
      linger_ms: Keyword.get(opts, :linger_ms, 5),
      max_batch_bytes: Keyword.get(opts, :max_batch_bytes, 1_048_576),
      queue_max: Keyword.get(opts, :queue_max, 1_000),
      queue_overflow: Keyword.get(opts, :queue_overflow, :drop_oldest),
      queue_block_timeout_ms: Keyword.get(opts, :queue_block_timeout_ms, 5_000),
      retries: Keyword.get(opts, :retries, 5),
      retry_backoff_ms: Keyword.get(opts, :retry_backoff_ms, 5),
      retry_max_backoff_ms: Keyword.get(opts, :retry_max_backoff_ms, 50),
      delivery_timeout_ms: Keyword.get(opts, :delivery_timeout_ms, 1_000),
      idempotent: Keyword.get(opts, :idempotent, true),
      max_in_flight: Keyword.get(opts, :max_in_flight, 1),
      auto_create_topic: Keyword.get(opts, :auto_create_topic, true),
      metadata_refresh_timeout_ms: 100,
      send_fn: Keyword.get(opts, :send_fn) || script_send_fn(test_pid, script_pid),
      init_producer_id_fn: Keyword.get(opts, :init_producer_id_fn, init_producer_id_ok()),
      metadata_refresh_fn: Keyword.get(opts, :metadata_refresh_fn, metadata_refresh_ok()),
      create_topic_fn: Keyword.get(opts, :create_topic_fn, create_topic_ok()),
      now_fn: Keyword.get(opts, :now_fn, fn -> System.monotonic_time(:millisecond) end),
      name: nil
    ]

    spec = worker_spec("test-#{System.unique_integer([:positive])}")

    {:ok, pid} =
      GenServer.start_link(
        ProducerWorker,
        Map.merge(spec, Map.new(base ++ Keyword.get(opts, :extra, [])))
      )

    {pid, script_pid}
  end

  @doc """
  A worker whose broker refuses every send, retrying until its delivery
  deadline: the shape every give-up scenario starts from.
  """
  def start_exhausting_worker do
    start_worker(
      retry_backoff_ms: 1,
      retry_max_backoff_ms: 5,
      delivery_timeout_ms: 30,
      retries: 100,
      script: List.duplicate({:return, {:error, :no_broker}}, 200)
    )
  end

  @doc "Replace the worker's broker-registry reference in place."
  def seed_registry_ref(pid, ref) do
    :sys.replace_state(pid, fn state -> %{state | registry_ref: ref} end)
  end

  @doc """
  Point the worker at a fresh registry reference and stand a replacement worker
  behind it, then script the one `swap` a `{:kafka_worker_dead, ref, _}` makes:
  `:swapped` hands over the replacement, `{:refused, reason}` does not.
  """
  def expect_worker_swap(pid, swap \\ :swapped) do
    registry_ref = {{[{"k", 9092}], 0, 0}, make_ref()}
    seed_registry_ref(pid, registry_ref)
    new_worker_pid = spawn(&Helpers.park/0)

    case swap do
      :swapped ->
        Mimic.expect(WorkerRegistry, :swap, fn ^registry_ref ->
          {:ok, new_worker_pid, {elem(registry_ref, 0), make_ref()}}
        end)

      {:refused, reason} ->
        Mimic.expect(WorkerRegistry, :swap, fn ^registry_ref -> {:error, reason} end)
    end

    {registry_ref, new_worker_pid}
  end

  @doc """
  Answer the first dispatch after `delay_ms` and the next `follow_ups` at once:
  a worker whose queue backs up behind one slow batch.
  """
  def script_slow_first(script_pid, delay_ms \\ 150, follow_ups \\ 10) do
    Agent.update(script_pid, fn _ ->
      [
        {:return, {:delayed, ok_response(@topic, @partition), delay_ms}}
        | List.duplicate({:return, ok_response(@topic, @partition)}, follow_ups)
      ]
    end)
  end

  @doc "Records across every batch the worker still has in flight."
  def window_records(state) do
    Enum.reduce(state.in_flight, 0, fn batch, n -> n + length(batch.records) end)
  end

  # The worker's own accounting is the settle signal for a caller that
  # enqueues from another process.
  def in_flight?(pid, n),
    do: Helpers.wait_until(fn -> ProducerWorker.stats(pid).in_flight == n end, 1_000, 5)

  def queued?(pid, n),
    do: Helpers.wait_until(fn -> ProducerWorker.stats(pid).queue_depth == n end, 1_000, 5)

  def dispatched?(pid),
    do: Helpers.wait_until(fn -> ProducerWorker.stats(pid).in_flight >= 1 end, 1_000, 5)

  @doc """
  Forward one of the producer's telemetry events to the calling process as
  `{:telemetry, event, measurements, metadata}` until the test exits.
  """
  def attach(tag) do
    event =
      case tag do
        :produce_ok -> [:flezha, :plugin, :kafka, :shipped]
        :produce_retry -> [:flezha, :plugin, :kafka, :retried]
        :produce_failed -> [:flezha, :plugin, :kafka, :failed]
        :dropped -> [:flezha, :plugin, :kafka, :overflowed]
        :metadata_refresh -> [:flezha, :plugin, :kafka, :metadata_refresh]
        :topic_created -> [:flezha, :plugin, :kafka, :topic_created]
        :worker_swapped -> [:flezha, :plugin, :kafka, :worker_swapped]
      end

    Helpers.capture(event)
  end

  def script_send_fn(test_pid, script_pid) do
    fn worker, request, node_selector ->
      send(test_pid, {:send_called, request, node_selector})

      scripted =
        Agent.get_and_update(script_pid, fn
          [] -> {{:return, :never}, []}
          [head | tail] -> {head, tail}
        end)

      case scripted do
        {:return, value} -> respond(value)
        {:return_fn, fun} -> respond(fun.(worker, request, node_selector))
      end
    end
  end

  def relay_send_fn(test_pid) do
    fn _worker, request, _node_selector ->
      ref = make_ref()
      send(test_pid, {:dispatched, ref, request})
      {:ok, ref}
    end
  end

  def answer(worker, ref, result), do: send(worker, {:kafka_ex_response, ref, result})

  def init_producer_id_ok(producer_id \\ 7777, epoch \\ 0) do
    fn _worker, _timeout -> {:ok, %{producer_id: producer_id, producer_epoch: epoch}} end
  end

  def init_producer_id_fail(reason), do: fn _worker, _timeout -> {:error, reason} end

  def metadata_refresh_ok, do: fn _worker, _timeout -> :ok end

  def create_topic_ok, do: fn _worker, _topic, _opts, _timeout -> :ok end

  def create_topic_already_exists,
    do: fn _worker, _topic, _opts, _timeout -> {:error, :topic_already_exists} end

  def ok_response(topic, partition, base_offset \\ 100),
    do: produce_response(topic, partition, 0, base_offset)

  def err_response(topic, partition, error_code),
    do: produce_response(topic, partition, error_code, -1)

  defp produce_response(topic, partition, error_code, base_offset) do
    {:ok,
     %Kayrock.Produce.V5.Response{
       responses: [
         %{
           topic: topic,
           partition_responses: [
             %{
               partition: partition,
               error_code: error_code,
               base_offset: base_offset,
               log_append_time: -1,
               log_start_offset: -1
             }
           ]
         }
       ],
       throttle_time_ms: 0
     }}
  end

  # The send runs in the worker, so the scripted outcome goes to its mailbox.
  defp respond(:never), do: {:ok, make_ref()}

  defp respond({:delayed, value, delay_ms}) do
    ref = make_ref()
    Process.send_after(self(), {:kafka_ex_response, ref, value}, delay_ms)

    {:ok, ref}
  end

  defp respond(value) do
    ref = make_ref()
    send(self(), {:kafka_ex_response, ref, value})
    {:ok, ref}
  end
end
