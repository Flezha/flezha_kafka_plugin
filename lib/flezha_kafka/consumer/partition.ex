defmodule FlezhaKafka.Consumer.Partition do
  @moduledoc """
  The fork's `KafkaEx.Consumer.GenConsumer` callback module for one assigned
  partition of a `FlezhaKafka.Consumer`: each fetched record set becomes one
  batch of inputs handed to the relay with `Host.inject_many/4` in
  `:verdict` mode.

  A batch the relay took in full is committed at once when its input is
  durable (`:sync_commit`), and by the timer when it is not (`:async_commit`).
  A batch with any refusal commits nothing: the consumer exits, its supervisor
  starts it again, and it fetches from the last committed offset. The member is
  told the outcome of every batch, which is what its health counts.
  """

  use KafkaEx.Consumer.GenConsumer

  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Host.MsgId
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Relay.Ingest.Verdict

  require MsgId
  require Verdict
  require Logger

  @provider :kafka_consumer

  # How long a revoked or stopped consumer is given to finish its batch, and the
  # inject deadline below it: a batch waiting on the relay ends refused, so
  # uncommitted, before the supervisor would kill it.
  @shutdown_ms 2_500
  @inject_timeout_ms 2_000

  # Without durable input only the timer commits: no batch acknowledges this many.
  @commit_threshold 1_000_000_000

  @doc """
  The GenConsumer options every partition consumer of the calling member is
  started with: its stop budget, its commit settings and the arguments its
  `init/3` is given.
  """
  @spec consumer_opts(map()) :: keyword()
  def consumer_opts(member) do
    [
      shutdown: @shutdown_ms,
      commit_interval: member.commit_interval_ms,
      commit_threshold: @commit_threshold,
      extra_consumer_args: %{
        host: member.overwatcher_pid,
        channel: member.entry_channel,
        plugin_id: member.plugin_id,
        durable: Map.get(member, :durable_input) == true,
        member: self()
      }
    ]
  end

  @impl true
  def init(topic, partition, args),
    do: {:ok, Map.merge(args, %{topic: topic, partition: partition})}

  @impl true
  def handle_message_set(records, state) do
    verdicts =
      Host.inject_many(
        state.host,
        Enum.map(records, &input(&1, state)),
        :verdict,
        @inject_timeout_ms
      )

    case Enum.find(verdicts, &(not Verdict.taken?(&1))) do
      nil ->
        send(state.member, :batch_taken)
        emit(:shipped, %{count: length(records)}, labels(state))
        {if(state.durable, do: :sync_commit, else: :async_commit), state}

      refusal ->
        refused(state, records, refusal)
    end
  end

  # The exit is the GenConsumer's stop: it commits nothing for this set, and a
  # `:shutdown` reason is restarted without a crash report.
  defp refused(state, [first | _] = records, refusal) do
    error = "ingest refused: #{inspect(refusal)}"

    Logger.warning(
      "[FlezhaKafka.Consumer] topic=#{state.topic} partition=#{state.partition} " <>
        "offset=#{first.offset} records=#{length(records)} #{error}; " <>
        "fetching again from the last committed offset"
    )

    emit(:failed, %{count: length(records)}, Map.put(labels(state), :reason, error))
    send(state.member, {:batch_refused, error})
    exit({:shutdown, {:ingest_refused, refusal}})
  end

  # ---------- one record ----------

  @doc """
  A record as the input it becomes: its value (empty for a null one) and its
  metadata, every value a string as every input's are.
  """
  @spec input(KafkaEx.Messages.Fetch.Record.t(), map()) :: {binary(), map()}
  def input(record, state) do
    position = "#{state.topic}/#{state.partition}/#{record.offset}"

    metadata =
      %{
        "channel" => state.channel,
        "kafka.topic" => state.topic,
        "kafka.partition" => Integer.to_string(state.partition),
        "kafka.offset" => Integer.to_string(record.offset)
      }
      |> MsgId.stamp(state.plugin_id, position)
      |> put_text("kafka.timestamp", record.timestamp && Integer.to_string(record.timestamp))
      |> put_text("kafka.key", record.key)

    {record.value || "", Enum.reduce(record.headers || [], metadata, &put_header/2)}
  end

  # The last header of a name wins, in either spelling.
  defp put_header(%{key: name, value: value}, metadata),
    do: put_text(metadata, "kafka.header." <> name, value)

  # A value that is not UTF-8 text is carried Base64-encoded under `<name>.b64`.
  defp put_text(metadata, _name, nil), do: metadata

  defp put_text(metadata, name, value) do
    if String.valid?(value),
      do: metadata |> Map.delete(name <> ".b64") |> Map.put(name, value),
      else: metadata |> Map.delete(name) |> Map.put(name <> ".b64", Base.encode64(value))
  end

  defp labels(state),
    do: %{plugin_id: state.plugin_id, topic: state.topic, partition: state.partition}

  defp emit(verb, measurements, metadata),
    do: ProviderBehaviour.emit(@provider, verb, measurements, metadata)
end
