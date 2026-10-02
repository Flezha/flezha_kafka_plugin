defmodule FlezhaKafka.ProducerWorker.Wire do
  @moduledoc """
  The Kayrock request/response layer of the Kafka producer: building a Produce
  request and its records, classifying a broker's answer, and the five default
  client calls a test replaces with its own functions.

  Every function here is total and holds no worker state, which is what makes
  it safe to call from any of the worker's paths. `do_send/2` is deliberately
  NOT here — it records the dispatch and cancels a timer, so it belongs with
  the state it mutates.
  """

  alias Flezha.State.Outbox
  alias Kayrock.RecordBatch.RecordHeader
  alias KafkaEx.Client, as: KafkaClient
  alias KafkaEx.Client.NodeSelector

  @retriable [
    :no_broker,
    :not_leader_for_partition,
    :network_exception,
    :request_timed_out,
    :not_enough_replicas,
    :not_enough_replicas_after_append,
    :replica_not_available,
    :leader_not_available,
    :broker_not_available,
    :closed,
    :correlation_mismatch
  ]

  @permanent [
    :invalid_topic_exception,
    :invalid_topic,
    :record_list_too_large,
    :message_size_too_large,
    :offset_metadata_too_large,
    :topic_authorization_failed,
    :cluster_authorization_failed
  ]

  # The batch is genuinely stored already (a retry that landed twice). Safe to ack.
  @idempotence_already_delivered [:duplicate_sequence_number]

  # The producer session is fenced/desynced and the batch was NOT stored. The
  # session must be re-initialised and the batch resent — acking it would drop data.
  @idempotence_fenced [:out_of_order_sequence_number, :invalid_producer_epoch]

  # The record-batch attribute bits of each codec, as Kayrock reads them.
  @codecs %{none: 0, gzip: 1}

  # The output identity a held record carries as headers, where its output has it.
  @identity_headers ["flz.key", "flz.cell", "flz.seq", "flz.epoch"]

  def build_produce_request(state, records, base_sequence) do
    record_batch = %Kayrock.RecordBatch{
      attributes: Map.fetch!(@codecs, state.compression),
      producer_id: if(state.idempotent, do: state.producer_id, else: -1),
      producer_epoch: if(state.idempotent, do: state.producer_epoch, else: -1),
      base_sequence: if(state.idempotent, do: base_sequence, else: -1),
      records:
        Enum.map(records, fn r ->
          %Kayrock.RecordBatch.Record{
            attributes: 0,
            timestamp: r.timestamp,
            offset: 0,
            key: r.key,
            value: r.value,
            headers: record_headers(r, state.static_headers)
          }
        end)
    }

    %Kayrock.Produce.V5.Request{
      transactional_id: nil,
      acks: state.required_acks,
      timeout: state.request_timeout_ms,
      topic_data: [
        %{
          topic: state.topic,
          data: [%{partition: state.partition, record_set: record_batch}]
        }
      ]
    }
  end

  defp record_headers(%{metadata: metadata, dedup_key: dedup_key}, static) do
    dedup =
      if is_binary(dedup_key),
        do: [%RecordHeader{key: "flezha.dedup_key", value: dedup_key}],
        else: []

    identity_headers(metadata) ++ dedup ++ static
  end

  # Only an output the outbox holds is deduplicated downstream; a plain output's
  # identity headers would cost produce throughput for nothing.
  defp identity_headers(metadata) do
    if Outbox.guaranteed?(metadata) do
      for name <- @identity_headers,
          value = metadata[name],
          value not in [nil, ""],
          do: %RecordHeader{key: name, value: to_string(value)}
    else
      []
    end
  end

  @doc "The plugin's `headers` option as the record headers every record carries."
  @spec static_headers(%{binary() => binary()}) :: [RecordHeader.t()]
  def static_headers(headers),
    do: for({name, value} <- Enum.sort(headers), do: %RecordHeader{key: name, value: value})

  def build_record(msg, metadata, from, timestamp_ms) do
    key = message_key(metadata)
    dedup_key = Outbox.dedup_key(metadata)

    %{
      from: from,
      value: msg,
      key: key,
      dedup_key: dedup_key,
      metadata: metadata,
      timestamp: timestamp_ms,
      bytes: byte_size_or(0, msg) + byte_size_or(0, key) + byte_size_or(0, dedup_key)
    }
  end

  # Effectively-once tier record (`flz.epoch` present): the natural key is
  # the Kafka message key — stable partitioning across replays, upsert
  # semantics on compacted/changelog topics — and the full durable identity
  # travels as the `flezha.dedup_key` header (`Outbox.dedup_key/1`) for
  # consumer-side exact dedup on append-only topics. Re-driven entries carry
  # identical identity.
  defp message_key(%{"flz.epoch" => _, "flz.cell" => _, "flz.seq" => _} = metadata),
    do: Map.get(metadata, "flz.key")

  # At-least-once records: no identity header, but a `.keyed()` channel's
  # natural key still partitions. Only the effectively-once tier stamps
  # `flz.epoch`, so requiring it here would ship every ordinary keyed emit
  # with a nil key — and a nil key scatters one cell's records across
  # partitions, losing exactly the ordering `.keyed()` was declared for.
  defp message_key(metadata) do
    Map.get(metadata, "flz.key") ||
      Map.get(metadata, "flezha.dedup_key") ||
      Map.get(metadata, "kafka_msg_key", nil)
  end

  defp byte_size_or(_default, bin) when is_binary(bin), do: byte_size(bin)
  defp byte_size_or(default, _), do: default

  @doc """
  Classify a broker answer into the outcome the in-flight window acts on. The
  clause order IS the rule: `@retriable` and `@permanent` are consulted before
  the shapes that fall through to `:permanent`.
  """
  @spec classify_result(term(), map()) :: {atom(), term()}
  def classify_result({:ok, %{responses: responses}}, state) do
    responses
    |> extract_partition(state.topic, state.partition)
    |> classify_partition()
  end

  def classify_result({:error, reason}, _state), do: classify_error(reason)

  defp classify_partition({:ok, base_offset}), do: {:ok, base_offset}
  defp classify_partition({:error, code}) when code in @retriable, do: {:retry, code}
  defp classify_partition({:error, code}) when code in @permanent, do: {:permanent, code}

  defp classify_partition({:error, :unknown_topic_or_partition}),
    do: {:retry, :unknown_topic_or_partition}

  defp classify_partition({:error, code}) when code in @idempotence_fenced, do: {:fenced, code}

  defp classify_partition({:error, code}) when code in @idempotence_already_delivered,
    do: {:already_delivered, code}

  defp classify_partition({:error, other}), do: {:permanent, other}

  defp classify_error(:no_broker), do: {:retry, :no_broker}
  defp classify_error(:timeout), do: {:retry, :request_timed_out}
  defp classify_error({:exit, _}), do: {:retry, :network_exception}
  defp classify_error(reason) when is_atom(reason) and reason in @retriable, do: {:retry, reason}

  defp classify_error(reason) when is_atom(reason) and reason in @permanent,
    do: {:permanent, reason}

  defp classify_error({:send_crashed, _kind, _reason}), do: {:retry, :network_exception}
  defp classify_error(:dispatch_timeout), do: {:retry, :dispatch_timeout}
  defp classify_error(reason), do: {:permanent, reason}

  def extract_partition(responses, topic, partition) do
    case Enum.find(responses, fn r -> Map.get(r, :topic) == topic end) do
      nil ->
        {:error, :no_topic_in_response}

      %{partition_responses: parts} ->
        parts
        |> Enum.find(fn p -> Map.get(p, :partition) == partition end)
        |> partition_outcome()
    end
  end

  defp partition_outcome(nil), do: {:error, :no_partition_in_response}
  defp partition_outcome(%{error_code: 0, base_offset: offset}), do: {:ok, offset}
  defp partition_outcome(%{error_code: code}), do: {:error, Kayrock.ErrorCode.code_to_atom(code)}

  def default_send(worker_pid, request, node_selector) do
    KafkaClient.send_request_async(worker_pid, request, node_selector)
  end

  def default_init_producer_id(worker_pid, timeout_ms) do
    request = %Kayrock.InitProducerId.V0.Request{
      transactional_id: nil,
      transaction_timeout_ms: timeout_ms
    }

    case KafkaClient.send_request(worker_pid, request, NodeSelector.first_available(), timeout_ms) do
      {:ok, %{error_code: 0, producer_id: pid, producer_epoch: epoch}} ->
        {:ok, %{producer_id: pid, producer_epoch: epoch}}

      {:ok, %{error_code: code}} ->
        {:error, Kayrock.ErrorCode.code_to_atom(code)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def default_create_topic(worker_pid, topic, opts, timeout_ms) do
    request = %Kayrock.CreateTopics.V0.Request{
      timeout_ms: opts.timeout_ms,
      topics: [
        %{
          name: topic,
          num_partitions: opts.num_partitions,
          replication_factor: opts.replication_factor,
          assignments: [],
          configs: Enum.map(opts.config_entries, &config_entry/1)
        }
      ]
    }

    case KafkaClient.send_request(worker_pid, request, NodeSelector.controller(), timeout_ms) do
      {:ok, %{topics: errors}} ->
        errors
        |> Enum.find(&(Map.get(&1, :name) == topic))
        |> create_outcome()

      {:error, reason} ->
        {:error, reason}
    end
  end

  # A response naming no topic is a success: the broker had nothing to report.
  defp create_outcome(%{error_code: 0}), do: :ok
  defp create_outcome(%{error_code: code}), do: {:error, Kayrock.ErrorCode.code_to_atom(code)}
  defp create_outcome(nil), do: :ok

  defp config_entry({k, v}), do: %{name: to_string(k), value: to_string(v)}

  defp config_entry(%{config_name: cn, config_value: cv}),
    do: %{name: to_string(cn), value: to_string(cv)}

  defp config_entry(%{name: _, value: _} = entry), do: entry

  def default_metadata_refresh(worker_pid, timeout_ms) do
    GenServer.call(worker_pid, :update_metadata, timeout_ms)
  end

  @doc """
  The topic's partition count from a fresh metadata read: `{:ok, n}`, `{:ok, :absent}`
  for a topic the broker does not know (a metadata read never creates one here),
  or `{:error, reason}`.
  """
  def default_partition_count(worker_pid, topic, timeout_ms) do
    case GenServer.call(worker_pid, {:topic_metadata, [topic], false}, timeout_ms) do
      {:ok, [%{partitions: [_ | _] = partitions}]} -> {:ok, length(partitions)}
      {:ok, _none} -> {:ok, :absent}
      {:error, reason} -> {:error, reason}
    end
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end
end
