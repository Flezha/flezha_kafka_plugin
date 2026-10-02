defmodule FlezhaKafka.Producer do
  @moduledoc """
  The `kafka_producer` provider: an `:on_input` / `:on_output` sink that produces
  each record to a Kafka topic, with retries, idempotence, batching, a bounded
  queue, compression and active metadata refresh on transient broker errors.

  Every operator bound to one plugin on a controller casts its records to ONE
  worker per topic (`FlezhaKafka.ProducerWorker`), which picks each record's
  partition and keeps a lane per partition. The dispatch holds a
  delivery-guaranteed output for the worker to confirm or park; a plain output
  holds nothing. Options, partitioning, headers and telemetry:
  `docs/plugins/kafka_producer.md` in the core.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Provider.HttpFetch
  alias Flezha.Plugin.Provider.WorkerSup
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.State.Outbox
  alias FlezhaKafka.ProducerWorker
  alias KafkaEx.Auth.Config, as: KafkaAuthConfig

  import ProviderBehaviour, only: [parse_enum: 3, refuse_value: 2]

  @queue_overflow_policies [:drop_nothing, :drop_oldest, :drop_newest]

  # The keys and the mechanisms `KafkaEx.Auth.Config` accepts.
  @auth_keys [:mechanism, :username, :password, :mechanism_opts]
  @auth_mechanisms [:plain, :scram, :oauthbearer, :msk_iam]

  use ProviderBehaviour,
    name: "kafka",
    shareable: true,
    telemetry: [metadata_refresh: :metric, topic_created: :event, worker_swapped: :event],
    options: [
      hosts: [
        type: :any,
        required: true,
        doc: "`{host, port}` list, or a comma-separated string"
      ],
      topic: [type: :string, required: true, doc: "Destination topic"],
      plugin_id: [type: :string, doc: "Plugin id; defaults to a generated one"],
      channel: [
        type: :names,
        doc:
          "The channel this plugin is attached to: only records naming it are produced. Several names, comma-separated, produce them all"
      ],
      channel_out: [
        type: :names,
        doc:
          "Produce only records whose metadata channel matches; the explicit spelling. Several names, comma-separated, produce them all"
      ],
      partition: [
        type: :int,
        min: 0,
        doc: "A fixed destination partition for every record; unset, `partitioner` chooses"
      ],
      partitioner: [
        type: :enum,
        values: [:murmur2, :sticky],
        default: :murmur2,
        doc:
          "Without `partition`: `murmur2` sends a keyed record where Kafka's Java client would and an unkeyed one to the sticky partition; `sticky` sends every record to the sticky partition, which moves after each batch"
      ],
      compression: [
        type: :enum,
        values: [:none, :gzip],
        default: :none,
        doc:
          "Batch compression; `snappy`, `lz4` and `zstd` are refused, the client library encodes them only with native code"
      ],
      headers: [
        type: :json,
        default: %{},
        doc:
          "Static record headers, a JSON object of strings; names starting `flz.` and `flezha.dedup_key` are refused"
      ],
      required_acks: [type: :int, default: -1, doc: "Broker acks; -1 is acks=all"],
      retries: [type: :int, default: 5, min: 0, doc: "Attempts per batch before parking it"],
      retry_backoff_ms: [type: :int, default: 100, min: 1, doc: "First delay between retries"],
      retry_max_backoff_ms: [type: :int, default: 5_000, min: 1, doc: "Ceiling for that delay"],
      delivery_timeout_ms: [
        type: :int,
        default: 30_000,
        min: 1,
        doc: "Total budget for one batch before it is parked"
      ],
      linger_ms: [type: :int, default: 10, min: 0, doc: "How long a batch waits to fill"],
      max_batch_bytes: [type: :int, default: 1_048_576, min: 1, doc: "Largest batch produced"],
      queue_max: [
        type: :int,
        default: 10_000,
        min: 1,
        doc: "Records held before the policy applies"
      ],
      queue_overflow: [
        type: :enum,
        values: @queue_overflow_policies,
        default: :drop_nothing,
        doc: "What a full queue does: block, drop the oldest, or drop the newest"
      ],
      queue_block_timeout_ms: [
        type: :int,
        default: 5_000,
        min: 1,
        doc: "How long an enqueue parks on a full queue before it is refused"
      ],
      idempotent: [
        type: :bool,
        default: true,
        doc: "Use the broker's idempotent producer session"
      ],
      max_in_flight: [
        type: :int,
        min: 1,
        doc: "Batches in flight; derived as 5 with `idempotent`, else 1"
      ],
      auto_create_topic: [type: :bool, default: true, doc: "Create the topic when missing"],
      auto_create_partitions: [type: :int, default: 1, min: 1, doc: "Partitions for that topic"],
      auto_create_replication_factor: [
        type: :int,
        default: 2,
        min: 1,
        doc: "Replication factor for that topic"
      ],
      topic_configs: [type: :list, default: [], doc: "`{name, value}` pairs for that topic"],
      metadata_refresh_timeout_ms: [
        type: :int,
        default: 5_000,
        min: 1,
        doc: "Budget for one broker metadata refresh"
      ],
      request_timeout_ms: [
        type: :int,
        default: 10_000,
        min: 1,
        doc: "Budget the broker is asked to honour for one produce request"
      ],
      dispatch_timeout_ms: [
        type: :int,
        default: 10_000,
        min: 1,
        doc: "Budget for one in-flight batch before it is treated as lost"
      ],
      auth: [type: :any, doc: "SASL configuration, as a `KafkaEx.Auth.Config` or a map"],
      # The resolved credential, held in memory so the client start and every
      # reconnect on this node authenticate with the real secret while the
      # stored opts carry only the placeholder. Never persisted, so a handover
      # to another node resolves from `sasl_password_env` or from nothing,
      # which is what the page already says.
      auth_secret: [type: :any, internal: true, transient: true],
      # The worker this plugin's records are cast to, named once here so the
      # dispatch builds nothing per record.
      producer: [type: :any, internal: true, transient: true],
      sasl_password_env: [
        type: :string,
        doc: "Environment variable holding the SASL password, preferred over an inline one"
      ],
      use_ssl: [type: :bool, doc: "TLS to the brokers"],
      ssl_options: [type: :any, doc: "Erlang `:ssl` options for that connection"],
      metadata_update_interval: [
        type: :int,
        min: 1,
        doc: "How often the shared broker client refreshes metadata"
      ]
    ],
    worker: [
      module: ProducerWorker,
      restart: :permanent,
      reusable: {ProducerWorker, :reusable?}
    ]

  @impl true
  def init_provider(opts) do
    # The plugin's own name where a host gives one, so a shared copy and the
    # operators bound to it name one producer.
    plugin_id =
      opts.plugin_id || opts[:source_plugin_id] ||
        "default-#{System.unique_integer([:positive])}"

    with {:ok, hosts} <- parse_hosts(opts.hosts),
         :ok <- validate_window(opts),
         {:ok, headers} <- validate_headers(opts.headers) do
      opts = %{opts | plugin_id: plugin_id}

      {:ok,
       opts
       |> Map.merge(%{
         hosts: hosts,
         headers: headers,
         auth_secret: opts.auth_secret || opts.auth,
         producer: WorkerSup.worker(__MODULE__, key(opts))
       })
       |> redact_sasl()}
    end
  end

  # A static header may not pass for one the producer writes from the output.
  defp validate_headers(headers) do
    with {:ok, headers} <- HttpFetch.validate_headers(headers, "kafka_producer") do
      case Enum.find(Map.keys(headers), &reserved_header?/1) do
        nil -> {:ok, headers}
        name -> {:error, {:headers_invalid, "kafka_producer", :headers, name}}
      end
    end
  end

  defp reserved_header?(name), do: String.starts_with?(name, "flz.") or name == "flezha.dedup_key"

  @doc """
  One producer worker per `{namespace, plugin_id, topic}`: the operators bound to
  the plugin share it, and it keeps a lane per partition. Two plugins on one
  cluster share a broker client without sharing a queue.

  Every tunable is already converted and already carries its declared default,
  so this names only what the worker needs and the declaration cannot supply:
  the connection identity, and the window whose default follows `:idempotent`.
  `:auth` never appears: the credentials travel in the identity closure.
  """
  @impl true
  def worker_specs(opts) do
    [
      {key(opts),
       opts
       |> Map.take([
         :topic,
         :plugin_id,
         :partition,
         :partitioner,
         :compression,
         :headers,
         :required_acks,
         :retries,
         :retry_backoff_ms,
         :retry_max_backoff_ms,
         :delivery_timeout_ms,
         :dispatch_timeout_ms,
         :request_timeout_ms,
         :linger_ms,
         :max_batch_bytes,
         :queue_max,
         :queue_overflow,
         :queue_block_timeout_ms,
         :idempotent,
         :auto_create_topic,
         :auto_create_partitions,
         :auto_create_replication_factor,
         :topic_configs,
         :metadata_refresh_timeout_ms
       ])
       |> Map.merge(%{
         connection_identity: identity(opts),
         # 5 is Kafka's own ceiling for ordered idempotent retries; without a
         # producer session there is nothing to keep a retry in order.
         max_in_flight: opts.max_in_flight || if(opts.idempotent, do: 5, else: 1)
       })}
    ]
  end

  # A `KafkaEx.Client` answers one request at a time, encoding each batch in
  # its own process, so every producer behind one client serialises on it.
  # The lane spreads a cluster's producers over as many clients as there are
  # schedulers — one connection per core at most, which is all the encode
  # parallelism the node has to give.
  #
  # SASL credentials ride the closure into the client start (mirroring the
  # Postgres provider): they never land in worker state, child args, or the
  # persisted/returned opts. `auth_key` is what lets two plugins with the same
  # credentials share one broker client without the registry ever seeing the
  # secret. Prefer `:sasl_password_env` — an inline password cannot survive a
  # restart from the redacted persisted opts.
  defp identity(opts) do
    # The credential the closure resolves is the transient one, which the
    # stored opts do not carry: `resolve_auth/1` reads it and the redacted
    # `:auth` never reaches the client.
    opts = Map.put(opts, :auth, opts.auth_secret || opts.auth)
    resolved_auth = resolve_auth(opts)

    %{
      hosts: opts.hosts,
      ssl_options: opts.ssl_options,
      use_ssl: opts.use_ssl,
      auth_fun: fn -> resolve_auth(opts) end,
      auth_key: resolved_auth && :erlang.phash2(resolved_auth),
      metadata_update_interval: opts.metadata_update_interval,
      lane: :erlang.phash2(key(opts), System.schedulers_online())
    }
  end

  # Only the broker's per-partition sequence check keeps a retry inside the
  # window ordered, so a window wider than one without idempotence would
  # trade the order the plugin keeps within each partition for throughput.
  defp validate_window(%{max_in_flight: window, idempotent: idempotent})
       when is_integer(window) and window > 1 and not idempotent do
    {:error, {:window_needs_idempotence, window}}
  end

  defp validate_window(_opts), do: :ok

  @redacted "**redacted**"

  # Resolve the SASL config presented by the plugin opts into a
  # `KafkaEx.Auth.Config`. `:sasl_password_env` (env-var name) overrides an
  # inline password, so credentials can stay out of the persisted opts
  # entirely. Called at every client (re)start, so a rotated env var takes
  # effect on the next reconnect — and it can only do that safely because the
  # keys and the mechanism were closed at preparation.
  defp resolve_auth(opts) do
    case Map.get(opts, :auth) do
      nil ->
        nil

      %KafkaEx.Auth.Config{} = config ->
        maybe_override_password(config, opts)

      %{} = map ->
        map
        |> Map.put_new(:mechanism, :plain)
        |> KafkaAuthConfig.new()
        |> maybe_override_password(opts)
    end
  end

  defp maybe_override_password(%KafkaEx.Auth.Config{} = config, opts) do
    case Map.get(opts, :sasl_password_env) do
      env when is_binary(env) and env != "" ->
        %{config | password: System.get_env(env)}

      _ ->
        config
    end
  end

  defp redact_sasl(opts) do
    case Map.get(opts, :auth) do
      %KafkaEx.Auth.Config{password: p} = config when is_binary(p) and p != "" ->
        Map.put(opts, :auth, %{config | password: @redacted})

      %{} = map ->
        Map.put(opts, :auth, redact_map_password(map))

      _ ->
        opts
    end
  end

  defp redact_map_password(%{password: p} = map) when is_binary(p) and p != "",
    do: %{map | password: @redacted}

  defp redact_map_password(map), do: map

  @impl true
  def on_input(%Ctx.IO{} = ctx, opts), do: produce(ctx, opts)
  @impl true
  def on_output(%Ctx.IO{} = ctx, opts), do: produce(ctx, opts)

  defp produce(%Ctx.IO{message: msg, metadata: md} = ctx, opts) do
    if ProviderBehaviour.in_channel?(md, opts.filter_channel),
      do: do_produce(ctx, opts, msg, md),
      else: {:ok, ctx}
  end

  # Nothing per record but the hold and the cast: the worker chooses the
  # partition when it takes the record off its mailbox.
  defp do_produce(%Ctx.IO{} = ctx, opts, msg, md) do
    # Take ownership of a delivery-guaranteed output before handing it over:
    # the worker answers the hold when the broker acks its batch, or parks it
    # when the retry ladder is permanently exhausted. A plain output holds
    # nothing and this is a no-op.
    Outbox.hold(md)
    :ok = ProducerWorker.cast_enqueue(opts.producer, msg, md)
    {:ok, ctx}
  end

  # The key one producer is registered under, for both the declaration and the
  # dispatch path. The namespace keeps two namespaces' plugins of one name
  # apart; a partition member's plugin id carries its member suffix.
  defp key(opts), do: {opts[:source_namespace], opts.plugin_id, opts.topic}

  @doc """
  Synchronous read of producer counters for `plugin_id`: the plugin's one worker
  per topic, the first found when it produces to several.

  Returns `{:ok, %{queue_depth, in_flight, dropped_total, last_error,
  consecutive_failures, partitions, topic}}`, where `partitions` holds each
  partition lane's `queue_depth` and `in_flight`, or `{:error, :no_producer}`.
  """
  @spec stats(binary()) :: {:ok, map()} | {:error, :no_producer}
  def stats(plugin_id) when is_binary(plugin_id) do
    pattern = {{:_, plugin_id, :"$1"}, :"$2", :_}

    case Registry.select(registry(), [{pattern, [], [:"$$"]}]) do
      [[topic, pid] | _] -> counters(pid, topic)
      [] -> {:error, :no_producer}
    end
  end

  # The Registry unregisters a worker asynchronously when it exits, so the
  # lookup can hand back a pid that is already gone. `ProducerWorker.stats/1`
  # declares the empty map as its refusal, which is the only way that window
  # shows: the call itself never exits.
  defp counters(pid, topic) do
    case ProducerWorker.stats(pid) do
      counters when map_size(counters) == 0 -> {:error, :no_producer}
      counters -> {:ok, Map.put(counters, :topic, topic)}
    end
  end

  # A worker that has given up on this many consecutive batches is
  # reported `:produce_stalled` so the convergence loop can escalate to a
  # plugin restart. The worker self-refreshes its broker client on the
  # same threshold; health flags a stall that a fresh connection did not
  # clear, which is the signal worth escalating.
  @stall_health_threshold 3

  @doc """
  Health verdict for the producer worker (plugin health contract). Healthy
  means the worker pid is alive and not permanently parked — a worker whose
  `last_error` is `{:swap_exhausted, _}` has exhausted its in-process
  broker-swap recovery and the queue cannot drain, and a worker with
  a sustained run of failed batches (`:produce_stalled`) is wedged against a
  healthy-but-unreachable broker, so the convergence loop escalates by
  restarting the provider.
  """
  @impl true
  @spec health(map()) :: Flezha.Plugin.ProviderBehaviour.health_verdict()
  def health(opts) do
    ProviderBehaviour.worker_health(
      opts,
      [absent: :no_producer, dead: :worker_dead],
      &health_from_stats/1
    )
  end

  defp registry(), do: WorkerSup.registry_name(WorkerSup.family(__MODULE__))

  defp health_from_stats(pid) do
    case ProducerWorker.stats(pid) do
      %{last_error: {:swap_exhausted, reason}} ->
        {:unhealthy, {:swap_exhausted, reason}}

      %{consecutive_failures: n} when n >= @stall_health_threshold ->
        {:unhealthy, :produce_stalled}

      %{} ->
        :healthy
    end
  end

  defp convert_value(:auth, %KafkaEx.Auth.Config{} = config), do: {:ok, config}

  defp convert_value(:auth, %{} = map),
    do: Enum.reduce_while(map, {:ok, %{}}, &put_auth_opt/2)

  defp convert_value(:auth, value), do: refuse_value(:auth, value)

  # `KafkaEx.Auth.Config.new/1` takes only these four and `Map.take/2`s the
  # rest away, so a misspelled key is a missing credential rather than an
  # error — and a mechanism outside its own set is an `ArgumentError` raised
  # inside the closure `WorkerRegistry` re-invokes on every client restart.
  # Both sets are the dependency's, closed here, once, at preparation.
  defp put_auth_opt({key, value}, {:ok, acc}) do
    with {:ok, field} <- parse_enum(key, "auth key", @auth_keys),
         {:ok, value} <- auth_value(field, value) do
      {:cont, {:ok, Map.put(acc, field, value)}}
    else
      {:error, _} = err -> {:halt, err}
    end
  end

  defp auth_value(:mechanism, value), do: parse_enum(value, "auth mechanism", @auth_mechanisms)
  defp auth_value(_field, value), do: {:ok, value}

  defp parse_hosts(hosts) when is_binary(hosts) do
    hosts
    |> String.split(",")
    |> Enum.reduce_while({:ok, []}, fn host, {:ok, acc} ->
      with [h, port] <- String.split(host, ":"),
           {port, ""} <- Integer.parse(port) do
        {:cont, {:ok, [{h, port} | acc]}}
      else
        _ -> {:halt, refuse_value(:hosts, hosts)}
      end
    end)
    |> case do
      {:ok, parsed} -> {:ok, Enum.reverse(parsed)}
      refusal -> refusal
    end
  end

  defp parse_hosts(hosts), do: {:ok, hosts}
end
