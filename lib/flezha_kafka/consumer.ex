defmodule FlezhaKafka.Consumer do
  @moduledoc """
  The `kafka_consumer` provider: one member of the Kafka consumer group
  `group_id` for `topics`, injecting every record of the partitions assigned to
  it as one input on the relay's entry channel.

  Each binding runs one `FlezhaKafka.Consumer.Member`, which starts the group
  and reads its lag; the group runs one `FlezhaKafka.Consumer.Partition` per
  assigned partition, which hands each fetched batch to the relay and commits
  its offsets to Kafka as the relay's input durability says. Several
  relays bound to consumers of one `group_id` split the partitions, on any
  controllers. One group member per binding: not shareable, and refused on a
  partitioned relay, whose members a record's key decides between. Options,
  the input's metadata, delivery, health and telemetry:
  `docs/plugins/kafka_consumer.md` in the core.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.ProviderBehaviour
  alias FlezhaKafka.Connection
  alias FlezhaKafka.Consumer.Member

  use ProviderBehaviour,
    name: "kafka_consumer",
    feeds: true,
    shareable: false,
    telemetry: [rebalanced: :event, lag: :metric],
    options:
      Connection.rows() ++
        [
          topics: [
            type: :names,
            required: true,
            doc: "The topics to consume, comma-separated"
          ],
          group_id: [type: :string, required: true, doc: "The consumer group"],
          channel: [
            type: :names,
            doc:
              "The channel this plugin is attached to: the entry channel every record is injected on, and one name. Required, unless `channel_in` names it"
          ],
          channel_in: [
            type: :names,
            doc:
              "Entry channel every record is injected on, and one name; the explicit spelling of `channel`. Required, unless `channel` names it"
          ],
          plugin_id: [type: :string, doc: "Plugin id; defaults to the id it was enabled under"],
          start_at: [
            type: :enum,
            values: ["beginning", "end"],
            default: "end",
            doc:
              "Where a group with no committed offset starts: the earliest record, or the next one written"
          ],
          max_bytes: [
            type: :int,
            default: 1_048_576,
            min: 1,
            doc: "Bytes one fetch takes from one partition"
          ],
          max_wait_ms: [
            type: :int,
            default: 500,
            min: 0,
            max: 4_000,
            doc: "How long a fetch waits for records; below the client's 5 s request timeout"
          ],
          isolation_level: [
            type: :enum,
            values: [:read_uncommitted, :read_committed],
            default: :read_uncommitted,
            doc:
              "Which records a fetch hands over: every record, or only those of committed transactions and no transaction markers"
          ],
          commit_interval_ms: [
            type: :int,
            default: 5_000,
            min: 1,
            doc:
              "Longest the offsets of taken records wait to be committed, unless the relay's input is durable"
          ],
          lag_interval_ms: [
            type: :int,
            default: 5_000,
            min: 1,
            doc: "How often the lag of each assigned partition is read"
          ],
          failure_threshold: [
            type: :int,
            default: 3,
            min: 1,
            doc: "Refused batches in a row before the plugin reports unhealthy"
          ],
          idle_after_ms: [
            type: :int,
            default: 0,
            min: 0,
            doc:
              "Age of the last settled lag read past which the source is idle; 0 derives it from `lag_interval_ms`"
          ]
        ],
    worker: [module: Member, restart: :temporary]

  defp convert_value(:hosts, value), do: Connection.convert_hosts(value)
  defp convert_value(:auth, value), do: Connection.convert_auth(value)

  @doc "Validate and prepare. Connects to nothing: the member does, at activation."
  @impl true
  def init_provider(opts) do
    with :ok <- ProviderBehaviour.require_entry_channel(opts, __MODULE__),
         :ok <- ProviderBehaviour.refuse_partitioned(opts, __MODULE__),
         {:ok, opts} <- Connection.prepare(opts) do
      {:ok, %{opts | plugin_id: opts.plugin_id || opts[:source_plugin_id]}}
    end
  end

  @doc "One group member per binding, started with `Consumer.Member.args/1`."
  @impl true
  def worker_specs(opts), do: [{opts.plugin_id, Member.args(opts)}]

  @doc """
  Teardown is the host's. This callback exists so the provider can be hooked at
  all: every hook requires the matching export.
  """
  @impl true
  def on_terminate(%Ctx.Terminate{}, _opts), do: :ok

  @doc """
  Health verdict from the member: `failure_threshold` refused batches in a row
  name the refusal, and a member whose last settled lag read is older than the
  idle window (a broker that is away, a group that cannot join) says how old.
  """
  @impl true
  @spec health(map()) :: ProviderBehaviour.health_verdict()
  def health(opts) do
    ProviderBehaviour.worker_health(
      opts,
      # The outer budget exceeds the probe's own, so the precise
      # `:probe_timeout` is not replaced by the generic budget verdict.
      [
        absent: :no_member,
        dead: :member_dead,
        budget_ms: 2 * ProviderBehaviour.probe_budget_ms()
      ],
      &member_health(&1, opts)
    )
  end

  @doc """
  What `flz relay info` shows beneath the plugin: the group, the topics, the
  assignment and lag the last read saw, each partition as `topic/partition`,
  and the verdict.
  """
  @impl true
  def inspect_state(opts) do
    facts =
      with pid when is_pid(pid) <- ProviderBehaviour.worker_pid(opts),
           {:ok, %{assignment: assignment, lag: lag}} <- Member.probe(pid) do
        %{
          assignment: Enum.map(assignment || [], &Member.partition_name/1),
          lag:
            Map.new(lag, fn {partition, value} -> {Member.partition_name(partition), value} end)
        }
      else
        _absent_or_silent -> %{}
      end

    Map.merge(%{group_id: opts.group_id, topics: opts.topics, health: health(opts)}, facts)
  end

  defp member_health(pid, opts) do
    case Member.probe(pid) do
      {:ok, facts} -> verdict(facts, opts)
      :timeout -> {:fail_open, :probe_timeout}
    end
  end

  defp verdict(%{consecutive_failures: failures, last_error: error}, opts)
       when failures >= opts.failure_threshold,
       do: {:unhealthy, {:read_failed, error}}

  defp verdict(%{last_settled_at: settled_at}, opts),
    do: ProviderBehaviour.idle_verdict(settled_at, opts.idle_after_ms, opts.lag_interval_ms)
end
