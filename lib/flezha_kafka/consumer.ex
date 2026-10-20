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
  `docs/public/plugins/kafka_consumer.md` in the core.
  """

  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Plugin.ProviderBehaviour.Options
  alias FlezhaKafka.Connection
  alias FlezhaKafka.Consumer.Member

  use ProviderBehaviour,
    name: "kafka_consumer",
    shareable: false,
    source: [
      partitioned: :refuse,
      idle_interval: :lag_interval_ms,
      absent: :no_member,
      dead: :member_dead
    ],
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
          ]
        ] ++
        Options.channel(:in, "record") ++
        Options.plugin_id("the id it was enabled under") ++
        Options.failure_threshold() ++ Options.idle_after(),
    worker: [module: Member, restart: :temporary]

  defp convert_value(:hosts, value), do: Connection.convert_hosts(value)
  defp convert_value(:auth, value), do: Connection.convert_auth(value)

  @doc "Validate and prepare. Connects to nothing: the member does, at activation."
  @impl true
  def init_provider(opts), do: Connection.prepare(opts)

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
end
