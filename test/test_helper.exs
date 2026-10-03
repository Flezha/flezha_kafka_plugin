# The suite runs with `--no-start`: no controller boots. What a controller would
# have running for the providers is started here: each provider family's
# supervisor, which the tests start workers under, and the client library.
alias Flezha.Plugin.Provider.WorkerSup

{:ok, _} = Application.ensure_all_started([:telemetry, :kafka_ex, :mimic])

for provider <- [FlezhaKafka.Producer, FlezhaKafka.Consumer] do
  {:ok, family} = WorkerSup.start_link(WorkerSup.family(provider))
  Process.unlink(family)
end

Mimic.copy(KafkaEx)
Mimic.copy(FlezhaKafka.WorkerRegistry)

# The core's own suite runs at this level: a log line a test provokes on purpose
# is captured and asserted where it is.
Logger.configure(level: :error)

# A broker on the dev stack is needed by the integration test alone.
ExUnit.start(exclude: [:kafka_integration])
