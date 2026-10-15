# flezha_kafka

The Kafka plugin for Flezha: a plugin package, `flezha_kafka`, with two providers.
`kafka_producer` produces the records passing through a relay to a Kafka topic. Every
relay bound to one plugin on a controller casts its records to one worker per topic, which
chooses each record's partition as Kafka's Java client does (`murmur2` of the key), batches,
compresses with gzip, retries, keeps an idempotent producer session per partition, and confirms
or parks a delivery-guaranteed output. `kafka_consumer` joins a consumer group for its topics
and injects every record of its share of the partitions as one input, committing the group's
offsets as the relay's input durability says. Both are built on the `kafka_ex` client.

The contracts (options, partitioning, headers, delivery, commits, health, telemetry) are the
core's pages `docs/plugins/kafka_producer.md` and `docs/plugins/kafka_consumer.md`; this
README covers the repository.

## Layout

```
lib/flezha_kafka/connection.ex              the hosts, SASL and TLS options both providers share
lib/flezha_kafka/producer.ex                the provider: options, validation, the worker it declares
lib/flezha_kafka/producer_worker.ex         the worker: one per topic, its partition lanes, routing
lib/flezha_kafka/producer_worker/lanes.ex   the lane held in the worker's state and the parked ones
lib/flezha_kafka/producer_worker/window.ex  one lane's queue, batches, in-flight window and retries
lib/flezha_kafka/producer_worker/client.ex  the broker client: acquire, swap after a death, replace
lib/flezha_kafka/producer_worker/wire.ex    Produce requests, records, headers, broker answers
lib/flezha_kafka/worker_registry.ex         the broker clients shared by identity, refcounted
lib/flezha_kafka/consumer.ex                the consumer provider: options, refusals, health
lib/flezha_kafka/consumer/member.ex         one group member per binding: the group, its lag
lib/flezha_kafka/consumer/partition.ex      one partition's batches: inputs, verdicts, commits
test/flezha_kafka/                          unit tests against a scripted client, the integration
                                            test against a broker, the catalogue and docs checks
test/support/                               the scripted client and the stand-ins for a controller
test/e2e/                                   the e2e runner (an Elixir SDK runner), run.sh and
                                            memory_drift.sh
```

## Building and testing

The project depends on the Flezha core as a path dependency: it is the core's submodule at
`plugins/flezha_kafka_plugin`, so the core is `../..`. Nothing is published before go-live.
`mix.lock` pins every library the core also ships to the core's version, which the packaging
task checks.

```
mix deps.get
RUSTLER_SKIP_COMPILATION=true mix compile --warnings-as-errors
RUSTLER_SKIP_COMPILATION=true mix test
RUSTLER_SKIP_COMPILATION=true mix format --check-formatted
```

`mix test` runs with `--no-start`: no controller boots, and the tests start the providers'
worker families and stand in for the broker and the relay. The catalogue test fails when a
code the producer refuses with is missing from the core's `flz_transport/errors.json`, and the
docs test when the option table of `docs/plugins/kafka_producer.md` or
`docs/plugins/kafka_consumer.md` differs from its provider's declaration, printing the table to
put there.

The integration tests need the core's dev broker, which advertises `localhost:19092` to the
host:

```
docker compose -f ../../docker/dev/docker-compose.yml up -d zookeeper kafka
KAFKA_INTEGRATION=1 RUSTLER_SKIP_COMPILATION=true mix test --include kafka_integration test/flezha_kafka/integration_test.exs test/flezha_kafka/consumer_integration_test.exs
```

Each pauses the broker for a while (`docker compose pause kafka`) and unpauses it, even when it
fails.

## Packaging

On the host, with the same OTP and Elixir as the controller release:

```
MIX_ENV=prod mix deps.get
MIX_ENV=prod RUSTLER_SKIP_COMPILATION=true mix flezha.plugin.package
```

The archive is `_build/flezha_kafka-0.1.0.fpz`. It bundles `kafka_ex`, `kayrock`, `varint`
and `connection`, with `kafka_ex` configured (`config/config.exs`) to start no worker of its
own and to bound each request that names no timeout of its own by a `sync_timeout` of 5 s.
A consumer's fetch is one: the broker holds it for up to `max_wait_ms` (at most 4 s) when no
record waits, which the 1 s default cannot cover; a group's join and sync name their own.
While a broker is away the producer's worker can answer a stats call that much later; it
loses nothing, and its pause test reads the budget from this config. It carries no
native code: the checksum library `crc32cer` the Kafka record format needs is shipped by the
controller release.

## Installing into a local release

```
flz plugin install _build/flezha_kafka-0.1.0.fpz
flz controller stop && flz controller start
flz plugin list --installed
```

Then enable `kafka_producer` or `kafka_consumer` on a relay as its page shows.

## The end-to-end run

`test/e2e/run.sh` checks the package against a controller release built from the core
(`MIX_ENV=prod mix release flezha --path /tmp/flzrel` there, then
`FLEZHA_RELEASE=/tmp/flzrel test/e2e/run.sh`; with no `FLEZHA_RELEASE` it refuses to run) and
the dev broker (`docker compose -f ../../docker/dev/docker-compose.yml up -d kafka`, which
listens for the host on `localhost:19092`). It first starts the controller without the
package and checks the provider is unknown, then packages the plugin, installs it under a
temporary plugin root, starts the controller and the e2e runner, and checks:
`kafka_ex` started no worker of its own and waits 5 s per request; a plain relay consumes
100 records of one topic and produces them to another; 100 outputs of an at-least-once relay and of an
effectively-once one are 100 records, only the latter with its identity headers; two relays
bound to one plugin produce through one worker; and an effectively-once relay loses
nothing across a 10 s `docker compose pause kafka`; and an effectively-once relay consuming
a topic loses no record across a killed and restarted controller. It prints PASS or FAIL per check, keeps
its logs when one fails, and exits non-zero on any failure.

`test/e2e/memory_drift.sh` runs the same release under a steady Kafka workload for
`MEMORY_DRIFT_DURATION_MIN` minutes, optionally with a websocket source
(`MEMORY_DRIFT_WORKLOAD=combined`), and fails when the controller's memory grows faster than
`MEMORY_DRIFT_MAX_MB_PER_HOUR`.

## Licence

Apache-2.0, see `LICENSE`.
