import Config

# Compile flezha's SDK the way the controller release does: a controller dials
# no socket, so the SDK's socket client is neither downloaded nor built.
config :flezha_sdk, socket_nif: false

# Bundled into the archive's `kafka_ex.app`: the providers start their own broker
# clients, so the library starts none of its own. `sync_timeout` bounds every
# request the library's client makes, whatever timeout its caller names. A
# consumer group's join waits at the broker for the rebalance (3 s for a new
# group, `group.initial.rebalance.delay.ms`), which the 1 s default cannot cover;
# the consumer's rebalance timeout is kept below 5 s. While a broker is away
# the producer's worker can answer that much later.
config :kafka_ex,
  disable_default_worker: true,
  kafka_version: "kayrock",
  sync_timeout: 5_000
