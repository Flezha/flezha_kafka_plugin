import Config

# Compile flezha's SDK the way the controller release does: a controller dials
# no socket, so the SDK's socket client is neither downloaded nor built.
config :flezha_sdk, socket_nif: false

# Bundled into the archive's `kafka_ex.app`: the providers start their own broker
# clients, so the library starts none of its own. `sync_timeout` bounds every
# request that names no timeout of its own. A fetch is one: the broker holds it
# for up to the consumer's `max_wait_ms` (at most 4 s) when no record waits,
# which the 1 s default cannot cover. While a broker is away the producer's
# worker can answer that much later.
config :kafka_ex,
  disable_default_worker: true,
  kafka_version: "kayrock",
  sync_timeout: 5_000
