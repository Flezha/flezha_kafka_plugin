import Config

# Compile flezha's SDK the way the controller release does: a controller dials
# no socket, so the SDK's socket client is neither downloaded nor built.
config :flezha_sdk, socket_nif: false

# Bundled into the archive's `kafka_ex.app`: the producer starts its own broker
# clients, so the library starts none of its own.
config :kafka_ex, disable_default_worker: true, kafka_version: "kayrock"
