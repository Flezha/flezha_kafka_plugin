# What run.sh and memory_drift.sh share: the plugin e2e library, and the broker
# helpers. Sourced, never run.
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PLUGIN=$(cd "$HERE/../.." && pwd)
CORE=$(cd "$PLUGIN/../.." && pwd)
PACKAGE=flezha_kafka
TMP_PREFIX=flzkafka
# shellcheck source=../../../../test/helpers/plugin_e2e.sh
. "$CORE/test/helpers/plugin_e2e.sh"

# The dev broker as the controller on the host reaches it, and as the broker
# container reaches itself.
BROKER=${KAFKA_E2E_BROKER:-localhost:19092}

# `producer <name> <topic> [X<key>=<value>...]`: a kafka_producer plugin, enabled.
producer() {
  local name=$1 topic=$2
  shift 2
  flz plugin enable "$name" kafka_producer "Xhosts=$BROKER" "Xtopic=$topic" \
    Xauto_create_replication_factor=1 "$@" >>"$TMP/out/enable-$name.log" 2>&1 ||
    { cat "$TMP/out/enable-$name.log"; exit 1; }
}

# Every record value of `topic`, one per line, as a consumer inside the broker
# container reads them back.
topic_values() {
  docker exec flezha_kafka kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic "$1" --from-beginning --timeout-ms "${2:-8000}" 2>/dev/null
}

# The records of `topic` with their headers.
topic_headers() {
  docker exec flezha_kafka kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic "$1" --from-beginning --timeout-ms 8000 --property print.headers=true 2>/dev/null
}

# `await_count <topic> <n>`: wait until the topic holds `n` distinct values.
await_count() {
  until_ok "${3:-60}" "[ \"\$(topic_values $1 4000 | sort -u | wc -l)\" -ge $2 ]"
}
