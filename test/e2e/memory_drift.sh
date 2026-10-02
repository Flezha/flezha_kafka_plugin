#!/usr/bin/env bash
# Controller memory under a steady Kafka workload: the load test the core ran as
# its `kafka` and `combined` workloads before the producer became this package.
# A release with the package, the relay runner, one operator whose every input
# the producer publishes (`on_input`) and, for `combined`, a websocket source
# feeding it; inputs at a fixed rate; the BEAM's total memory sampled over the
# run and fitted to a line. Prints the samples and the slope, and fails when the
# slope exceeds MEMORY_DRIFT_MAX_MB_PER_HOUR.
#
#   MEMORY_DRIFT_WORKLOAD          kafka | combined (default kafka)
#   MEMORY_DRIFT_DURATION_MIN      minutes to run (default 30)
#   MEMORY_DRIFT_SAMPLE_S          seconds between samples (default 10)
#   MEMORY_DRIFT_RATE              inputs per second sent with `flz` (default 20)
#   MEMORY_DRIFT_WS_URI            the websocket `combined` subscribes to
#   MEMORY_DRIFT_WS_SUBSCRIBE      the message it sends on connect, as JSON
#   MEMORY_DRIFT_MAX_MB_PER_HOUR   the slope that fails the run (default 5)
set -uo pipefail
. "$(dirname "$0")/lib.sh"

WORKLOAD=${MEMORY_DRIFT_WORKLOAD:-kafka}
DURATION_S=$((${MEMORY_DRIFT_DURATION_MIN:-30} * 60))
SAMPLE_S=${MEMORY_DRIFT_SAMPLE_S:-10}
RATE=${MEMORY_DRIFT_RATE:-20}
MAX_SLOPE=${MEMORY_DRIFT_MAX_MB_PER_HOUR:-5}

cleanup() {
  [ -n "${SENDER:-}" ] && kill "$SENDER" 2>/dev/null
  stop_runner runner
  stop_controller
  rm -rf "$TMP"
}
trap cleanup EXIT

controller_env "$TMP/c1" "flzdrift$RUN" "$REL"
say_release "$REL"
package_plugin
install_plugin
start_controller
build_relay
start_runner runner default
start_operator echo drift

producer drift_out "memory_drift_$RUN" Xlinger_ms=10 Xqueue_max=10000
flz plugin bind drift_out --operator_id drift --hook=on_input >/dev/null 2>&1 ||
  { say "the bind failed"; exit 1; }

if [ "$WORKLOAD" = combined ]; then
  [ -n "${MEMORY_DRIFT_WS_URI:-}" ] || { say "combined needs MEMORY_DRIFT_WS_URI"; exit 1; }
  flz plugin enable drift_ws operator_websocket "Xuri=$MEMORY_DRIFT_WS_URI" \
    "Xpost_connect={\"payload\":{\"type\":\"text\",\"message\":${MEMORY_DRIFT_WS_SUBSCRIBE:-\"{}\"}}}" \
    --operator_id drift >/dev/null 2>&1 || { say "the websocket source did not enable"; exit 1; }
fi

say "workload=$WORKLOAD duration=${DURATION_S}s sample=${SAMPLE_S}s rate=${RATE}/s"
(
  i=0
  while :; do
    i=$((i + 1))
    flz operator send drift --text "$(printf 'payload-%064d' "$i")" >/dev/null 2>&1
    sleep "$(python3 -c "print(1 / $RATE)")"
  done
) &
SENDER=$!

SAMPLES="$TMP/out/samples.tsv"
END=$(($(date +%s) + DURATION_S))
while [ "$(date +%s)" -lt "$END" ]; do
  mem=$(flzvm rpc 'IO.puts(:erlang.memory(:total))' 2>/dev/null | tail -1)
  printf '%s\t%s\n' "$(date +%s)" "$mem" | tee -a "$SAMPLES"
  sleep "$SAMPLE_S"
done

slope=$(python3 - "$SAMPLES" <<'PY'
import sys
rows = [l.split("\t") for l in open(sys.argv[1]) if l.strip()]
xs = [float(r[0]) for r in rows if r[1].strip().isdigit()]
ys = [int(r[1]) / 2**20 for r in rows if r[1].strip().isdigit()]
n = len(xs)
mx, my = sum(xs) / n, sum(ys) / n
den = sum((x - mx) ** 2 for x in xs) or 1
print(f"{sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den * 3600:.2f}")
PY
)
if python3 -c "import sys; sys.exit(0 if $slope <= $MAX_SLOPE else 1)"; then
  pass drift "memory slope $slope MB/h over ${DURATION_S}s (limit $MAX_SLOPE)"
else
  fail drift "memory slope $slope MB/h over ${DURATION_S}s (limit $MAX_SLOPE)"
fi
exit "$FAILED"
