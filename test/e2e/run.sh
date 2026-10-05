#!/usr/bin/env bash
# The Kafka package's producer and consumer against a real controller release and
# the core's dev broker: it packages the plugin, installs it into a release under a temporary
# plugin root, starts the controller and the e2e runner, and checks
# what a user would see. PASS or FAIL per check; exits non-zero on any failure.
#
#   docker compose -f ../../docker/dev/docker-compose.yml up -d kafka   # in the core
#   MIX_ENV=prod mix release flezha --path /tmp/flzrel                  # in the core
#   FLEZHA_RELEASE=/tmp/flzrel test/e2e/run.sh
set -uo pipefail
. "$(dirname "$0")/lib.sh"

cleanup() {
  local code=$?
  "${COMPOSE[@]}" unpause kafka >/dev/null 2>&1
  stop_runner runner
  stop_controller
  if [ "$FAILED" -eq 0 ] && [ "$code" -eq 0 ]; then rm -rf "$TMP"; else say "logs kept in $TMP/out"; fi
}
trap cleanup EXIT

controller_env "$TMP/c1" "flzkafka$RUN" "$REL"
say_release "$REL"
until_ok 60 "docker exec flezha_kafka kafka-topics --bootstrap-server localhost:9092 --list" ||
  { say "the dev broker does not answer"; exit 1; }

# ---------- without the package ----------

check_z() {
  local out
  out=$(flz plugin enable nope kafka_producer "Xhosts=$BROKER" Xtopic=t 2>&1)
  if [ $? -ne 0 ] && grep -q "kafka_producer" <<<"$out" && ! ls "$REL/lib" | grep -q '^kafka_ex-'; then
    pass z "without the package the provider is unknown and the release carries no kafka_ex: $(grep -m1 -i error <<<"$out")"
  else
    fail z "enable without the package answered: $out"
  fi
}

say "starting the controller without the package"
start_controller_bare() {
  (cd "$FLEZHA_CTL_DIR" && nohup flzvm start </dev/null >>"$TMP/out/controller-bare.log" 2>&1 &)
  until_ok 90 "flz controller status" || { say "the controller did not come up"; exit 1; }
}
start_controller_bare
check_z
stop_controller

# ---------- with it ----------

package_plugin
install_plugin
say "starting the controller with the package"
start_controller
build_runner
start_runner runner default

check_a() {
  local env
  env=$(flzvm rpc 'IO.inspect({Application.get_env(:kafka_ex, :disable_default_worker), Supervisor.which_children(KafkaEx.Supervisor), Application.get_env(:kafka_ex, :sync_timeout)})' 2>&1)
  if [ "$env" = "{true, [], 5000}" ] && ls "$REL/lib" | grep -q '^crc32cer-'; then
    pass a "the package loaded, kafka_ex started no worker of its own and waits 5 s per request, crc32cer is the release's"
  else
    fail a "kafka_ex at boot: $env"
  fi
}

check_b() {
  local topic=e2e_plain_$RUN
  start_relay echo plain
  producer plain_out "$topic"
  bind plain_out plain
  send_all plain 1 100 plain
  if await_count "$topic" 100; then
    pass b "100 outputs of an at-least-once relay are 100 records of $topic"
  else
    fail b "$topic holds $(topic_values "$topic" | sort -u | wc -l) of 100"
  fi
}

check_c() {
  local topic=e2e_once_$RUN headers
  start_relay echo_once once
  producer once_out "$topic" 'Xheaders={"source":"e2e"}'
  bind once_out once
  send_all once 1 100 once
  headers=$(topic_headers "$topic" | head -1)
  if await_count "$topic" 100 && grep -q "flezha.dedup_key:" <<<"$headers" &&
    grep -q "flz.epoch:" <<<"$headers" && grep -q "source:e2e" <<<"$headers"; then
    pass c "100 outputs of an effectively-once relay, each with its identity headers: ${headers:0:160}"
  else
    fail c "$topic holds $(topic_values "$topic" | sort -u | wc -l) of 100; headers: $headers"
  fi
}

check_d() {
  local topic=e2e_shared_$RUN workers
  start_relay echo share_a
  start_relay echo share_b
  producer shared_out "$topic"
  bind shared_out share_a
  bind shared_out share_b
  send_all share_a 1 50 a
  send_all share_b 1 50 b
  workers=$(flzvm rpc 'IO.inspect(length(Registry.select(FlezhaKafka.Producer.WorkerSup.Registry, [{{{:_, "shared_out", :_}, :_, :_}, [], [true]}])))' 2>&1)
  if await_count "$topic" 100 && [ "$workers" = 1 ]; then
    pass d "two relays bound to one plugin produce through one worker: 100 records"
  else
    fail d "$topic holds $(topic_values "$topic" | sort -u | wc -l) of 100, $workers worker(s)"
  fi
}

check_e() {
  local topic=e2e_pause_$RUN
  start_relay echo_once paused
  producer pause_out "$topic"
  bind pause_out paused
  send_all paused 1 20 p
  await_count "$topic" 20 || { fail e "the first 20 never arrived"; return; }
  say "pausing the broker for 10 s"
  "${COMPOSE[@]}" pause kafka >/dev/null 2>&1
  send_all paused 21 70 p
  sleep 10
  "${COMPOSE[@]}" unpause kafka >/dev/null 2>&1
  if await_count "$topic" 70 120; then
    pass e "an effectively-once relay lost nothing across a 10 s broker pause: 70 distinct records"
  else
    fail e "$topic holds $(topic_values "$topic" | sort -u | wc -l) of 70 after the pause"
  fi
}

# ---------- the consumer ----------

# `consumer <name> <topic> <relay>`: a kafka_consumer plugin reading `topic`
# from its beginning onto the relay's entry channel `in`.
consumer() {
  flz plugin enable "$1" kafka_consumer "Xhosts=$BROKER" "Xtopics=$2" "Xgroup_id=e2e_$1_$RUN" \
    Xchannel=in Xstart_at=beginning >>"$TMP/out/enable-$1.log" 2>&1 &&
    flz plugin bind "$1" --relay_id "$3" --hook=on_terminate >>"$TMP/out/bind-$1.log" 2>&1 ||
    { cat "$TMP/out/enable-$1.log" "$TMP/out/bind-$1.log"; exit 1; }
}

# `feed <topic> <from> <to> <prefix>`: one record per number, written by a
# producer inside the broker container.
feed() {
  seq "$2" "$3" | sed "s/^/$4-/" |
    docker exec -i flezha_kafka kafka-console-producer --bootstrap-server localhost:9092 --topic "$1" \
      >/dev/null 2>&1
}

check_f() {
  local in=e2e_cin_$RUN out=e2e_cout_$RUN
  feed "$in" 1 100 f
  start_relay echo pass_plain
  producer pass_plain_out "$out"
  bind pass_plain_out pass_plain
  consumer pass_plain_in "$in" pass_plain
  if await_count "$out" 100 &&
    until_ok 30 "grep -q 'assignment: $in/0' <<<\"\$(flz relay info pass_plain)\""; then
    pass f "a plain relay consumed 100 records of $in into $out; relay info: $(flz relay info pass_plain | grep -m1 'lag:')"
  else
    fail f "$out holds $(topic_values "$out" | sort -u | wc -l) of 100; $(flz relay info pass_plain 2>&1 | tail -3)"
  fi
}

check_g() {
  local in=e2e_kin_$RUN out=e2e_kout_$RUN
  feed "$in" 1 1000 g
  start_relay echo_once pass_once
  producer pass_once_out "$out"
  bind pass_once_out pass_once
  consumer pass_once_in "$in" pass_once
  await_count "$out" 1 || { fail g "nothing reached $out"; return; }
  say "killing the controller mid-way"
  pkill -KILL -f "beam.smp.* $FLEZHA_NODE_NAME@"
  feed "$in" 1001 2000 g
  start_controller
  if await_count "$out" 2000 180; then
    pass g "an effectively-once relay lost nothing across a controller kill: 2000 distinct records of $in in $out"
  else
    fail g "$out holds $(topic_values "$out" | sort -u | wc -l) of 2000 after the kill"
  fi
}

check_a
check_f
check_b
check_c
check_d
check_e
check_g

[ "$FAILED" -eq 0 ] && say "all checks passed"
exit "$FAILED"
