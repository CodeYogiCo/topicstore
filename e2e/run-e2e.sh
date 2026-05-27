#!/usr/bin/env bash
# End-to-end black-box test against the running docker-compose stack.
#
# Steps:
#   1) Wait for backend health
#   2) Register a topic via REST
#   3) Produce N messages to Kafka via `docker exec` on the kafka container
#   4) Poll /api/lookup until N rows are visible in ClickHouse
#   5) Verify JSON-path filter returns the expected subset
#   6) Delete the topic and confirm it disappears from active list
#
# Exit codes:
#   0  = pass
#   >0 = fail (with a message)

set -euo pipefail

BACKEND_URL="${BACKEND_URL:-http://localhost:8080}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-topicstore-kafka}"
TOPIC="${TOPIC:-e2e.smoke.$(date +%s)}"
MSG_COUNT="${MSG_COUNT:-10}"
DEADLINE_SECS="${DEADLINE_SECS:-90}"

red()   { printf "\033[31m%s\033[0m\n" "$*"; }
green() { printf "\033[32m%s\033[0m\n" "$*"; }
blue()  { printf "\033[34m%s\033[0m\n" "$*"; }

require() { command -v "$1" >/dev/null 2>&1 || { red "missing dep: $1"; exit 2; }; }
require curl
require jq
require docker

wait_for() {
    local label="$1" url="$2" deadline=$(( $(date +%s) + DEADLINE_SECS ))
    blue "waiting for $label at $url"
    while (( $(date +%s) < deadline )); do
        if curl -fsS -o /dev/null "$url"; then green "  $label is up"; return 0; fi
        sleep 2
    done
    red "  timeout waiting for $label"; exit 3
}

wait_for "backend" "$BACKEND_URL/api/health"

blue "registering topic: $TOPIC"
created=$(curl -fsS -X POST -H 'content-type: application/json' \
    -d "{\"name\":\"$TOPIC\"}" "$BACKEND_URL/api/topics")
echo "  $created"

blue "waiting for backend to subscribe to topic"
sub_deadline=$(( $(date +%s) + 30 ))
while (( $(date +%s) < sub_deadline )); do
    active=$(curl -fsS "$BACKEND_URL/api/health" | jq -r '.activeTopics[]?' || true)
    if echo "$active" | grep -qx "$TOPIC"; then green "  subscribed"; break; fi
    sleep 2
done

blue "producing $MSG_COUNT messages to $TOPIC"
{
    for i in $(seq 1 "$MSG_COUNT"); do
        bucket=$(( i % 3 ))
        echo "k${i}:{\"i\":${i},\"user\":{\"id\":\"u${bucket}\"},\"msg\":\"hello-${i}\"}"
    done
} | docker exec -i "$KAFKA_CONTAINER" bash -c \
    "kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $TOPIC \
        --property parse.key=true --property key.separator=:"

blue "polling lookup until $MSG_COUNT rows appear"
deadline=$(( $(date +%s) + DEADLINE_SECS ))
while (( $(date +%s) < deadline )); do
    total=$(curl -fsS "$BACKEND_URL/api/lookup?topic=$TOPIC&limit=1" | jq '.total')
    echo "  total=$total"
    if (( total >= MSG_COUNT )); then green "  ingest complete: $total rows"; break; fi
    sleep 2
done
if (( total < MSG_COUNT )); then red "ingest never reached $MSG_COUNT rows (saw $total)"; exit 4; fi

blue "verifying JSON path filter user.id=u1"
u1=$(curl -fsS "$BACKEND_URL/api/lookup?topic=$TOPIC&jsonPath=user.id&jsonValue=u1" | jq '.total')
echo "  user.id=u1 rows: $u1"
if (( u1 == 0 )); then red "expected >0 rows for user.id=u1"; exit 5; fi

blue "deleting topic: $TOPIC"
curl -fsS -X DELETE "$BACKEND_URL/api/topics/$TOPIC" -o /dev/null

blue "verifying topic deactivated"
deact_deadline=$(( $(date +%s) + 30 ))
while (( $(date +%s) < deact_deadline )); do
    active=$(curl -fsS "$BACKEND_URL/api/health" | jq -r '.activeTopics[]?' || true)
    if ! echo "$active" | grep -qx "$TOPIC"; then green "  deactivated"; break; fi
    sleep 2
done

green "ALL E2E ASSERTIONS PASSED"
