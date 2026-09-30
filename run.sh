#!/usr/bin/env bash
# Run a local Raft cluster. Ctrl+C stops everything.
#
#   ./run.sh           3 nodes
#   ./run.sh 5         5 nodes
#   ./run.sh 3 fresh   wipe saved state first

set -uo pipefail
cd "$(dirname "$0")"

N=${1:-3}
BASE=9000

if [ "${2:-}" = "fresh" ]; then
  rm -f raft-*.json
  echo "wiped saved state"
fi

PEERS=""
for ((i = 0; i < N; i++)); do
  PEERS="${PEERS}${PEERS:+,}localhost:$((BASE + i))"
done

echo "building..."
go build -o raft . || exit 1

COLORS=(36 32 33 35 34 31)
pids=()

cleanup() {
  trap - INT TERM EXIT
  echo
  echo "stopping..."
  exec 2>/dev/null
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null; done
  pkill -P $$ 2>/dev/null
  wait 2>/dev/null
  exit 0
}
trap cleanup INT TERM EXIT

echo "peers: $PEERS"
echo
for ((i = 0; i < N; i++)); do
  c=${COLORS[$((i % ${#COLORS[@]}))]}
  ./raft -id "$i" -peers "$PEERS" \
    > >(sed "s/^/$(printf '\033[%sm[node %d]\033[0m' "$c" "$i") /") 2>&1 &
  pids+=($!)
  echo "node $i  pid ${pids[$i]}  localhost:$((BASE + i))"
done

echo
echo "kill a node:  kill <pid>     stop all: Ctrl+C"
echo
wait
