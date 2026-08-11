#!/usr/bin/env bash
set -euo pipefail

# One canonical HydraRPC + CXLfork + WordCount lifecycle benchmark.
# Infrastructure (VMs, DAX mappings and resident agent transport) is outside
# the worker-cold-start interval.  Each cycle creates a new CXLfork template,
# starts a transport-only reducer agent, lets the first real reduce task lazily
# construct the porter/pool, runs subsequent reducers through the same pool,
# then drains the pool and releases the template.

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RESULT_ROOT=""
VM0_PORT=2200
VM1_PORT=2201
SSH_USER=ubuntu
SSH_PASSWORD=123
CYCLES=100
STEADY_TASKS=10
WORKERS=4
TEMPLATE_BYTES=4194304
TEMPLATE_KEY=2026081101
POLICY=mow
VALIDATION=fast
MAP_SIZE=2147483648
PAYLOAD_MAX=4194304
QUEUE_CAPACITY=4096
WORDCOUNT_INPUT=/home/xtl/cxlfork/mr-file-sim-smoke/input/input_0.txt
WORDCOUNT_EXPECTED=/home/xtl/cxlfork/mr-file-sim-smoke/expected.txt
GUEST_BIN=/tmp/mr_hydra_lifecycle
BUILD=1

usage() {
  printf '%s\n' \
    "usage: $0 --output-dir=DIR [options]" \
    "  --cycles=N --steady-tasks=N --workers=N" \
    "  --template-bytes=N --template-key=N" \
    "  --wordcount-input=FILE --wordcount-expected=FILE" \
    "  --vm0-port=N --vm1-port=N --ssh-user=USER --ssh-password=PASS" \
    "  --no-build --smoke"
}

for arg in "$@"; do
  case "$arg" in
    --output-dir=*) RESULT_ROOT=${arg#*=} ;;
    --cycles=*) CYCLES=${arg#*=} ;;
    --steady-tasks=*) STEADY_TASKS=${arg#*=} ;;
    --workers=*) WORKERS=${arg#*=} ;;
    --template-bytes=*) TEMPLATE_BYTES=${arg#*=} ;;
    --template-key=*) TEMPLATE_KEY=${arg#*=} ;;
    --vm0-port=*) VM0_PORT=${arg#*=} ;;
    --vm1-port=*) VM1_PORT=${arg#*=} ;;
    --ssh-user=*) SSH_USER=${arg#*=} ;;
    --ssh-password=*) SSH_PASSWORD=${arg#*=} ;;
    --wordcount-input=*) WORDCOUNT_INPUT=${arg#*=} ;;
    --wordcount-expected=*) WORDCOUNT_EXPECTED=${arg#*=} ;;
    --no-build) BUILD=0 ;;
    --smoke)
      CYCLES=2
      STEADY_TASKS=2
      WORKERS=2
      ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$arg" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$RESULT_ROOT" ]] || { usage >&2; exit 2; }
[[ "$CYCLES" =~ ^[0-9]+$ && "$CYCLES" -gt 0 ]]
[[ "$STEADY_TASKS" =~ ^[0-9]+$ && "$STEADY_TASKS" -gt 0 ]]
[[ "$WORKERS" =~ ^[0-9]+$ && "$WORKERS" -gt 0 ]]
[[ "$TEMPLATE_BYTES" =~ ^[0-9]+$ && "$TEMPLATE_BYTES" -ge 2097152 ]]
((TEMPLATE_BYTES % 2097152 == 0))
[[ -f "$WORDCOUNT_INPUT" && -f "$WORDCOUNT_EXPECTED" ]] || {
  printf 'WordCount input or expected output is missing\n' >&2
  exit 2
}
[[ ! -e "$RESULT_ROOT" ]] || {
  printf 'output directory already exists: %s\n' "$RESULT_ROOT" >&2
  exit 2
}

mkdir -p "$RESULT_ROOT/raw" "$RESULT_ROOT/system"
RESULT_ROOT=$(cd "$RESULT_ROOT" && pwd)
RUN_ID=$(basename "$RESULT_ROOT" | tr -cd 'A-Za-z0-9_-')
[[ -n "$RUN_ID" ]]
BINARY="$ROOT/build-static-cxlfork/mr_hydra"
CXLFORK_ROOT=$(cd "$ROOT/../cxlfork-repro" && pwd)
CXLFORK_UAPI="$CXLFORK_ROOT/kernel/build-6.6.32/usr/include"
HYDRARPC_ROOT=${HYDRARPC_ROOT:-/home/xtl/cxlfork/HydraRPC}
REDUCERS=$((STEADY_TASKS + 1))

ssh_cmd=(sshpass -p "$SSH_PASSWORD" ssh -o BatchMode=no \
  -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)
scp_cmd=(sshpass -p "$SSH_PASSWORD" scp -q -o BatchMode=no \
  -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)

guest() {
  local port=$1
  shift
  "${ssh_cmd[@]}" -p "$port" "$SSH_USER"@127.0.0.1 "$@"
}

copy_to_guest() {
  local port=$1 src=$2 dst=$3
  "${scp_cmd[@]}" -P "$port" "$src" \
    "$SSH_USER"@127.0.0.1:"$dst"
}

copy_from_guest() {
  local port=$1 src=$2 dst=$3
  "${scp_cmd[@]}" -P "$port" \
    "$SSH_USER"@127.0.0.1:"$src" "$dst"
}

stop_agents() {
  guest "$VM0_PORT" "sudo pkill -f '^$GUEST_BIN( |$)' 2>/dev/null || true" || true
  guest "$VM1_PORT" "sudo pkill -f '^$GUEST_BIN( |$)' 2>/dev/null || true" || true
}

wait_agents_exit() {
  local port=$1
  guest "$port" "for i in \$(seq 1 1000); do sudo pgrep -f '^$GUEST_BIN( |$)' >/dev/null || exit 0; sleep 0.01; done; exit 1"
}

ACTIVE_GENERATION=""
cleanup() {
  stop_agents
  if [[ -n "$ACTIVE_GENERATION" ]]; then
    guest "$VM0_PORT" \
      "sudo '$GUEST_BIN' cxlfork-release '$TEMPLATE_KEY' '$ACTIVE_GENERATION'" \
      >> "$RESULT_ROOT/system/cleanup-release.log" 2>&1 || true
    ACTIVE_GENERATION=""
  fi
}
trap cleanup EXIT INT TERM

if ((BUILD)); then
  cmake -S "$ROOT" -B "$ROOT/build-static-cxlfork" \
    -DHYDRARPC_ROOT="$HYDRARPC_ROOT" \
    -DMR_ENABLE_CXLFORK=ON \
    -DCXLFORK_UAPI_INCLUDE_DIR="$CXLFORK_UAPI" \
    > "$RESULT_ROOT/system/build.log" 2>&1
  cmake --build "$ROOT/build-static-cxlfork" -j"$(nproc)" \
    >> "$RESULT_ROOT/system/build.log" 2>&1
fi
[[ -x "$BINARY" ]] || { printf 'binary missing: %s\n' "$BINARY" >&2; exit 1; }

for port in "$VM0_PORT" "$VM1_PORT"; do
  copy_to_guest "$port" "$BINARY" "$GUEST_BIN"
  guest "$port" "sudo chmod 755 '$GUEST_BIN'; sudo '$GUEST_BIN' cxlfork-caps" \
    > "$RESULT_ROOT/system/caps-vm${port}.log"
  guest "$port" "uname -a; cat /proc/cmdline; sudo dmesg" \
    > "$RESULT_ROOT/system/before-vm${port}.log" 2>&1
done

INPUT_GUEST="/tmp/${RUN_ID}-input.txt"
copy_to_guest "$VM0_PORT" "$WORDCOUNT_INPUT" "$INPUT_GUEST"

{
  printf 'format=cxlfork-lifecycle-run-v1\n'
  printf 'date_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'mr_commit=%s\n' "$(git -C "$ROOT" rev-parse HEAD)"
  printf 'mr_dirty=%s\n' "$(git -C "$ROOT" status --porcelain | wc -l)"
  printf 'main_commit=%s\n' "$(git -C "$CXLFORK_ROOT" rev-parse HEAD)"
  printf 'kernel_commit=%s\n' "$(git -C "$CXLFORK_ROOT/kernel/linux-6.6.32" rev-parse HEAD)"
  printf 'cycles=%s\nsteady_tasks_per_cycle=%s\nworkers=%s\n' \
    "$CYCLES" "$STEADY_TASKS" "$WORKERS"
  printf 'reducers_per_job=%s\ntemplate_bytes=%s\n' "$REDUCERS" "$TEMPLATE_BYTES"
  printf 'mode=cxlfork-lifecycle\npolicy=%s\nvalidation=%s\n' "$POLICY" "$VALIDATION"
  printf 'mapper_launch=localfork\nreducer_launch=cxlfork\nporter_start=lazy\nrestore_setup=preopen\n'
  printf 'clock_contract=never subtract absolute CLOCK_MONOTONIC timestamps from different VMs\n'
} > "$RESULT_ROOT/metadata.txt"
printf 'scenario,cycle,expected_cold,expected_steady,reducers,workers,template_bytes\n' \
  > "$RESULT_ROOT/run-manifest.csv"

publish_template() {
  local log=$1 out
  out=$(guest "$VM0_PORT" \
    "sudo '$GUEST_BIN' template --role=reduce --key='$TEMPLATE_KEY' --bytes='$TEMPLATE_BYTES' --value-scale=1")
  printf '%s\n' "$out" | tee "$log"
  ACTIVE_GENERATION=$(sed -n 's/.* generation=\([0-9][0-9]*\).*/\1/p' <<< "$out")
  TEMPLATE_CHECKSUM=$(sed -n 's/.* page_checksum=\([0-9][0-9]*\).*/\1/p' <<< "$out")
  TEMPLATE_STATE_CHECKSUM=$(sed -n 's/.* state_checksum=\([0-9][0-9]*\).*/\1/p' <<< "$out")
  [[ -n "$ACTIVE_GENERATION" && -n "$TEMPLATE_CHECKSUM" && -n "$TEMPLATE_STATE_CHECKSUM" ]]
}

release_template() {
  local directory=$1
  guest "$VM0_PORT" "sudo '$GUEST_BIN' cxlfork-inspect '$TEMPLATE_KEY'" \
    > "$directory/template-inspect-before-release.log"
  guest "$VM0_PORT" \
    "sudo '$GUEST_BIN' cxlfork-release '$TEMPLATE_KEY' '$ACTIVE_GENERATION'" \
    > "$directory/template-release.log"
  if guest "$VM0_PORT" "sudo '$GUEST_BIN' cxlfork-inspect '$TEMPLATE_KEY'" \
      > "$directory/template-inspect-after-release.log" 2>&1; then
    printf 'released template is still inspectable\n' >&2
    return 1
  fi
  ACTIVE_GENERATION=""
}

wait_ready() {
  local port=$1 path=$2
  guest "$port" \
    "for i in \$(seq 1 1000); do sudo test -f '$path' && exit 0; sleep 0.01; done; exit 1"
}

for ((cycle = 0; cycle < CYCLES; cycle++)); do
  printf -v scenario '%s-cycle%03d' "$RUN_ID" "$cycle"
  directory="$RESULT_ROOT/raw/$scenario"
  mkdir -p "$directory"
  stop_agents
  publish_template "$directory/template-publish.log"

  master_ready="/tmp/${scenario}-master.ready"
  master_start="/tmp/${scenario}-master.start"
  map_ready="/tmp/${scenario}-agent0.ready"
  reduce_ready="/tmp/${scenario}-agent1.ready"
  output_guest="/tmp/${scenario}-output.txt"

  guest "$VM0_PORT" \
    "sudo '$GUEST_BIN' master --backend=ivshmem --input='$INPUT_GUEST' --output='$output_guest' --agent=/sys/bus/pci/devices/0000:00:02.0/resource2 --agent=/sys/bus/pci/devices/0000:00:03.0/resource2 --schedule=role-static --workers-per-agent=1 --reducers='$REDUCERS' --ready-file='$master_ready' --start-file='$master_start' --size='$MAP_SIZE' --payload-max='$PAYLOAD_MAX' --queue-capacity='$QUEUE_CAPACITY' --connections='$WORKERS' --queue-policy=shared_pair --quiet" \
    > "$directory/master.log" 2>&1 &
  master_ssh_pid=$!

  wait_ready "$VM0_PORT" "$master_ready"
  guest "$VM0_PORT" \
    "sudo sh -c 'exec $GUEST_BIN agent --backend=ivshmem --path=/sys/bus/pci/devices/0000:00:02.0/resource2 --node-id=0 --agent-count=2 --workers=$WORKERS --role=map --launch=localfork --porter-start=eager --template-bytes=$TEMPLATE_BYTES --ready-file=$map_ready --store-dir=/tmp/${scenario}-map --size=$MAP_SIZE --payload-max=$PAYLOAD_MAX --queue-capacity=$QUEUE_CAPACITY --shuffle-connections=$WORKERS --queue-policy=shared_pair --quiet > /tmp/${scenario}-agent0.log 2>&1 &'"
  guest "$VM1_PORT" \
    "sudo sh -c 'exec $GUEST_BIN agent --backend=ivshmem --path=/sys/bus/pci/devices/0000:00:03.0/resource2 --node-id=1 --agent-count=2 --workers=$WORKERS --global-worker-base=$WORKERS --role=reduce --launch=cxlfork --porter-start=lazy --template-bytes=$TEMPLATE_BYTES --template-value-scale=1 --template-key=$TEMPLATE_KEY --template-generation=$ACTIVE_GENERATION --template-page-checksum=$TEMPLATE_CHECKSUM --cxlfork-policy=$POLICY --validation=$VALIDATION --restore-setup=preopen --ready-file=$reduce_ready --store-dir=/tmp/${scenario}-reduce --size=$MAP_SIZE --payload-max=$PAYLOAD_MAX --queue-capacity=$QUEUE_CAPACITY --shuffle-connections=$WORKERS --queue-policy=shared_pair --quiet > /tmp/${scenario}-agent1.log 2>&1 &'"

  wait_ready "$VM0_PORT" "$map_ready"
  wait_ready "$VM1_PORT" "$reduce_ready"
  guest "$VM0_PORT" "sudo touch '$master_start'"
  wait "$master_ssh_pid"
  wait_agents_exit "$VM0_PORT"
  wait_agents_exit "$VM1_PORT"

  copy_from_guest "$VM0_PORT" "/tmp/${scenario}-agent0.log" "$directory/agent0.log"
  copy_from_guest "$VM1_PORT" "/tmp/${scenario}-agent1.log" "$directory/agent1.log"
  copy_from_guest "$VM0_PORT" "$output_guest" "$directory/output.txt"
  LC_ALL=C sort "$directory/output.txt" > "$directory/output.sorted"
  LC_ALL=C sort "$WORDCOUNT_EXPECTED" > "$directory/expected.sorted"
  diff -u "$directory/expected.sorted" "$directory/output.sorted" \
    > "$directory/output.diff"
  release_template "$directory"
  printf '%s,%s,1,%s,%s,%s,%s\n' "$scenario" "$cycle" \
    "$STEADY_TASKS" "$REDUCERS" "$WORKERS" "$TEMPLATE_BYTES" \
    >> "$RESULT_ROOT/run-manifest.csv"
done

for port in "$VM0_PORT" "$VM1_PORT"; do
  guest "$port" "sudo dmesg" > "$RESULT_ROOT/system/after-vm${port}.log" 2>&1
done

python3 "$ROOT/scripts/analyze-lifecycle.py" "$RESULT_ROOT"
python3 "$ROOT/scripts/check-lifecycle-results.py" "$RESULT_ROOT"
trap - EXIT INT TERM
cleanup
printf 'Lifecycle benchmark complete: %s\n' "$RESULT_ROOT"
