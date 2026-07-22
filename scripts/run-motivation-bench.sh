#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RESULT_ROOT=""
VM0_PORT=2200
VM1_PORT=2201
SSH_USER=ubuntu
SSH_PASSWORD=123
SAMPLES=1000
WARMUPS=50
WORKERS=4
MODES="persistent,localfork,local-prefork,cxlfork-preopen,cxlfork-per-request"
TEMPLATE_BYTES_LIST=4194304
TEMPLATE_KEY=2026072201
POLICY=mow
VALIDATION=fast
MAP_SIZE=2147483648
PAYLOAD_MAX=4194304
QUEUE_CAPACITY=4096
WORDCOUNT_INPUT=""
WORDCOUNT_EXPECTED=""
WORDCOUNT_MODES="localfork,cxlfork-preopen"
GUEST_BIN=/tmp/mr_hydra
BUILD=1

usage() {
  printf '%s\n' \
    "usage: $0 --output-dir=DIR [options]" \
    "  --samples=N --warmups=N --workers=N" \
    "  --modes=CSV --template-bytes-list=CSV" \
    "  --policy=mow|moa|hybrid|readonly --validation=fast|full" \
    "  --wordcount-input=FILE --wordcount-expected=FILE" \
    "  --wordcount-modes=CSV --no-build --smoke"
}

for arg in "$@"; do
  case "$arg" in
    --output-dir=*) RESULT_ROOT=${arg#*=} ;;
    --samples=*) SAMPLES=${arg#*=} ;;
    --warmups=*) WARMUPS=${arg#*=} ;;
    --workers=*) WORKERS=${arg#*=} ;;
    --modes=*) MODES=${arg#*=} ;;
    --template-bytes-list=*) TEMPLATE_BYTES_LIST=${arg#*=} ;;
    --template-key=*) TEMPLATE_KEY=${arg#*=} ;;
    --policy=*) POLICY=${arg#*=} ;;
    --validation=*) VALIDATION=${arg#*=} ;;
    --vm0-port=*) VM0_PORT=${arg#*=} ;;
    --vm1-port=*) VM1_PORT=${arg#*=} ;;
    --ssh-user=*) SSH_USER=${arg#*=} ;;
    --ssh-password=*) SSH_PASSWORD=${arg#*=} ;;
    --wordcount-input=*) WORDCOUNT_INPUT=${arg#*=} ;;
    --wordcount-expected=*) WORDCOUNT_EXPECTED=${arg#*=} ;;
    --wordcount-modes=*) WORDCOUNT_MODES=${arg#*=} ;;
    --no-build) BUILD=0 ;;
    --smoke)
      SAMPLES=5
      WARMUPS=2
      WORKERS=2
      MODES="persistent,cxlfork-preopen"
      ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$arg" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$RESULT_ROOT" ]] || { usage >&2; exit 2; }
[[ "$SAMPLES" =~ ^[0-9]+$ && "$SAMPLES" -gt 0 ]]
[[ "$WARMUPS" =~ ^[0-9]+$ ]]
[[ "$WORKERS" =~ ^[0-9]+$ && "$WORKERS" -gt 0 ]]
[[ "$POLICY" == mow || "$POLICY" == moa || "$POLICY" == hybrid || "$POLICY" == readonly ]]
[[ "$VALIDATION" == fast || "$VALIDATION" == full ]]
if [[ -n "$WORDCOUNT_INPUT" || -n "$WORDCOUNT_EXPECTED" ]]; then
  [[ -f "$WORDCOUNT_INPUT" && -f "$WORDCOUNT_EXPECTED" ]] || {
    printf 'both WordCount input and expected files must exist\n' >&2
    exit 2
  }
fi

mkdir -p "$RESULT_ROOT/raw" "$RESULT_ROOT/system"
RESULT_ROOT=$(cd "$RESULT_ROOT" && pwd)
BINARY="$ROOT/build-static-cxlfork/mr_hydra"
CXLFORK_ROOT=$(cd "$ROOT/../cxlfork-repro" && pwd)
CXLFORK_UAPI="$CXLFORK_ROOT/kernel/build-6.6.32/usr/include"
HYDRARPC_ROOT=${HYDRARPC_ROOT:-/home/xtl/HydraRPC}

ssh_cmd=(sshpass -p "$SSH_PASSWORD" ssh -o StrictHostKeyChecking=no)
scp_cmd=(sshpass -p "$SSH_PASSWORD" scp -q -o StrictHostKeyChecking=no)

guest() {
  local port=$1
  shift
  "${ssh_cmd[@]}" -p "$port" "$SSH_USER"@127.0.0.1 "$@"
}

copy_to_guest() {
  local port=$1 src=$2 dst=$3
  "${scp_cmd[@]}" -P "$port" "$src" "$SSH_USER"@127.0.0.1:"$dst"
}

copy_from_guest() {
  local port=$1 src=$2 dst=$3
  "${scp_cmd[@]}" -P "$port" "$SSH_USER"@127.0.0.1:"$src" "$dst"
}

stop_agents() {
  guest "$VM0_PORT" "sudo pkill -x mr_hydra 2>/dev/null || true" || true
  guest "$VM1_PORT" "sudo pkill -x mr_hydra 2>/dev/null || true" || true
}

ACTIVE_GENERATION=""
cleanup() {
  stop_agents
  if [[ -n "$ACTIVE_GENERATION" ]]; then
    guest "$VM0_PORT" "sudo '$GUEST_BIN' cxlfork-release '$TEMPLATE_KEY' '$ACTIVE_GENERATION'" \
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
  guest "$port" "uname -a; printf '%s\\n' \"\$(cat /proc/cmdline)\"; sudo dmesg" \
    > "$RESULT_ROOT/system/before-vm${port}.log" 2>&1
done

{
  printf 'format=cxlfork-motivation-run-v1\n'
  printf 'date_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'mr_commit=%s\n' "$(git -C "$ROOT" rev-parse HEAD)"
  printf 'main_commit=%s\n' "$(git -C "$CXLFORK_ROOT" rev-parse HEAD)"
  printf 'kernel_commit=%s\n' "$(git -C "$CXLFORK_ROOT/kernel/linux-6.6.32" rev-parse HEAD)"
  printf 'samples=%s\nwarmups=%s\nworkers=%s\n' "$SAMPLES" "$WARMUPS" "$WORKERS"
  printf 'modes=%s\ntemplate_bytes_list=%s\n' "$MODES" "$TEMPLATE_BYTES_LIST"
  printf 'policy=%s\nvalidation=%s\n' "$POLICY" "$VALIDATION"
  printf 'clock_contract=never subtract absolute CLOCK_MONOTONIC timestamps from different VMs\n'
} > "$RESULT_ROOT/metadata.txt"
printf 'scenario,type,expected_measured,expected_warmup,mode,template_bytes,policy,validation\n' \
  > "$RESULT_ROOT/run-manifest.csv"

publish_template() {
  local bytes=$1 log=$2 out
  out=$(guest "$VM0_PORT" \
    "sudo '$GUEST_BIN' template --role=reduce --key='$TEMPLATE_KEY' --bytes='$bytes' --value-scale=1")
  printf '%s\n' "$out" | tee "$log"
  ACTIVE_GENERATION=$(sed -n 's/.* generation=\([0-9][0-9]*\).*/\1/p' <<< "$out")
  TEMPLATE_CHECKSUM=$(sed -n 's/.* page_checksum=\([0-9][0-9]*\).*/\1/p' <<< "$out")
  TEMPLATE_STATE_CHECKSUM=$(sed -n 's/.* state_checksum=\([0-9][0-9]*\).*/\1/p' <<< "$out")
  [[ -n "$ACTIVE_GENERATION" && -n "$TEMPLATE_CHECKSUM" && -n "$TEMPLATE_STATE_CHECKSUM" ]]
}

release_template() {
  local log=$1
  [[ -n "$ACTIVE_GENERATION" ]] || return 0
  guest "$VM0_PORT" \
    "sudo '$GUEST_BIN' cxlfork-release '$TEMPLATE_KEY' '$ACTIVE_GENERATION'" \
    > "$log"
  if guest "$VM0_PORT" "sudo '$GUEST_BIN' cxlfork-inspect '$TEMPLATE_KEY'" \
      > "${log%.log}-inspect-after.log" 2>&1; then
    printf 'released template is still inspectable: key=%s generation=%s\n' \
      "$TEMPLATE_KEY" "$ACTIVE_GENERATION" >&2
    return 1
  fi
  ACTIVE_GENERATION=""
}

mode_args() {
  case "$1" in
    persistent) printf '%s\t%s' persistent preopen ;;
    localfork) printf '%s\t%s' localfork preopen ;;
    local-prefork) printf '%s\t%s' local-prefork preopen ;;
    cxlfork-preopen) printf '%s\t%s' cxlfork preopen ;;
    cxlfork-per-request) printf '%s\t%s' cxlfork per-request ;;
    *) printf 'unsupported mode: %s\n' "$1" >&2; return 2 ;;
  esac
}

start_agents() {
  local scenario=$1 launch=$2 setup=$3 bytes=$4 scenario_dir=$5
  local reduce_template="--template-bytes='$bytes' --template-value-scale=1"
  if [[ "$launch" == cxlfork ]]; then
    reduce_template+=" --template-key='$TEMPLATE_KEY' --template-generation='$ACTIVE_GENERATION'"
    reduce_template+=" --template-page-checksum='$TEMPLATE_CHECKSUM'"
  fi
  stop_agents
  guest "$VM0_PORT" \
    "sudo rm -rf /tmp/${scenario}-map; sudo sh -c 'nohup $GUEST_BIN agent --backend=ivshmem --path=/sys/bus/pci/devices/0000:00:02.0/resource2 --node-id=0 --agent-count=2 --workers=$WORKERS --role=map --launch=localfork --template-bytes=$bytes --store-dir=/tmp/${scenario}-map --size=$MAP_SIZE --payload-max=$PAYLOAD_MAX --queue-capacity=$QUEUE_CAPACITY --shuffle-connections=$WORKERS --queue-policy=shared_pair --quiet > /tmp/${scenario}-agent0.log 2>&1 &'"
  guest "$VM1_PORT" \
    "sudo rm -rf /tmp/${scenario}-reduce; sudo sh -c 'nohup $GUEST_BIN agent --backend=ivshmem --path=/sys/bus/pci/devices/0000:00:03.0/resource2 --node-id=1 --agent-count=2 --workers=$WORKERS --global-worker-base=$WORKERS --role=reduce --launch=$launch $reduce_template --cxlfork-policy=$POLICY --validation=$VALIDATION --restore-setup=$setup --store-dir=/tmp/${scenario}-reduce --size=$MAP_SIZE --payload-max=$PAYLOAD_MAX --queue-capacity=$QUEUE_CAPACITY --shuffle-connections=$WORKERS --queue-policy=shared_pair --quiet > /tmp/${scenario}-agent1.log 2>&1 &'"
  : > "$scenario_dir/agent0.log"
  : > "$scenario_dir/agent1.log"
}

collect_agent_logs() {
  local scenario=$1 scenario_dir=$2
  copy_from_guest "$VM0_PORT" "/tmp/${scenario}-agent0.log" "$scenario_dir/agent0.log" || true
  copy_from_guest "$VM1_PORT" "/tmp/${scenario}-agent1.log" "$scenario_dir/agent1.log" || true
}

run_probe_scenario() {
  local mode=$1 bytes=$2 parsed launch setup scenario scenario_dir rc=0
  parsed=$(mode_args "$mode")
  launch=${parsed%%$'\t'*}
  setup=${parsed#*$'\t'}
  scenario="probe_${mode//-/_}_${bytes}"
  scenario_dir="$RESULT_ROOT/raw/$scenario"
  mkdir -p "$scenario_dir"
  if [[ "$launch" == cxlfork ]]; then
    publish_template "$bytes" "$scenario_dir/template-publish.log"
  else
    ACTIVE_GENERATION=""
    TEMPLATE_CHECKSUM=0
    TEMPLATE_STATE_CHECKSUM=0
  fi
  start_agents "$scenario" "$launch" "$setup" "$bytes" "$scenario_dir"
  guest "$VM0_PORT" \
    "sudo '$GUEST_BIN' master --backend=ivshmem --agent=/sys/bus/pci/devices/0000:00:02.0/resource2 --agent=/sys/bus/pci/devices/0000:00:03.0/resource2 --workers-per-agent='$WORKERS' --activation-warmups='$WARMUPS' --activation-probes='$SAMPLES' --probe-agent=1 --probe-role=reduce --probe-state-checksum='$TEMPLATE_STATE_CHECKSUM' --size='$MAP_SIZE' --payload-max='$PAYLOAD_MAX' --queue-capacity='$QUEUE_CAPACITY' --connections='$WORKERS' --queue-policy=shared_pair --quiet" \
    > "$scenario_dir/master.log" 2>&1 || rc=$?
  collect_agent_logs "$scenario" "$scenario_dir"
  if [[ "$launch" == cxlfork ]]; then
    release_template "$scenario_dir/template-release.log" || rc=$?
  fi
  printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$scenario" probe "$SAMPLES" "$WARMUPS" \
    "$mode" "$bytes" "$POLICY" "$VALIDATION" >> "$RESULT_ROOT/run-manifest.csv"
  ((rc == 0)) || { printf 'scenario failed: %s\n' "$scenario" >&2; return "$rc"; }
  local got
  got=$(grep -c '^task_result_v3 ' "$scenario_dir/master.log" || true)
  [[ "$got" -eq $((SAMPLES + WARMUPS)) ]] || {
    printf '%s: expected %s task records, got %s\n' "$scenario" "$((SAMPLES + WARMUPS))" "$got" >&2
    return 1
  }
}

run_wordcount_scenario() {
  local mode=$1 bytes=$2 parsed launch setup scenario scenario_dir rc=0
  parsed=$(mode_args "$mode")
  launch=${parsed%%$'\t'*}
  setup=${parsed#*$'\t'}
  scenario="wordcount_${mode//-/_}_${bytes}"
  scenario_dir="$RESULT_ROOT/raw/$scenario"
  mkdir -p "$scenario_dir"
  copy_to_guest "$VM0_PORT" "$WORDCOUNT_INPUT" /tmp/motivation-wordcount-input.txt
  if [[ "$launch" == cxlfork ]]; then
    publish_template "$bytes" "$scenario_dir/template-publish.log"
  else
    ACTIVE_GENERATION=""
    TEMPLATE_CHECKSUM=0
    TEMPLATE_STATE_CHECKSUM=0
  fi
  start_agents "$scenario" "$launch" "$setup" "$bytes" "$scenario_dir"
  guest "$VM0_PORT" \
    "sudo '$GUEST_BIN' master --backend=ivshmem --input=/tmp/motivation-wordcount-input.txt --output=/tmp/${scenario}-output.txt --agent=/sys/bus/pci/devices/0000:00:02.0/resource2 --agent=/sys/bus/pci/devices/0000:00:03.0/resource2 --schedule=role-static --workers-per-agent='$WORKERS' --reducers='$WORKERS' --size='$MAP_SIZE' --payload-max='$PAYLOAD_MAX' --queue-capacity='$QUEUE_CAPACITY' --connections='$WORKERS' --queue-policy=shared_pair --quiet" \
    > "$scenario_dir/master.log" 2>&1 || rc=$?
  collect_agent_logs "$scenario" "$scenario_dir"
  copy_from_guest "$VM0_PORT" "/tmp/${scenario}-output.txt" "$scenario_dir/output.txt" || rc=$?
  if [[ -f "$scenario_dir/output.txt" ]]; then
    LC_ALL=C sort "$scenario_dir/output.txt" > "$scenario_dir/output.sorted"
    LC_ALL=C sort "$WORDCOUNT_EXPECTED" > "$scenario_dir/expected.sorted"
    diff -u "$scenario_dir/expected.sorted" "$scenario_dir/output.sorted" \
      > "$scenario_dir/output.diff" || rc=$?
  fi
  if [[ "$launch" == cxlfork ]]; then
    release_template "$scenario_dir/template-release.log" || rc=$?
  fi
  printf '%s,%s,%s,%s,%s,%s,%s,%s\n' "$scenario" wordcount 0 0 \
    "$mode" "$bytes" "$POLICY" "$VALIDATION" >> "$RESULT_ROOT/run-manifest.csv"
  ((rc == 0)) || { printf 'WordCount scenario failed: %s\n' "$scenario" >&2; return "$rc"; }
}

IFS=, read -r -a byte_values <<< "$TEMPLATE_BYTES_LIST"
IFS=, read -r -a mode_values <<< "$MODES"
for bytes in "${byte_values[@]}"; do
  [[ "$bytes" =~ ^[0-9]+$ && "$bytes" -ge 2097152 && $((bytes % 2097152)) -eq 0 ]]
  for mode in "${mode_values[@]}"; do
    run_probe_scenario "$mode" "$bytes"
  done
done

if [[ -n "$WORDCOUNT_INPUT" ]]; then
  IFS=, read -r -a wordcount_mode_values <<< "$WORDCOUNT_MODES"
  for mode in "${wordcount_mode_values[@]}"; do
    run_wordcount_scenario "$mode" "${byte_values[0]}"
  done
fi

for port in "$VM0_PORT" "$VM1_PORT"; do
  guest "$port" "sudo dmesg" > "$RESULT_ROOT/system/after-vm${port}.log" 2>&1
done
python3 "$ROOT/scripts/analyze-motivation.py" "$RESULT_ROOT"
python3 "$ROOT/scripts/check-motivation-results.py" "$RESULT_ROOT"
trap - EXIT INT TERM
cleanup
printf 'Motivation benchmark complete: %s\n' "$RESULT_ROOT"
