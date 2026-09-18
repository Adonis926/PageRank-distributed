#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PROFILE=${PR_PROFILE:-$ROOT/config/pagerank-real-cxl-103.env}
# shellcheck source=/dev/null
source "$PROFILE"

ACTION=${1:-bench}
STAMP=${PR_STAMP:-$(date +%Y%m%d-%H%M%S)}
RESULT_DIR=${PR_RESULT_DIR:-/home/xtl/cxlfork/results/pagerank-cxlfork-hydrarpc/$STAMP}
STATE_FILE=$RESULT_DIR/template.env
KEY=${PR_KEY:-$((0x50520000 + 10#$(date +%H%M%S)))}

SSH_OPTIONS=(
  -i "$PR_SSH_KEY"
  -o BatchMode=yes
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o ConnectTimeout=10
)

vm_ssh() {
  local port=$1
  shift
  ssh "${SSH_OPTIONS[@]}" -p "$port" ubuntu@127.0.0.1 "$@"
}

vm_scp() {
  local port=$1
  shift
  scp "${SSH_OPTIONS[@]}" -P "$port" "$@"
}

field() {
  local name=$1 file=$2
  awk -v wanted="$name" '{
    for (i = 1; i <= NF; i++) {
      split($i, pair, "=")
      if (pair[1] == wanted) { print pair[2]; exit }
    }
  }' "$file"
}

build() {
  cmake -S "$PR_HOST_REPO" -B "$PR_HOST_REPO/build-pagerank" \
    -DHYDRARPC_ROOT="$PR_HYDRA_ROOT" \
    -DMR_ENABLE_CXLFORK=ON \
    -DCXLFORK_UAPI_INCLUDE_DIR="$PR_UAPI"
  cmake --build "$PR_HOST_REPO/build-pagerank" -j"$(nproc)"
}

install_guest_binary() {
  for port in "$PR_VM0_PORT" "$PR_VM1_PORT"; do
    vm_ssh "$port" "true"
    vm_scp "$port" "$PR_BINARY" ubuntu@127.0.0.1:"$PR_GUEST_BINARY" >/dev/null
    vm_ssh "$port" "chmod 755 '$PR_GUEST_BINARY'; \
      sudo test -e /dev/cxlfork; sudo test -r '$PR_ENDPOINT0'; \
      sudo test -r '$PR_ENDPOINT1'"
  done
}

common_args() {
  printf '%s ' \
    "--backend=ivshmem" \
    "--vertices-per-node=$PR_VERTICES_PER_NODE" \
    "--degree=$PR_DEGREE" \
    "--remote-degree=$PR_REMOTE_DEGREE" \
    "--iterations=$PR_ITERATIONS" \
    "--chunks=$PR_CHUNKS" \
    "--threads=$PR_THREADS" \
    "--cpus=$PR_GUEST_CPUS" \
    "--map-size=$PR_MAP_SIZE" \
    "--queue-capacity=$PR_QUEUE_CAPACITY" \
    "--payload-max=$PR_PAYLOAD_MAX" \
    "--timeout-ms=$PR_TIMEOUT_MS"
}

prepare_template() {
  mkdir -p "$RESULT_DIR"
  local log=$RESULT_DIR/template.log
  vm_ssh "$PR_VM0_PORT" "sudo '$PR_GUEST_BINARY' template \
    --key='$KEY' --vertices-per-node='$PR_VERTICES_PER_NODE' \
    --degree='$PR_DEGREE' --remote-degree='$PR_REMOTE_DEGREE' \
    --template-base='$PR_TEMPLATE_BASE' --template-bytes='$PR_TEMPLATE_BYTES'" \
    | tee "$log"
  local generation page_checksum graph_checksum
  generation=$(field generation "$log")
  page_checksum=$(field page_checksum "$log")
  graph_checksum=$(field graph_checksum "$log")
  [[ -n "$generation" && -n "$page_checksum" && -n "$graph_checksum" ]]
  printf 'PR_KEY=%q\nPR_GENERATION=%q\nPR_PAGE_CHECKSUM=%q\nPR_GRAPH_CHECKSUM=%q\n' \
    "$KEY" "$generation" "$page_checksum" "$graph_checksum" >"$STATE_FILE"
}

load_template() {
  [[ -f "$STATE_FILE" ]] || {
    echo "missing template state: $STATE_FILE" >&2
    exit 1
  }
  # shellcheck source=/dev/null
  source "$STATE_FILE"
}

run_once() {
  local label=$1 checksum=$2 full_validate=$3 state=${4:-cxlfork}
  local coordinator_log=$RESULT_DIR/runs/$label.coordinator.log
  local worker_log=$RESULT_DIR/runs/$label.worker.log
  local common
  common=$(common_args)
  local state_args="--state=$state"
  if [[ "$state" == cxlfork ]]; then
    state_args+=" --key=$PR_KEY --generation=$PR_GENERATION"
    state_args+=" --page-checksum=$PR_PAGE_CHECKSUM"
    state_args+=" --template-base=$PR_TEMPLATE_BASE --template-bytes=$PR_TEMPLATE_BYTES"
  fi
  local validate_arg=
  [[ "$full_validate" == yes ]] && validate_arg=--full-validate

  vm_ssh "$PR_VM0_PORT" "sudo '$PR_GUEST_BINARY' coordinator \
    --tx-path='$PR_ENDPOINT0' --rx-path='$PR_ENDPOINT1' \
    $common $state_args --checksum='$checksum'" >"$coordinator_log" 2>&1 &
  local coordinator_pid=$!
  vm_ssh "$PR_VM1_PORT" "sudo '$PR_GUEST_BINARY' worker \
    --tx-path='$PR_ENDPOINT1' --rx-path='$PR_ENDPOINT0' \
    $common $state_args --checksum='$checksum' $validate_arg" >"$worker_log" 2>&1 &
  local worker_pid=$!
  local coordinator_status=0 worker_status=0
  wait "$coordinator_pid" || coordinator_status=$?
  wait "$worker_pid" || worker_status=$?
  if (( coordinator_status != 0 || worker_status != 0 )); then
    echo "run failed label=$label coordinator=$coordinator_status worker=$worker_status" >&2
    sed -n '1,160p' "$coordinator_log" >&2
    sed -n '1,160p' "$worker_log" >&2
    return 1
  fi
  grep -q 'pr_run_v1 status=ok .* verify=ok' "$coordinator_log"
  grep -q 'pr_worker_v1 status=ok .* verify=ok' "$worker_log"
  if [[ "$state" == cxlfork ]]; then
    vm_ssh "$PR_VM0_PORT" "sudo '$PR_GUEST_BINARY' inspect \
      --key='$PR_KEY' --generation='$PR_GENERATION'" \
      >"$RESULT_DIR/runs/$label.inspect.log"
  fi
}

collect_manifest() {
  mkdir -p "$RESULT_DIR/runs" "$RESULT_DIR/manifest"
  {
    printf 'timestamp=%s\n' "$(date --iso-8601=seconds)"
    printf 'mr_git=%s\n' "$(git -C "$PR_HOST_REPO" rev-parse HEAD)"
    printf 'hydra_git=%s\n' "$(git -C "$PR_HYDRA_ROOT" rev-parse HEAD)"
    printf 'binary_sha256=%s\n' "$(sha256sum "$PR_BINARY" | awk '{print $1}')"
    printf 'vertices_per_node=%s\ndegree=%s\nremote_degree=%s\n' \
      "$PR_VERTICES_PER_NODE" "$PR_DEGREE" "$PR_REMOTE_DEGREE"
    printf 'iterations=%s\nchunks=%s\nthreads_per_node=%s\n' \
      "$PR_ITERATIONS" "$PR_CHUNKS" "$PR_THREADS"
    printf 'guest_memory=%s\nguest_vcpus=%s\n' "$PR_VM_MEM" "$PR_VM_SMP"
    printf 'vm0_host_cpus=%s\nvm1_host_cpus=%s\n' \
      "$PR_VM0_HOST_CPUS" "$PR_VM1_HOST_CPUS"
    printf 'endpoint0=%s\nendpoint1=%s\n' "$PR_ENDPOINT0" "$PR_ENDPOINT1"
  } >"$RESULT_DIR/manifest/config.txt"
  uname -a >"$RESULT_DIR/manifest/host-uname.txt"
  lscpu >"$RESULT_DIR/manifest/host-lscpu.txt"
  vm_ssh "$PR_VM0_PORT" "uname -a; cat /proc/cmdline; lscpu; free -h" \
    >"$RESULT_DIR/manifest/vm0.txt"
  vm_ssh "$PR_VM1_PORT" "uname -a; cat /proc/cmdline; lscpu; free -h" \
    >"$RESULT_DIR/manifest/vm1.txt"
}

bench() {
  build
  install_guest_binary
  collect_manifest
  prepare_template
  load_template
  for ((warmup = 1; warmup <= PR_WARMUPS; warmup++)); do
    run_once "warmup-$warmup" on yes cxlfork
  done
  for ((run = 1; run <= PR_RUNS; run++)); do
    run_once "run-$run" off no cxlfork
  done
  if [[ ${PR_LOCAL_DIAGNOSTIC:-1} == 1 ]]; then
    run_once "diagnostic-local" off no local
  fi
  "$ROOT/scripts/analyze-pagerank.py" "$RESULT_DIR"
  vm_ssh "$PR_VM0_PORT" "sudo '$PR_GUEST_BINARY' inspect \
    --key='$PR_KEY' --generation='$PR_GENERATION'" \
    >"$RESULT_DIR/final-inspect.log"
  vm_ssh "$PR_VM0_PORT" "sudo '$PR_GUEST_BINARY' release \
    --key='$PR_KEY' --generation='$PR_GENERATION'" \
    >"$RESULT_DIR/release.log"
  printf 'result_dir=%s\n' "$RESULT_DIR"
}

case "$ACTION" in
  build)
    build
    ;;
  prepare)
    build
    install_guest_binary
    collect_manifest
    prepare_template
    ;;
  run)
    install_guest_binary
    load_template
    mkdir -p "$RESULT_DIR/runs"
    run_once run-1 off no cxlfork
    ;;
  bench)
    bench
    ;;
  *)
    echo "usage: $0 build|prepare|run|bench" >&2
    exit 2
    ;;
esac
