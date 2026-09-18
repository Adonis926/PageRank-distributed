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

qemu_pid() {
  local name=$1
  pgrep -f -o "qemu-system-x86_64 -name $name "
}

cmdline_has_arg() {
  local pid=$1 expected=$2
  tr '\0' '\n' <"/proc/$pid/cmdline" | grep -Fqx -- "$expected"
}

validate_qemu_platform() {
  local name cpus pid actual_qemu expected_cpu expected_machine expected_smp
  local begin end index tid actual_cpu
  expected_cpu=${PR_CPU_MODEL:?PR_CPU_MODEL is required}
  expected_machine="${PR_MACHINE:?PR_MACHINE is required},accel=kvm,nvdimm=on"
  expected_smp="$PR_VM_SMP,sockets=$PR_VM_SOCKETS,cores=$PR_VM_CORES,threads=$PR_VM_THREADS"

  [[ $PR_VM_SMP -eq $((PR_VM_SOCKETS * PR_VM_CORES * PR_VM_THREADS)) ]] || {
    echo "invalid expected topology: SMP=$PR_VM_SMP but sockets*cores*threads=$((PR_VM_SOCKETS * PR_VM_CORES * PR_VM_THREADS))" >&2
    return 1
  }

  for spec in "vm0:$PR_VM0_HOST_CPUS" "vm1:$PR_VM1_HOST_CPUS"; do
    name=${spec%%:*}
    cpus=${spec#*:}
    pid=$(qemu_pid "$name")
    [[ -n $pid ]] || {
      echo "cannot find QEMU process for $name" >&2
      return 1
    }
    actual_qemu=
    IFS= read -r -d '' actual_qemu <"/proc/$pid/cmdline" || true
    [[ $actual_qemu == "$PR_QEMU" ]] || {
      echo "$name uses unexpected QEMU: $actual_qemu" >&2
      return 1
    }
    cmdline_has_arg "$pid" "$expected_machine" || {
      echo "$name missing exact machine argument: $expected_machine" >&2
      return 1
    }
    cmdline_has_arg "$pid" "$expected_cpu" || {
      echo "$name missing exact CPU argument: $expected_cpu" >&2
      return 1
    }
    cmdline_has_arg "$pid" "$expected_smp" || {
      echo "$name missing exact SMP argument: $expected_smp" >&2
      return 1
    }
    cmdline_has_arg "$pid" "ivshmem-plain,id=hydra-shm0,memdev=hydra0,bus=$PR_HYDRA_PCI_BUS,addr=$PR_HYDRA0_PCI_ADDR" &&
      cmdline_has_arg "$pid" "ivshmem-plain,id=hydra-shm1,memdev=hydra1,bus=$PR_HYDRA_PCI_BUS,addr=$PR_HYDRA1_PCI_ADDR" || {
        echo "$name HydraRPC device bus/BDF does not match the experiment profile" >&2
        return 1
      }

    [[ $cpus =~ ^([0-9]+)-([0-9]+)$ ]] || {
      echo "$name expected CPU range is not contiguous: $cpus" >&2
      return 1
    }
    begin=${BASH_REMATCH[1]}
    end=${BASH_REMATCH[2]}
    [[ $((end - begin + 1)) -eq $PR_VM_SMP ]] || return 1
    for ((index = 0; index < PR_VM_SMP; index++)); do
      tid=$(ps -T -p "$pid" -o tid=,comm= |
        awk -v wanted="qemu-vcpu$index" '$2 == wanted {print $1; exit}')
      [[ -n $tid ]] || {
        echo "missing $name qemu-vcpu$index" >&2
        return 1
      }
      actual_cpu=$(taskset -pc "$tid" | awk -F': ' '{print $2}')
      [[ $actual_cpu == $((begin + index)) ]] || {
        echo "$name qemu-vcpu$index affinity=$actual_cpu expected=$((begin + index))" >&2
        return 1
      }
    done
  done
  echo "pagerank_qemu_platform=ok machine=$PR_MACHINE cpu=$PR_CPU_MODEL topology=${PR_VM_SOCKETS}x${PR_VM_CORES}x${PR_VM_THREADS} pinning=exact"
}

validate_guest_platform() {
  local port name
  for spec in "vm0:$PR_VM0_PORT" "vm1:$PR_VM1_PORT"; do
    name=${spec%%:*}
    port=${spec##*:}
    vm_ssh "$port" "
      set -eu
      test \"\$(nproc)\" -eq '$PR_VM_SMP'
      test \"\$(lscpu -p=SOCKET | awk -F, '\$1 !~ /^#/ {seen[\$1]=1} END {print length(seen)}')\" -eq '$PR_VM_SOCKETS'
      guest_cores=\$(lscpu -p=CORE | awk -F, '\$1 !~ /^#/ {seen[\$1]=1} END {print length(seen)}')
      test \"\$guest_cores\" -eq '$PR_VM_CORES'
      test \"\$(lscpu -p=NODE | awk -F, '\$1 !~ /^#/ && \$1 >= 0 {seen[\$1]=1} END {print length(seen)}')\" -eq '$PR_VM_NUMA_NODES'
      test \"\$(( \$(nproc) / guest_cores ))\" -eq '$PR_VM_THREADS'
      grep -qw waitpkg /proc/cpuinfo
      test \"\$(cat /sys/devices/system/node/online)\" = 0
    " || {
      echo "$name guest topology or WAITPKG validation failed" >&2
      return 1
    }
  done
  echo "pagerank_guest_platform=ok numa_nodes=$PR_VM_NUMA_NODES topology=${PR_VM_SOCKETS}x${PR_VM_CORES}x${PR_VM_THREADS} waitpkg=on"
}

pin_one_vm() {
  local name=$1 pidfile=$2 cpus=$3
  local begin end pid count tid index
  [[ $cpus =~ ^([0-9]+)-([0-9]+)$ ]] || {
    echo "$name CPU list must be one contiguous range, found: $cpus" >&2
    return 1
  }
  begin=${BASH_REMATCH[1]}
  end=${BASH_REMATCH[2]}
  count=$((end - begin + 1))
  [[ $count -eq $PR_VM_SMP ]] || {
    echo "$name CPU range has $count CPUs, expected $PR_VM_SMP" >&2
    return 1
  }
  pid=$(<"$pidfile")
  kill -0 "$pid"
  for ((index = 0; index < PR_VM_SMP; index++)); do
    tid=$(ps -T -p "$pid" -o tid=,comm= |
      awk -v wanted="qemu-vcpu$index" '$2 == wanted {print $1; exit}')
    [[ -n $tid ]] || {
      echo "missing $name qemu-vcpu$index below pid $pid" >&2
      return 1
    }
    taskset -pc $((begin + index)) "$tid" >/dev/null
  done
}

pin_vcpus() {
  [[ ${EUID:-$(id -u)} -eq 0 ]] || {
    echo "pin-vcpus must be run with sudo" >&2
    exit 1
  }
  pin_one_vm vm0 "$PR_VM_WORKSPACE/pids/vm0.pid" "$PR_VM0_HOST_CPUS"
  pin_one_vm vm1 "$PR_VM_WORKSPACE/pids/vm1.pid" "$PR_VM1_HOST_CPUS"
  echo "pagerank_vcpu_pinning=ok vm0=$PR_VM0_HOST_CPUS vm1=$PR_VM1_HOST_CPUS"
}

capture_qemu_topology() {
  local name pid tid comm
  : >"$RESULT_DIR/manifest/qemu-vcpu-affinity.txt"
  for name in vm0 vm1; do
    pid=$(qemu_pid "$name")
    [[ -n $pid ]] || {
      echo "cannot find QEMU process for $name" >&2
      return 1
    }
    printf '%s pid=%s cmdline=' "$name" "$pid" \
      >>"$RESULT_DIR/manifest/qemu-vcpu-affinity.txt"
    tr '\0' ' ' <"/proc/$pid/cmdline" \
      >>"$RESULT_DIR/manifest/qemu-vcpu-affinity.txt"
    printf '\n' >>"$RESULT_DIR/manifest/qemu-vcpu-affinity.txt"
    while read -r tid comm; do
      [[ $comm == qemu-vcpu* ]] || continue
      printf '%s ' "$name/$comm" \
        >>"$RESULT_DIR/manifest/qemu-vcpu-affinity.txt"
      taskset -pc "$tid" >>"$RESULT_DIR/manifest/qemu-vcpu-affinity.txt"
    done < <(ps -T -p "$pid" -o tid=,comm=)
  done
}

build() {
  cmake -S "$PR_HOST_REPO" -B "$PR_HOST_REPO/build-pagerank" \
    -DHYDRARPC_ROOT="$PR_HYDRA_ROOT" \
    -DMR_ENABLE_CXLFORK=ON \
    -DCXLFORK_UAPI_INCLUDE_DIR="$PR_UAPI"
  cmake --build "$PR_HOST_REPO/build-pagerank" -j"$(nproc)"
}

install_guest_binary() {
  validate_qemu_platform
  for port in "$PR_VM0_PORT" "$PR_VM1_PORT"; do
    vm_ssh "$port" "true"
    vm_scp "$port" "$PR_BINARY" ubuntu@127.0.0.1:"$PR_GUEST_BINARY" >/dev/null
    vm_ssh "$port" "chmod 755 '$PR_GUEST_BINARY'; \
      sudo test -e /dev/cxlfork; sudo test -r '$PR_ENDPOINT0'; \
      sudo test -r '$PR_ENDPOINT1'; \
      grep -qw 'cxlfork.cxl_phys=$PR_CXL_PHYS' /proc/cmdline; \
      grep -qw 'cxlfork.cxl_size=$PR_CXL_SIZE' /proc/cmdline; \
      for token in $PR_GUEST_APPEND_EXTRA; do grep -qw \"\$token\" /proc/cmdline; done; \
      test '$PR_NOTIFY' != umwait || grep -qw waitpkg /proc/cpuinfo; \
      test \"\$(nproc)\" -eq '$PR_VM_SMP'"
  done
  validate_guest_platform
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
    "--timeout-ms=$PR_TIMEOUT_MS" \
    "--notify=$PR_NOTIFY"
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
  local host_log=$RESULT_DIR/runs/$label.host.log
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

  local host_begin_ns host_end_ns host_elapsed_ns
  host_begin_ns=$(date +%s%N)
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
  host_end_ns=$(date +%s%N)
  host_elapsed_ns=$((host_end_ns - host_begin_ns))
  printf 'pr_host_v1 label=%s state=%s orchestration_e2e_ns=%s scope=host_ssh_exec_to_both_exit\n' \
    "$label" "$state" "$host_elapsed_ns" >"$host_log"
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
    printf 'qemu=%s\nmachine=%s\ncpu_model=%s\nhydra_pci_bus=%s\n' \
      "$PR_QEMU" "$PR_MACHINE" "$PR_CPU_MODEL" "$PR_HYDRA_PCI_BUS"
    printf 'hydra0_pci_addr=%s\nhydra1_pci_addr=%s\n' \
      "$PR_HYDRA0_PCI_ADDR" "$PR_HYDRA1_PCI_ADDR"
    printf 'guest_sockets=%s\nguest_cores=%s\nguest_threads=%s\nguest_numa_nodes=%s\n' \
      "$PR_VM_SOCKETS" "$PR_VM_CORES" "$PR_VM_THREADS" "$PR_VM_NUMA_NODES"
    printf 'cxlfork_guest_phys=%s\ncxlfork_guest_size=%s\n' \
      "$PR_CXL_PHYS" "$PR_CXL_SIZE"
    printf 'guest_append_extra=%s\n' "$PR_GUEST_APPEND_EXTRA"
    printf 'vm0_host_cpus=%s\nvm1_host_cpus=%s\n' \
      "$PR_VM0_HOST_CPUS" "$PR_VM1_HOST_CPUS"
    printf 'endpoint0=%s\nendpoint1=%s\n' "$PR_ENDPOINT0" "$PR_ENDPOINT1"
    printf 'notify=%s\n' "$PR_NOTIFY"
  } >"$RESULT_DIR/manifest/config.txt"
  uname -a >"$RESULT_DIR/manifest/host-uname.txt"
  lscpu >"$RESULT_DIR/manifest/host-lscpu.txt"
  vm_ssh "$PR_VM0_PORT" "uname -a; cat /proc/cmdline; lscpu; free -h" \
    >"$RESULT_DIR/manifest/vm0.txt"
  vm_ssh "$PR_VM1_PORT" "uname -a; cat /proc/cmdline; lscpu; free -h" \
    >"$RESULT_DIR/manifest/vm1.txt"
  capture_qemu_topology
}

collect_final_state() {
  local port name
  for spec in "vm0:$PR_VM0_PORT" "vm1:$PR_VM1_PORT"; do
    name=${spec%%:*}
    port=${spec##*:}
    vm_ssh "$port" "sudo dmesg" >"$RESULT_DIR/manifest/$name-dmesg.txt"
    if grep -Eiq 'BUG:|WARNING: CPU|Oops:|general protection fault|Kernel panic' \
        "$RESULT_DIR/manifest/$name-dmesg.txt"; then
      echo "fatal kernel diagnostic found in $name dmesg" >&2
      return 1
    fi
  done
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
  grep -q 'readers=0 .* verify=ok' "$RESULT_DIR/final-inspect.log"
  vm_ssh "$PR_VM0_PORT" "sudo '$PR_GUEST_BINARY' release \
    --key='$PR_KEY' --generation='$PR_GENERATION'" \
    >"$RESULT_DIR/release.log"
  grep -q 'verify=ok' "$RESULT_DIR/release.log"
  collect_final_state
  printf 'result_dir=%s\n' "$RESULT_DIR"
}

case "$ACTION" in
  build)
    build
    ;;
  pin-vcpus)
    pin_vcpus
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
    run_once "${PR_RUN_LABEL:-run-1}" off no cxlfork
    ;;
  bench)
    bench
    ;;
  *)
    echo "usage: $0 build|pin-vcpus|prepare|run|bench" >&2
    exit 2
    ;;
esac
