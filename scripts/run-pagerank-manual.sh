#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SCALE=${1:-}
OPTION=${2:-}
PROFILE=${PR_PROFILE_OVERRIDE:-$ROOT/config/pagerank-real-cxl-103-direct-scaling.env}

VM_WORKSPACE=/home/xtl/cxlfork/cxlfork-repro
START_VMS=$VM_WORKSPACE/scripts/start-cxlfork-vms.sh
STOP_VMS=$VM_WORKSPACE/scripts/stop-cxlfork-vms.sh
LOAD_KVM=/home/xtl/start/hpca/load-native-kvm.sh
DAX_ZERO=/home/xtl/start/hpca-cxlfork/bin/dax_range_zero
DAX_DEVICE=/dev/dax0.0
KERNEL=$VM_WORKSPACE/kernel/build-6.6.32/arch/x86/boot/bzImage
RUNNER=$ROOT/scripts/run-pagerank-cxlfork.sh

usage() {
  cat <<'EOF'
usage:
  ./scripts/run-pagerank-manual.sh 8|16|32|64|128
  ./scripts/run-pagerank-manual.sh 8|16|32|64|128 --check

The number is both the total PageRank compute-worker count and the total guest
vCPU count across both VMs. The selected profile defines the relay mode.
EOF
}

case "$SCALE" in
  8|16|32|64|128) ;;
  -h|--help|help|"") usage; exit 0 ;;
  *) usage >&2; exit 2 ;;
esac
[[ -z $OPTION || $OPTION == --check ]] || {
  usage >&2
  exit 2
}

export PR_SCALE=$SCALE
export PR_PROFILE=$PROFILE
# shellcheck source=/dev/null
source "$PROFILE"

STAMP=manual-${PR_RECEIVE_MODE}-${SCALE}u-8g-$(date +%Y%m%d-%H%M%S)
RESULT_DIR=/home/xtl/cxlfork/results/pagerank-cxlfork-hydrarpc/$STAMP

LAUNCH_ENV=(
  "WS=$VM_WORKSPACE"
  "QEMU=$PR_QEMU"
  "MACHINE=$PR_MACHINE"
  "CPU_MODEL=$PR_CPU_MODEL"
  "KERNEL=$KERNEL"
  "GUEST_APPEND_EXTRA=$PR_GUEST_APPEND_EXTRA"
  "CXL_FILE=$DAX_DEVICE"
  "CXL_BACKEND_SIZE=4G"
  "CXL_BACKEND_OFFSET=4G"
  "CXL_BACKEND_ALIGN=2M"
  "CXL_PHYS=$PR_CXL_PHYS"
  "CXL_SIZE=$PR_CXL_SIZE"
  "HYDRA_SHM_FILE=$DAX_DEVICE"
  "HYDRA_SHM_SIZE=4G"
  "HYDRA_BACKEND_OFFSET=8G"
  "HYDRA_ENDPOINT_SIZE=2G"
  "HYDRA_ENDPOINT1_OFFSET=2G"
  "HYDRA_BACKEND_ALIGN=2M"
  "BACKEND_RESET=none"
  "MEM=$PR_VM_MEM"
  "SMP=$PR_VM_SMP"
  "VM_MAXMEM=20G"
  "VM0_HOST_CPUS=$PR_VM0_HOST_CPUS"
  "VM1_HOST_CPUS=$PR_VM1_HOST_CPUS"
  "VM0_MEMORY_NODE=$PR_VM0_MEMORY_NODE"
  "VM1_MEMORY_NODE=$PR_VM1_MEMORY_NODE"
  "HYDRA_PCI_BUS=$PR_HYDRA_PCI_BUS"
  "HYDRA0_PCI_ADDR=$PR_HYDRA0_PCI_ADDR"
  "HYDRA1_PCI_ADDR=$PR_HYDRA1_PCI_ADDR"
  # The scalable profile can use non-contiguous host CPU sets.  The runner
  # performs exact per-vCPU pinning immediately after QEMU starts.
  "VM_PIN_VCPUS=0"
)

require_paths() {
  local path
  for path in "$START_VMS" "$STOP_VMS" "$LOAD_KVM" "$DAX_ZERO" \
      "$KERNEL" "$RUNNER" "$DAX_DEVICE"; do
    [[ -e $path ]] || {
      echo "missing required path: $path" >&2
      return 1
    }
  done
  [[ $(hostname) == Lux ]] || {
    echo "this launcher is pinned to the 10.0.16.103 host (hostname Lux)" >&2
    return 1
  }
  daxctl list -d dax0.0 2>/dev/null |
    grep -Eq '"mode"[[:space:]]*:[[:space:]]*"devdax"' || {
      echo "$DAX_DEVICE is not in devdax mode" >&2
      return 1
    }
}

print_plan() {
  local relay_threads_per_vm=0
  if [[ $PR_RECEIVE_MODE == relay ]]; then
    relay_threads_per_vm=$PR_THREADS
  elif [[ $PR_RECEIVE_MODE == single-relay ]]; then
    relay_threads_per_vm=1
  fi
  cat <<EOF
profile_check=ok
compute_workers_total=$PR_SCALE
compute_workers_per_vm=$PR_THREADS
receive_mode=$PR_RECEIVE_MODE
relay_threads_per_vm=$relay_threads_per_vm
data_lanes_per_vm=$PR_THREADS
guest_vcpus_per_vm=$PR_VM_SMP
guest_memory_per_vm=$PR_VM_MEM
vm0_host_cpus=$PR_VM0_HOST_CPUS
vm1_host_cpus=$PR_VM1_HOST_CPUS
result_dir=$RESULT_DIR
EOF
  env "${LAUNCH_ENV[@]}" "$START_VMS" check
}

require_paths
if [[ $OPTION == --check ]]; then
  print_plan
  exit 0
fi

if (( EUID == 0 )); then
  echo "run this script as xtl, without a leading sudo; it requests sudo itself" >&2
  exit 2
fi

exec 9>/tmp/cxlfork-pagerank-manual.lock
flock -n 9 || {
  echo "another manual CXLfork PageRank run is active" >&2
  exit 1
}

sudo -v
keep_sudo_alive() {
  while sleep 60; do
    sudo -n true || exit
  done
}
keep_sudo_alive &
SUDO_KEEPALIVE_PID=$!
VMS_MAY_BE_RUNNING=0

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  kill "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
  if (( VMS_MAY_BE_RUNNING )); then
    sudo "$STOP_VMS" cxl || true
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

# Stop only VMs recorded by this CXLfork workspace.  Never kill unrelated
# QEMU processes automatically.
sudo "$STOP_VMS" cxl
OTHER_QEMU_FILE=/tmp/cxlfork-pagerank-other-qemu.$$
if pgrep -af '[q]emu-system-x86_64' >"$OTHER_QEMU_FILE"; then
  echo "another QEMU process is running; refusing to disturb it:" >&2
  cat "$OTHER_QEMU_FILE" >&2
  rm -f "$OTHER_QEMU_FILE"
  exit 1
fi
rm -f "$OTHER_QEMU_FILE"

holders=$(sudo fuser "$DAX_DEVICE" 2>/dev/null || true)
if [[ -n $holders ]]; then
  echo "$DAX_DEVICE is already in use by PID(s): $holders" >&2
  exit 1
fi

sudo "$LOAD_KVM"

# Clear exactly 4--12 GiB: CXLfork uses 4--8 GiB and HydraRPC uses 8--12 GiB.
# The safety token is checked by dax_range_zero; this never clears all 64 GiB.
sudo "$DAX_ZERO" "$DAX_DEVICE" 4294967296 8589934592 \
  ZERO-CXLFORK-MR-4G-12G

print_plan
VMS_MAY_BE_RUNNING=1
sudo env "${LAUNCH_ENV[@]}" "$START_VMS" cxl

# The generic launcher binds each QEMU process to its allowed set.  This
# action pins qemu-vcpuN one-to-one and validates non-overlap for 8--64u.
sudo env PR_SCALE="$PR_SCALE" PR_PROFILE="$PROFILE" \
  "$RUNNER" pin-vcpus

SSH_OPTIONS=(
  -i "$PR_SSH_KEY"
  -o BatchMode=yes
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o ConnectTimeout=5
)
wait_for_guest() {
  local port=$1 name=$2 attempt
  for attempt in $(seq 1 120); do
    if ssh "${SSH_OPTIONS[@]}" -p "$port" ubuntu@127.0.0.1 true \
        >/dev/null 2>&1; then
      echo "$name ssh=ready port=$port"
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for $name SSH on port $port" >&2
  return 1
}
wait_for_guest "$PR_VM0_PORT" vm0
wait_for_guest "$PR_VM1_PORT" vm1

PR_STAMP="$STAMP" \
PR_RESULT_DIR="$RESULT_DIR" \
PR_LOCAL_DIAGNOSTIC=0 \
PR_SCALE="$PR_SCALE" \
PR_PROFILE="$PROFILE" \
  "$RUNNER" bench

echo
echo "manual_benchmark=ok"
echo "result_dir=$RESULT_DIR"
echo "summary:"
cat "$RESULT_DIR/summary.csv"
