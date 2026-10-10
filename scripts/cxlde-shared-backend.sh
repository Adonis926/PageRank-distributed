#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
RESERVED_PHYS_START=${RESERVED_DRAM_PHYS_START:-0x5040000000}
RESERVED_SIZE=${RESERVED_DRAM_SIZE:-64G}
REAL_CXL_PHYS_START=${REAL_CXL_PHYS_START:-0x6040000000}
REAL_CXL_SIZE=${REAL_CXL_SIZE:-64G}
ZERO_SOURCE=$ROOT/tools/dax_range_zero.c
ZERO_BINARY=${CXLFORK_DAX_ZERO_BINARY:-$ROOT/build-tools/dax_range_zero}
ZERO_OFFSET=$((4 * 1024 * 1024 * 1024))
ZERO_LENGTH=$((8 * 1024 * 1024 * 1024))
ZERO_TOKEN=ZERO-CXLFORK-MR-4G-12G

die() {
  echo "cxlde-shared-backend: $*" >&2
  exit 1
}

size_to_bytes() {
  numfmt --from=iec "$1"
}

canonical_backend() {
  case "$1" in
    auto)
      if grep -qw "memmap=64G!$RESERVED_PHYS_START" /proc/cmdline; then
        echo reserved-dram
      else
        echo real-cxl
      fi
      ;;
    dram|reserved-dram) echo reserved-dram ;;
    cxl|real-cxl) echo real-cxl ;;
    *) die "backend must be auto, reserved-dram, or real-cxl" ;;
  esac
}

mapping_start() {
  local path=/sys/bus/dax/devices/$1/mapping0/start
  if [[ -r $path ]]; then
    cat "$path"
  elif [[ $(readlink -f "/sys/bus/dax/devices/$1") == */e820_pmem/* ]]; then
    # A label-less e820 namespace reserves a 2 MiB metadata prefix.
    printf '0x%x\n' "$((RESERVED_PHYS_START + 2 * 1024 * 1024))"
  else
    printf '0x%x\n' "$((REAL_CXL_PHYS_START))"
  fi
}

mapping_end() {
  local path=/sys/bus/dax/devices/$1/mapping0/end
  if [[ -r $path ]]; then
    cat "$path"
  elif [[ $(readlink -f "/sys/bus/dax/devices/$1") == */e820_pmem/* ]]; then
    printf '0x%x\n' "$((RESERVED_PHYS_START + $(size_to_bytes "$RESERVED_SIZE") - 1))"
  else
    printf '0x%x\n' "$((REAL_CXL_PHYS_START + $(size_to_bytes "$REAL_CXL_SIZE") - 1))"
  fi
}

discover_reserved_dax() {
  local path name start end limit reserved_bytes
  local -a matches=()
  reserved_bytes=$(size_to_bytes "$RESERVED_SIZE")
  limit=$((RESERVED_PHYS_START + reserved_bytes - 1))
  for path in /sys/bus/dax/devices/dax*; do
    [[ -d $path ]] || continue
    name=${path##*/}
    [[ $(readlink -f "/sys/bus/dax/devices/$name") == */e820_pmem/* ]] || continue
    start=$(( $(mapping_start "$name") ))
    end=$(( $(mapping_end "$name") ))
    if (( start >= RESERVED_PHYS_START && end == limit )); then
      matches+=("$name")
    fi
  done
  (( ${#matches[@]} == 1 )) ||
    die "expected one e820 devdax mapping inside $RESERVED_PHYS_START+$RESERVED_SIZE; found ${#matches[@]}"
  echo "${matches[0]}"
}

discover_real_cxl_dax() {
  local path name start end limit cxl_bytes
  local -a matches=()
  cxl_bytes=$(size_to_bytes "$REAL_CXL_SIZE")
  limit=$((REAL_CXL_PHYS_START + cxl_bytes - 1))
  for path in /sys/bus/dax/devices/dax*; do
    [[ -d $path ]] || continue
    name=${path##*/}
    [[ $(readlink -f "/sys/bus/dax/devices/$name") == */hmem.*/* ]] || continue
    start=$(( $(mapping_start "$name") ))
    end=$(( $(mapping_end "$name") ))
    if (( start == REAL_CXL_PHYS_START && end == limit )); then
      matches+=("$name")
    fi
  done
  (( ${#matches[@]} == 1 )) ||
    die "expected one CXL DAX mapping at $REAL_CXL_PHYS_START+$REAL_CXL_SIZE; found ${#matches[@]}"
  echo "${matches[0]}"
}

discover_namespace() {
  local path namespace start
  local -a matches=()
  for path in /sys/bus/nd/devices/namespace*/resource; do
    [[ -r $path ]] || continue
    [[ $(readlink -f "${path%/resource}") == */e820_pmem/* ]] || continue
    start=$(( $(<"$path") ))
    if (( start == RESERVED_PHYS_START )); then
      namespace=${path#/sys/bus/nd/devices/}
      namespace=${namespace%%/*}
      matches+=("$namespace")
    fi
  done
  (( ${#matches[@]} == 1 )) ||
    die "expected one e820 namespace at $RESERVED_PHYS_START; found ${#matches[@]}"
  echo "${matches[0]}"
}

discover_dax() {
  case "$1" in
    reserved-dram) discover_reserved_dax ;;
    real-cxl) discover_real_cxl_dax ;;
  esac
}

validate_physical_identity() {
  local backend=$1 name=$2 start end expected_start expected_end expected_size
  [[ ${EUID:-$(id -u)} -eq 0 ]] || return 0
  start=$(( $(<"/sys/bus/dax/devices/$name/mapping0/start") ))
  end=$(( $(<"/sys/bus/dax/devices/$name/mapping0/end") ))
  if [[ $backend == reserved-dram ]]; then
    expected_start=$((RESERVED_PHYS_START))
    expected_size=$(size_to_bytes "$RESERVED_SIZE")
    expected_end=$((expected_start + expected_size - 1))
    (( start >= expected_start && end == expected_end )) ||
      die "$name mapping does not match reserved DRAM range"
    [[ $(readlink -f "/sys/bus/dax/devices/$name") == */e820_pmem/* ]] ||
      die "$name is not backed by the e820 memmap namespace"
  else
    expected_start=$((REAL_CXL_PHYS_START))
    expected_size=$(size_to_bytes "$REAL_CXL_SIZE")
    expected_end=$((expected_start + expected_size - 1))
    (( start == expected_start && end == expected_end )) ||
      die "$name mapping does not match the real CXL range"
  fi
}

require_idle() {
  local active
  active=$(pgrep -af '[q]emu-system.*x86_64' || true)
  [[ -z $active ]] || die "QEMU is active; stop it before preparing a shared backend:\n$active"
}

prepare_reserved() {
  local namespace namespace_size expected_size name driver
  command -v ndctl >/dev/null || die "ndctl is required"
  modprobe nd_pmem
  modprobe dax_pmem
  udevadm settle
  namespace=$(discover_namespace)
  namespace_size=$(<"/sys/bus/nd/devices/$namespace/size")
  expected_size=$(size_to_bytes "$RESERVED_SIZE")
  (( namespace_size == expected_size )) ||
    die "$namespace size=$namespace_size, expected $expected_size"

  name=$(discover_reserved_dax 2>/dev/null || true)
  if [[ -n $name ]]; then
    driver=$(readlink -f "/sys/bus/dax/devices/$name/driver" 2>/dev/null || true)
  else
    driver=
  fi
  if [[ $driver != */device_dax ]]; then
    ndctl create-namespace --force --reconfig="$namespace" \
      --mode=devdax --map=mem --align="${RESERVED_DRAM_DAX_ALIGN:-4096}" >/dev/null
    udevadm settle
  fi
  discover_reserved_dax >/dev/null
}

resolve_backend() {
  local backend=$1 name sysfs device driver size align start end
  name=$(discover_dax "$backend")
  sysfs=/sys/bus/dax/devices/$name
  device=/dev/$name
  driver=$(readlink -f "$sysfs/driver" 2>/dev/null || true)
  [[ $driver == */device_dax ]] || {
    if [[ $backend == real-cxl ]]; then
      die "$name is not in devdax mode; use the preserved real-CXL dax-admin enter flow"
    fi
    die "$name is not in devdax mode; run '$0 prepare reserved-dram' after stopping QEMU"
  }
  [[ -c $device ]] || die "missing devdax character device $device"
  size=$(<"$sysfs/size")
  align=$(<"$sysfs/align")
  start=$(mapping_start "$name")
  end=$(mapping_end "$name")
  validate_physical_identity "$backend" "$name"
  (( size >= ZERO_OFFSET + ZERO_LENGTH )) ||
    die "$device is too small for the locked [4,12) GiB layout"
  case "$align" in
    4096|2097152) ;;
    *) die "$device has unsupported mmap alignment $align" ;;
  esac
  printf 'export CXLFORK_SHARED_BACKEND=%q\n' "$backend"
  printf 'export CXLFORK_DAX_DEVICE=%q\n' "$device"
  printf 'export CXLFORK_DAX_NAME=%q\n' "$name"
  printf 'export CXLFORK_DAX_ALIGN=%q\n' "$align"
  printf 'export CXLFORK_DAX_SIZE=%q\n' "$size"
  printf 'export CXLFORK_DAX_PHYS_START=%q\n' "$start"
  printf 'export CXLFORK_DAX_PHYS_END=%q\n' "$end"
}

build_zero_helper() {
  command -v gcc >/dev/null || die "gcc is required"
  mkdir -p "$(dirname "$ZERO_BINARY")"
  if [[ ! -x $ZERO_BINARY || $ZERO_SOURCE -nt $ZERO_BINARY ]]; then
    gcc -O2 -Wall -Wextra -Werror -std=c11 "$ZERO_SOURCE" -o "$ZERO_BINARY"
  fi
}

show_status() {
  local backend name driver
  backend=$(canonical_backend "${1:-auto}")
  echo "selected_backend=$backend"
  echo "kernel=$(uname -r)"
  echo "cmdline=$(cat /proc/cmdline)"
  name=$(discover_dax "$backend" 2>/dev/null || true)
  if [[ -z $name ]]; then
    echo "dax_device=unresolved"
    return
  fi
  driver=$(readlink -f "/sys/bus/dax/devices/$name/driver" 2>/dev/null || true)
  echo "dax_device=/dev/$name"
  echo "dax_driver=${driver:-unbound}"
  echo "dax_align=$(cat "/sys/bus/dax/devices/$name/align" 2>/dev/null || echo unavailable)"
  echo "dax_size=$(cat "/sys/bus/dax/devices/$name/size" 2>/dev/null || echo unavailable)"
  echo "phys_start=$(mapping_start "$name")"
  echo "phys_end=$(mapping_end "$name")"
}

usage() {
  echo "usage: $0 {status|resolve|prepare|zero} [auto|reserved-dram|real-cxl]" >&2
  exit 2
}

command=${1:-}
requested=${2:-auto}
backend=$(canonical_backend "$requested")
case "$command" in
  status)
    show_status "$requested"
    ;;
  resolve)
    resolve_backend "$backend"
    ;;
  prepare)
    [[ ${EUID:-$(id -u)} -eq 0 ]] || die "prepare must run through sudo"
    require_idle
    if [[ $backend == reserved-dram ]]; then
      prepare_reserved
    fi
    resolve_backend "$backend"
    ;;
  zero)
    [[ ${EUID:-$(id -u)} -eq 0 ]] || die "zero must run through sudo"
    require_idle
    eval "$(resolve_backend "$backend")"
    if fuser "$CXLFORK_DAX_DEVICE" >/dev/null 2>&1; then
      fuser -v "$CXLFORK_DAX_DEVICE" >&2 || true
      die "$CXLFORK_DAX_DEVICE has active holders"
    fi
    build_zero_helper
    "$ZERO_BINARY" "$CXLFORK_DAX_DEVICE" "$ZERO_OFFSET" "$ZERO_LENGTH" "$ZERO_TOKEN"
    ;;
  *) usage ;;
esac
