#!/usr/bin/env bash

set -euo pipefail

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

now_ms() {
  date +%s%3N
}

normalize_wordcount() {
  awk 'NF >= 2 { print $1 "\t" $2 }' "$1" | LC_ALL=C sort > "$2"
}

usage() {
  cat <<'USAGE'
usage: mr-distributed/scripts/run-bench.sh [options]

Required:
  --input-dir=DIR
  --expected=FILE
  --output-dir=DIR
  --input-bytes=N

Local file-backend mode:
  --mode=local
  --local-workers=N

QEMU/SSH control-plane mode:
  --mode=ssh
  --master-host=HOST
  --worker=HOST:MASTER_BACKEND_PATH:WORKER_BACKEND_PATH
  --ssh-user=USER
  --ssh-key=PATH
  --remote-dir=DIR
  --remote-repo=DIR
  --remote-hydrarpc-root=DIR
  --no-remote-build

Common distributed options:
  --repo=DIR
  --backend=file|devdax|ivshmem
  --reducers=N
  --payload-max=N
  --map-chunk=N
  --queue-capacity=N
  --repeat=N
  --warmup=N

This script does not start QEMU VMs by default. It uses SSH to start commands
inside already-running VMs and leaves HydraRPC backend paths as the data plane.
USAGE
}

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$SCRIPT_DIR/.." && pwd)
input_dir=""
expected=""
output_dir=""
input_bytes=0
repeat=3
warmup=1
mode=local
local_workers=2
master_host=local
workers=()
backend=file
reducers=""
payload_max=1048576
map_chunk=""
queue_capacity=4096
ssh_user=""
ssh_key=""
remote_dir="/tmp/mr-bench"
remote_repo=""
remote_hydrarpc_root=""
remote_build=1

while (($#)); do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --repo=*) repo=${1#*=} ;;
    --input-dir=*) input_dir=${1#*=} ;;
    --expected=*) expected=${1#*=} ;;
    --output-dir=*) output_dir=${1#*=} ;;
    --input-bytes=*) input_bytes=${1#*=} ;;
    --repeat=*) repeat=${1#*=} ;;
    --warmup=*) warmup=${1#*=} ;;
    --mode=*) mode=${1#*=} ;;
    --local-workers=*) local_workers=${1#*=} ;;
    --master-host=*) master_host=${1#*=} ;;
    --worker=*) workers+=("${1#*=}") ;;
    --backend=*) backend=${1#*=} ;;
    --reducers=*) reducers=${1#*=} ;;
    --payload-max=*) payload_max=${1#*=} ;;
    --map-chunk=*) map_chunk=${1#*=} ;;
    --queue-capacity=*) queue_capacity=${1#*=} ;;
    --ssh-user=*) ssh_user=${1#*=} ;;
    --ssh-key=*) ssh_key=${1#*=} ;;
    --remote-dir=*) remote_dir=${1#*=} ;;
    --remote-repo=*) remote_repo=${1#*=} ;;
    --remote-hydrarpc-root=*) remote_hydrarpc_root=${1#*=} ;;
    --no-remote-build) remote_build=0 ;;
    *) die "unknown argument: $1" ;;
  esac
  shift
done

[[ -n "$input_dir" && -d "$input_dir" ]] || die "--input-dir is required"
[[ -n "$expected" && -f "$expected" ]] || die "--expected is required"
[[ -n "$output_dir" ]] || die "--output-dir is required"
[[ "$mode" == local || "$mode" == ssh ]] || die "invalid --mode=$mode"
mkdir -p "$output_dir"/logs

mapfile -t input_files < <(find "$input_dir" -maxdepth 1 -type f -name '*.txt' | LC_ALL=C sort)
((${#input_files[@]} > 0)) || die "no input files found in $input_dir"
combined_input="$output_dir/combined-input.txt"
cat "${input_files[@]}" > "$combined_input"

ssh_target() {
  local host=$1
  if [[ -n "$ssh_user" && "$host" != *@* ]]; then
    printf '%s@%s' "$ssh_user" "$host"
  else
    printf '%s' "$host"
  fi
}

ssh_opts=()
if [[ -n "$ssh_key" ]]; then
  ssh_opts+=(-i "$ssh_key")
fi

remote_cmd() {
  local host=$1
  shift
  if [[ "$host" == local || "$host" == localhost || -z "$host" ]]; then
    bash -lc "$*"
  else
    ssh "${ssh_opts[@]}" "$(ssh_target "$host")" "$*"
  fi
}

remote_copy_to() {
  local src=$1
  local host=$2
  local dest=$3
  if [[ "$host" == local || "$host" == localhost || -z "$host" ]]; then
    mkdir -p "$(dirname "$dest")"
    cp -R "$src" "$dest"
  else
    scp "${ssh_opts[@]}" -r "$src" "$(ssh_target "$host"):$dest"
  fi
}

remote_copy_from() {
  local host=$1
  local src=$2
  local dest=$3
  if [[ "$host" == local || "$host" == localhost || -z "$host" ]]; then
    cp "$src" "$dest"
  else
    scp "${ssh_opts[@]}" "$(ssh_target "$host"):$src" "$dest"
  fi
}

build_local() {
  local start end
  start=$(now_ms)
  cmake -S "$repo" -B "$repo/build" > "$output_dir/logs/build.log" 2>&1
  cmake --build "$repo/build" >> "$output_dir/logs/build.log" 2>&1
  end=$(now_ms)
  echo $((end - start))
}

make_worker_paths_local() {
  local run_id=$1
  local paths=()
  local i
  for ((i = 0; i < local_workers; i++)); do
    paths+=("/tmp/mr-bench-${run_id}-worker${i}.raw")
  done
  (IFS=,; printf '%s' "${paths[*]}")
}

run_local_once() {
  local phase=$1
  local index=$2
  local master_output=$3
  local run_id="${phase}-${index}-$$"
  local worker_paths
  worker_paths=$(make_worker_paths_local "$run_id")

  start=$(now_ms)
  "$repo/build/mr_hydra" master \
    --backend="$backend" \
    --input="$combined_input" \
    --output="$master_output" \
    --workers="$worker_paths" \
    --payload-max="$payload_max" \
    --queue-capacity="$queue_capacity" \
    ${reducers:+--reducers="$reducers"} \
    ${map_chunk:+--map-chunk="$map_chunk"} \
    --quiet > "$output_dir/logs/${phase}_${index}_master.log" 2>&1 &
  master_pid=$!

  sleep 0.2
  IFS=, read -r -a path_array <<< "$worker_paths"
  worker_pids=()
  local i
  for ((i = 0; i < ${#path_array[@]}; i++)); do
    "$repo/build/mr_hydra" worker \
      --backend="$backend" \
      --path="${path_array[$i]}" \
      --worker-id="$i" \
      --payload-max="$payload_max" \
      --queue-capacity="$queue_capacity" \
      --quiet > "$output_dir/logs/${phase}_${index}_worker${i}.log" 2>&1 &
    worker_pids+=("$!")
  done

  local rc=0
  wait "$master_pid" || rc=$?
  for pid in "${worker_pids[@]}"; do
    wait "$pid" || rc=$?
  done
  end=$(now_ms)
  rm -f "${path_array[@]}"
  echo "$((end - start)):$rc"
}

parse_worker_spec() {
  local spec=$1
  local host=${spec%%:*}
  local rest=${spec#*:}
  local master_path=${rest%%:*}
  local worker_path=${rest#*:}
  [[ "$host" != "$spec" && "$master_path" != "$rest" && -n "$worker_path" ]] ||
    die "worker spec must be HOST:MASTER_BACKEND_PATH:WORKER_BACKEND_PATH"
  printf '%s\t%s\t%s\n' "$host" "$master_path" "$worker_path"
}

prepare_remote_host() {
  local host=$1
  local repo_on_host=$2
  remote_cmd "$host" "mkdir -p '$remote_dir/input' '$remote_dir/output' '$remote_dir/logs'"
  if ((remote_build)); then
    remote_cmd "$host" "cmake -S '$repo_on_host' -B '$repo_on_host/build' ${remote_hydrarpc_root:+-DHYDRARPC_ROOT='$remote_hydrarpc_root'} && cmake --build '$repo_on_host/build'"
  fi
}

run_ssh_once() {
  local phase=$1
  local index=$2
  local local_output=$3
  local repo_on_master=${remote_repo:-$repo}
  local remote_input="$remote_dir/input/combined-input.txt"
  local remote_output="$remote_dir/output/${phase}_${index}.out"

  prepare_remote_host "$master_host" "$repo_on_master"
  remote_copy_to "$combined_input" "$master_host" "$remote_input"

  master_paths=()
  worker_hosts=()
  worker_paths=()
  local spec parsed host master_path worker_path
  for spec in "${workers[@]}"; do
    parsed=$(parse_worker_spec "$spec")
    host=$(printf '%s' "$parsed" | awk -F '\t' '{print $1}')
    master_path=$(printf '%s' "$parsed" | awk -F '\t' '{print $2}')
    worker_path=$(printf '%s' "$parsed" | awk -F '\t' '{print $3}')
    master_paths+=("$master_path")
    worker_hosts+=("$host")
    worker_paths+=("$worker_path")
    prepare_remote_host "$host" "${remote_repo:-$repo}"
  done

  local worker_arg
  worker_arg=$(IFS=,; printf '%s' "${master_paths[*]}")

  start=$(now_ms)
  remote_cmd "$master_host" "'$repo_on_master/build/mr_hydra' master --backend='$backend' --input='$remote_input' --output='$remote_output' --workers='$worker_arg' --payload-max='$payload_max' --queue-capacity='$queue_capacity' ${reducers:+--reducers='$reducers'} ${map_chunk:+--map-chunk='$map_chunk'} --quiet > '$remote_dir/logs/${phase}_${index}_master.log' 2>&1" &
  master_pid=$!

  sleep 0.5
  worker_pids=()
  local i
  for ((i = 0; i < ${#worker_hosts[@]}; i++)); do
    remote_cmd "${worker_hosts[$i]}" "'${remote_repo:-$repo}/build/mr_hydra' worker --backend='$backend' --path='${worker_paths[$i]}' --worker-id='$i' --payload-max='$payload_max' --queue-capacity='$queue_capacity' --quiet > '$remote_dir/logs/${phase}_${index}_worker${i}.log' 2>&1" &
    worker_pids+=("$!")
  done

  local rc=0
  wait "$master_pid" || rc=$?
  for pid in "${worker_pids[@]}"; do
    wait "$pid" || rc=$?
  done
  end=$(now_ms)
  remote_copy_from "$master_host" "$remote_output" "$local_output"
  echo "$((end - start)):$rc"
}

if [[ "$mode" == ssh && ${#workers[@]} -eq 0 ]]; then
  die "--mode=ssh requires at least one --worker"
fi

build_start=$(now_ms)
if [[ "$mode" == local ]]; then
  build_ms=$(build_local)
else
  build_ms=0
fi
build_end=$(now_ms)
((build_ms == 0)) && build_ms=$((build_end - build_start))

total_runs=$((warmup + repeat))
run_csv="$output_dir/runs.csv"
printf 'phase,index,run_ms,correct\n' > "$run_csv"

run_values=()
status=ok
correctness=ok

for ((i = 0; i < total_runs; i++)); do
  if ((i < warmup)); then
    phase=warmup
    logical_index=$i
  else
    phase=repeat
    logical_index=$((i - warmup))
  fi

  raw="$output_dir/${phase}_${logical_index}.out"
  norm="$output_dir/${phase}_${logical_index}.norm"
  if [[ "$mode" == local ]]; then
    result=$(run_local_once "$phase" "$logical_index" "$raw")
  else
    result=$(run_ssh_once "$phase" "$logical_index" "$raw")
  fi
  run_ms=${result%%:*}
  rc=${result#*:}

  if [[ "$rc" != 0 ]]; then
    status=failed
    correctness=failed
  fi

  normalize_wordcount "$raw" "$norm"
  if diff -u "$expected" "$norm" > "$output_dir/logs/${phase}_${logical_index}.diff"; then
    correct=yes
  else
    correct=no
    status=failed
    correctness=failed
  fi

  printf '%s,%s,%s,%s\n' "$phase" "$logical_index" "$run_ms" "$correct" >> "$run_csv"
  [[ "$phase" == repeat ]] && run_values+=("$run_ms")
done

read -r run_ms_avg run_ms_min run_ms_max < <(
  printf '%s\n' "${run_values[@]}" |
    awk 'NR == 1 {min = max = sum = $1} NR > 1 {if ($1 < min) min = $1; if ($1 > max) max = $1; sum += $1} END {if (NR == 0) print "0 0 0"; else printf "%.3f %d %d\n", sum / NR, min, max}'
)

throughput_mib_s=$(awk -v bytes="$input_bytes" -v ms="$run_ms_avg" 'BEGIN { if (ms <= 0) print "0"; else printf "%.3f", bytes / 1048576 / (ms / 1000) }')
worker_count=$([[ "$mode" == local ]] && printf '%s' "$local_workers" || printf '%s' "${#workers[@]}")
details="mode=${mode};backend=${backend};workers=${worker_count};reducers=${reducers:-$worker_count};payload_max=${payload_max}"

cat > "$output_dir/result.env" <<EOF
impl_name=distributed
status=$status
correctness=$correctness
build_ms=$build_ms
run_ms_avg=$run_ms_avg
run_ms_min=$run_ms_min
run_ms_max=$run_ms_max
throughput_mib_s=$throughput_mib_s
input_bytes=$input_bytes
details='$details'
EOF

cat > "$output_dir/result.json" <<EOF
{
  "impl": "distributed",
  "status": "$status",
  "correctness": "$correctness",
  "build_ms": $build_ms,
  "run_ms_avg": $run_ms_avg,
  "run_ms_min": $run_ms_min,
  "run_ms_max": $run_ms_max,
  "throughput_mib_s": $throughput_mib_s,
  "mode": "$mode",
  "backend": "$backend",
  "workers": $worker_count,
  "reducers": ${reducers:-$worker_count},
  "input_bytes": $input_bytes
}
EOF

[[ "$status" == ok ]] || exit 1
