#!/usr/bin/env bash

set -euo pipefail

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

now_ms() {
  date +%s%3N
}

now_ns() {
  date +%s%N
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
  --master-host=HOST[:PORT]
  --worker=HOST:MASTER_BACKEND_PATH:AGENT_ENDPOINT_PATH
  --worker=[HOST:PORT]:MASTER_BACKEND_PATH:AGENT_ENDPOINT_PATH
  --worker=HOST,MASTER_BACKEND_PATH,AGENT_ENDPOINT_PATH
  --worker=HOST,MASTER_PATH,AGENT_PATH,ROLE,LAUNCH,WORKERS
  --ssh-user=USER
  --ssh-key=PATH
  --remote-dir=DIR
  --remote-repo=DIR
  --remote-hydrarpc-root=DIR
  --no-remote-build
  --remote-binary=PATH
  --deploy-binary=LOCAL_PATH
  --sudo

Common distributed options:
  --repo=DIR
  --backend=file|devdax|ivshmem
  --reducers=N
  --schedule=static|role-static|file:PATH
  --payload-max=N
  --map-chunk=N
  --queue-capacity=N
  --size=N
  --workers-per-agent=N
  --shuffle-connections=N
  --queue-policy=shared_pair|per_connection
  --verify=last|all|none
  --repeat=N
  --warmup=N
  --template-base=N
  --template-bytes=N
  --template-seed=N
  --template-value-scale=N
  --map-template-key=N --map-template-generation=N --map-template-checksum=N
  --reduce-template-key=N --reduce-template-generation=N --reduce-template-checksum=N
  --prepare-reduce-template-key=N
  --cxlfork-policy=readonly|mow|moa|hybrid
  --validation=fast|full
  --ready-timeout=N

This script does not start QEMU VMs by default. It uses SSH to start commands
inside already-running VMs and leaves HydraRPC backend paths as the measured
agent endpoint/data plane.
Prefer the comma-separated --worker form for devdax/ivshmem paths, because PCI
BDF paths contain ':' characters. Remote configure/build output is copied into
logs/prepare_<host>.log instead of stdout. For devdax, --size must match the
mapped device size.
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
workers_per_agent=1
master_host=local
workers=()
backend=file
shuffle_backend=file
reducers=""
schedule=static
payload_max=1048576
map_chunk=""
queue_capacity=4096
map_size=67108864
shuffle_connections=1
queue_policy=shared_pair
verify=last
ssh_user=""
ssh_key=""
remote_dir="/tmp/mr-bench"
remote_repo=""
remote_hydrarpc_root=""
remote_build=1
remote_binary=""
deploy_binary=""
use_sudo=0
template_base=87960930222080
template_bytes=67108864
template_seed=4852712463353924401
template_value_scale=1
map_template_key=0
map_template_generation=0
map_template_checksum=0
reduce_template_key=0
reduce_template_generation=0
reduce_template_checksum=0
prepare_reduce_template_key=0
cxlfork_policy=mow
validation=fast
ready_timeout=120

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
    --workers-per-agent=*) workers_per_agent=${1#*=} ;;
    --master-host=*) master_host=${1#*=} ;;
    --worker=*) workers+=("${1#*=}") ;;
    --backend=*) backend=${1#*=} ;;
    --shuffle-backend=*) shuffle_backend=${1#*=} ;;
    --reducers=*) reducers=${1#*=} ;;
    --schedule=*) schedule=${1#*=} ;;
    --payload-max=*) payload_max=${1#*=} ;;
    --map-chunk=*) map_chunk=${1#*=} ;;
    --queue-capacity=*) queue_capacity=${1#*=} ;;
    --size=*) map_size=${1#*=} ;;
    --shuffle-connections=*) shuffle_connections=${1#*=} ;;
    --queue-policy=*) queue_policy=${1#*=} ;;
    --verify=*) verify=${1#*=} ;;
    --ssh-user=*) ssh_user=${1#*=} ;;
    --ssh-key=*) ssh_key=${1#*=} ;;
    --remote-dir=*) remote_dir=${1#*=} ;;
    --remote-repo=*) remote_repo=${1#*=} ;;
    --remote-hydrarpc-root=*) remote_hydrarpc_root=${1#*=} ;;
    --no-remote-build) remote_build=0 ;;
    --remote-binary=*) remote_binary=${1#*=} ;;
    --deploy-binary=*) deploy_binary=${1#*=} ;;
    --sudo) use_sudo=1 ;;
    --template-base=*) template_base=${1#*=} ;;
    --template-bytes=*) template_bytes=${1#*=} ;;
    --template-seed=*) template_seed=${1#*=} ;;
    --template-value-scale=*) template_value_scale=${1#*=} ;;
    --map-template-key=*) map_template_key=${1#*=} ;;
    --map-template-generation=*) map_template_generation=${1#*=} ;;
    --map-template-checksum=*) map_template_checksum=${1#*=} ;;
    --reduce-template-key=*) reduce_template_key=${1#*=} ;;
    --reduce-template-generation=*) reduce_template_generation=${1#*=} ;;
    --reduce-template-checksum=*) reduce_template_checksum=${1#*=} ;;
    --prepare-reduce-template-key=*) prepare_reduce_template_key=${1#*=} ;;
    --cxlfork-policy=*) cxlfork_policy=${1#*=} ;;
    --validation=*) validation=${1#*=} ;;
    --ready-timeout=*) ready_timeout=${1#*=} ;;
    *) die "unknown argument: $1" ;;
  esac
  shift
done

[[ -n "$input_dir" && -d "$input_dir" ]] || die "--input-dir is required"
[[ -n "$expected" && -f "$expected" ]] || die "--expected is required"
[[ -n "$output_dir" ]] || die "--output-dir is required"
[[ "$mode" == local || "$mode" == ssh ]] || die "invalid --mode=$mode"
[[ "$repeat" =~ ^[0-9]+$ && "$repeat" -gt 0 ]] || die "--repeat must be a positive integer"
[[ "$warmup" =~ ^[0-9]+$ ]] || die "--warmup must be a non-negative integer"
[[ "$local_workers" =~ ^[0-9]+$ && "$local_workers" -gt 0 ]] || die "--local-workers must be a positive integer"
[[ "$workers_per_agent" =~ ^[0-9]+$ && "$workers_per_agent" -gt 0 ]] || die "--workers-per-agent must be a positive integer"
[[ "$payload_max" =~ ^[0-9]+$ && "$payload_max" -gt 0 ]] || die "--payload-max must be a positive integer"
[[ "$queue_capacity" =~ ^[0-9]+$ && "$queue_capacity" -gt 0 ]] || die "--queue-capacity must be a positive integer"
[[ "$map_size" =~ ^[0-9]+$ && "$map_size" -gt 0 ]] || die "--size must be a positive integer"
[[ "$shuffle_connections" =~ ^[0-9]+$ && "$shuffle_connections" -gt 0 ]] || die "--shuffle-connections must be a positive integer"
[[ "$input_bytes" =~ ^[0-9]+$ && "$input_bytes" -gt 0 ]] || die "--input-bytes must be a positive integer"
[[ "$prepare_reduce_template_key" =~ ^[0-9]+$ ]] || die "--prepare-reduce-template-key must be a non-negative integer"
[[ "$ready_timeout" =~ ^[0-9]+$ && "$ready_timeout" -gt 0 ]] || die "--ready-timeout must be a positive integer"
[[ "$verify" == last || "$verify" == all || "$verify" == none ]] || die "--verify must be last, all, or none"
if [[ "$schedule" != static && "$schedule" != role-static && "$schedule" != file:* ]]; then
  die "--schedule must be static, role-static or file:PATH"
fi
if [[ "$schedule" == file:* ]]; then
  schedule_path=${schedule#file:}
  [[ -n "$schedule_path" && -f "$schedule_path" ]] || die "schedule file not found: $schedule_path"
fi
[[ "$cxlfork_policy" == readonly || "$cxlfork_policy" == mow || "$cxlfork_policy" == moa || "$cxlfork_policy" == hybrid ]] || die "invalid --cxlfork-policy"
[[ "$validation" == fast || "$validation" == full ]] || die "--validation must be fast or full"
[[ -z "$deploy_binary" || -x "$deploy_binary" ]] || die "--deploy-binary must name an executable file"
[[ "$template_value_scale" =~ ^[0-9]+$ && "$template_value_scale" -ge 1 && "$template_value_scale" -le 1024 ]] || die "--template-value-scale must be in [1, 1024]"
mkdir -p "$output_dir"/logs

mapfile -t input_files < <(find "$input_dir" -maxdepth 1 -type f -name '*.txt' | LC_ALL=C sort)
((${#input_files[@]} > 0)) || die "no input files found in $input_dir"
combined_input="$output_dir/combined-input.txt"
cat "${input_files[@]}" > "$combined_input"
actual_input_bytes=$(stat -c %s "$combined_input")
[[ "$actual_input_bytes" -eq "$input_bytes" ]] ||
  die "combined input size mismatch: declared=$input_bytes actual=$actual_input_bytes"
sha256sum "${input_files[@]}" > "$output_dir/input-files.sha256"
combined_input_sha256=$(sha256sum "$combined_input" | awk '{print $1}')
expected_sha256=$(sha256sum "$expected" | awk '{print $1}')
printf '%s  combined-input.txt\n' "$combined_input_sha256" > "$output_dir/input.sha256"
printf '%s  expected.txt\n' "$expected_sha256" > "$output_dir/expected.sha256"

ssh_base_opts=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)
if [[ -n "$ssh_key" ]]; then
	ssh_base_opts+=(-o IdentitiesOnly=yes -i "$ssh_key")
fi

split_ssh_endpoint() {
  local endpoint=$1
  local user=""
  local hostport
  local host
  local port=""

  if [[ "$endpoint" == \[*\] ]]; then
    endpoint=${endpoint#\[}
    endpoint=${endpoint%\]}
  fi

  if [[ "$endpoint" == *@* ]]; then
    user=${endpoint%%@*}
    hostport=${endpoint#*@}
  else
    user=$ssh_user
    hostport=$endpoint
  fi

  if [[ "$hostport" == *:* ]]; then
    host=${hostport%:*}
    port=${hostport##*:}
  else
    host=$hostport
  fi

  printf '%s\t%s\t%s\n' "$user" "$host" "$port"
}

ssh_endpoint_target() {
  local endpoint=$1
  local parsed user host
  parsed=$(split_ssh_endpoint "$endpoint")
  user=$(printf '%s' "$parsed" | awk -F '\t' '{print $1}')
  host=$(printf '%s' "$parsed" | awk -F '\t' '{print $2}')

  if [[ -n "$user" ]]; then
    printf '%s@%s' "$user" "$host"
  else
    printf '%s' "$host"
  fi
}

ssh_endpoint_port() {
  local endpoint=$1
  local parsed
  parsed=$(split_ssh_endpoint "$endpoint")
  printf '%s' "$parsed" | awk -F '\t' '{print $3}'
}

remote_cmd() {
  local host=$1
  shift
  if [[ "$host" == local || "$host" == localhost || -z "$host" ]]; then
    bash -lc "$*"
  else
    local port
    port=$(ssh_endpoint_port "$host")
    ssh_opts=("${ssh_base_opts[@]}")
    [[ -n "$port" ]] && ssh_opts+=(-p "$port")
    ssh "${ssh_opts[@]}" "$(ssh_endpoint_target "$host")" "$*"
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
    local port
    port=$(ssh_endpoint_port "$host")
    scp_opts=("${ssh_base_opts[@]}")
    [[ -n "$port" ]] && scp_opts+=(-P "$port")
    scp "${scp_opts[@]}" -r "$src" "$(ssh_endpoint_target "$host"):$dest"
  fi
}

remote_copy_from() {
  local host=$1
  local src=$2
  local dest=$3
  if [[ "$host" == local || "$host" == localhost || -z "$host" ]]; then
    cp "$src" "$dest"
  else
    local port
    port=$(ssh_endpoint_port "$host")
    scp_opts=("${ssh_base_opts[@]}")
    [[ -n "$port" ]] && scp_opts+=(-P "$port")
    scp "${scp_opts[@]}" "$(ssh_endpoint_target "$host"):$src" "$dest"
  fi
}

remote_file_exists() {
  local host=$1
  local path=$2
  remote_cmd "$host" "test -f '$path'"
}

wait_remote_file() {
  local host=$1
  local path=$2
  remote_cmd "$host" "for i in \$(seq 1 $((ready_timeout * 10))); do test -f '$path' && exit 0; sleep 0.1; done; echo 'timed out waiting for $path' >&2; exit 1"
}

copy_remote_log() {
  local host=$1
  local remote_path=$2
  local local_path=$3
  if remote_file_exists "$host" "$remote_path"; then
    remote_copy_from "$host" "$remote_path" "$local_path" || true
  else
    printf 'remote log not found: %s:%s\n' "$host" "$remote_path" > "$local_path"
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

make_agent_paths_local() {
  local run_id=$1
  local paths=()
  local i
  for ((i = 0; i < local_workers; i++)); do
    paths+=("/tmp/mr-bench-${run_id}-agent${i}.raw")
  done
  (IFS=,; printf '%s' "${paths[*]}")
}

run_local_once() {
  local phase=$1
  local index=$2
  local master_output=$3
  local run_id="${phase}-${index}-$$"
  local agent_paths
  local begin_ns end_ns elapsed_ns elapsed_ms
  agent_paths=$(make_agent_paths_local "$run_id")
  IFS=, read -r -a path_array <<< "$agent_paths"

  agent_pids=()
  local i
  for ((i = 0; i < ${#path_array[@]}; i++)); do
    local role=both
    if [[ "$schedule" == role-static ]]; then
      ((local_workers == 2)) || die "local role-static requires --local-workers=2"
      if ((i == 0)); then role=map; else role=reduce; fi
    fi
    "$repo/build/mr_hydra" agent \
      --backend="$backend" \
      --path="${path_array[$i]}" \
      --node-id="$i" \
      --agent-count="$local_workers" \
      --workers="$workers_per_agent" \
      --role="$role" \
      --template-value-scale="$template_value_scale" \
      --global-worker-base="$((i * workers_per_agent))" \
      --store-dir="$output_dir/store/agent${i}_${phase}_${index}" \
      --shuffle-backend="$shuffle_backend" \
      --shuffle-connections="$shuffle_connections" \
      --queue-policy="$queue_policy" \
      --payload-max="$payload_max" \
      --queue-capacity="$queue_capacity" \
      --size="$map_size" \
      --quiet > "$output_dir/logs/${phase}_${index}_worker${i}.log" 2>&1 &
    agent_pids+=("$!")
  done

  begin_ns=$(now_ns)
  "$repo/build/mr_hydra" master \
    --backend="$backend" \
    --input="$combined_input" \
    --output="$master_output" \
    --agents="$agent_paths" \
    --payload-max="$payload_max" \
    --queue-capacity="$queue_capacity" \
    --size="$map_size" \
    --schedule="$schedule" \
    --workers-per-agent="$workers_per_agent" \
    --connections="$shuffle_connections" \
    --queue-policy="$queue_policy" \
    ${reducers:+--reducers="$reducers"} \
    ${map_chunk:+--map-chunk="$map_chunk"} \
    --quiet > "$output_dir/logs/${phase}_${index}_master.log" 2>&1 &
  master_pid=$!

  local rc=0
  wait "$master_pid" || rc=$?
  end_ns=$(now_ns)
  for pid in "${agent_pids[@]}"; do
    wait "$pid" || rc=$?
  done
  rm -f "${path_array[@]}"
  elapsed_ns=$((end_ns - begin_ns))
  elapsed_ms=$(awk -v ns="$elapsed_ns" 'BEGIN {printf "%.3f", ns / 1000000.0}')
  echo "$elapsed_ms:$rc:$elapsed_ms:$elapsed_ns"
}

parse_worker_spec() {
  local spec=$1
  local host rest
  if [[ "$spec" == *,*,* ]]; then
    local master_path worker_path role launch count extra
    IFS=, read -r host master_path worker_path role launch count extra <<< "$spec"
    role=${role:-both}
    launch=${launch:-persistent}
    count=${count:-$workers_per_agent}
    [[ -n "$host" && -n "$master_path" && -n "$worker_path" && -z "${extra:-}" ]] ||
      die "worker spec must be HOST,MASTER_PATH,AGENT_PATH[,ROLE,LAUNCH,WORKERS]"
    [[ "$role" == map || "$role" == reduce || "$role" == both ]] || die "invalid worker role: $role"
    [[ "$launch" == persistent || "$launch" == localfork || "$launch" == local-prefork || "$launch" == cxlfork ]] || die "invalid worker launch: $launch"
    [[ "$count" =~ ^[0-9]+$ && "$count" -gt 0 ]] || die "invalid worker count: $count"
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$host" "$master_path" "$worker_path" "$role" "$launch" "$count"
    return
  fi

  if [[ "$spec" == \[*\]:* ]]; then
    host=${spec%%\]:*}
    host="${host}]"
    rest=${spec#"$host":}
  else
    host=${spec%%:*}
    rest=${spec#*:}
  fi
  local master_path=${rest%%:*}
  local worker_path=${rest#*:}
  [[ "$host" != "$spec" && "$master_path" != "$rest" && -n "$worker_path" ]] ||
    die "worker spec must be HOST:MASTER_BACKEND_PATH:AGENT_ENDPOINT_PATH"
  printf '%s\t%s\t%s\tboth\tpersistent\t%s\n' "$host" "$master_path" "$worker_path" "$workers_per_agent"
}

host_label() {
  printf '%s' "$1" | tr -c 'A-Za-z0-9_.-' '_'
}

prepare_remote_host() {
  local host=$1
  local repo_on_host=$2
  local log_file="$output_dir/logs/prepare_$(host_label "$host").log"

  {
    printf 'prepare host=%s repo=%s remote_dir=%s\n' "$host" "$repo_on_host" "$remote_dir"
    remote_cmd "$host" "mkdir -p '$remote_dir/input' '$remote_dir/output' '$remote_dir/logs'"
    if [[ -n "$deploy_binary" ]]; then
      remote_binary=${remote_binary:-$remote_dir/mr_hydra}
      remote_copy_to "$deploy_binary" "$host" "$remote_binary"
      remote_cmd "$host" "chmod 0755 '$remote_binary'"
    elif ((remote_build)); then
      remote_cmd "$host" "cmake -S '$repo_on_host' -B '$repo_on_host/build' ${remote_hydrarpc_root:+-DHYDRARPC_ROOT='$remote_hydrarpc_root'} && cmake --build '$repo_on_host/build'"
    fi
  } >> "$log_file" 2>&1
}

repo_on_master=${remote_repo:-$repo}
remote_input="$remote_dir/input/combined-input.txt"
remote_schedule="$remote_dir/input/schedule.csv"
master_schedule_arg="$schedule"
master_paths=()
worker_hosts=()
worker_paths=()
worker_roles=()
worker_launches=()
worker_counts=()
worker_arg=""

prepare_ssh_environment() {
  local start end spec parsed host master_path worker_path role launch count

  master_paths=()
  worker_hosts=()
  worker_paths=()
  worker_roles=()
  worker_launches=()
  worker_counts=()

  start=$(now_ms)
  prepare_remote_host "$master_host" "$repo_on_master"
  remote_copy_to "$combined_input" "$master_host" "$remote_input"
  if [[ "$schedule" == file:* ]]; then
    remote_copy_to "${schedule#file:}" "$master_host" "$remote_schedule"
    master_schedule_arg="file:$remote_schedule"
  fi

  for spec in "${workers[@]}"; do
    parsed=$(parse_worker_spec "$spec")
    host=$(printf '%s' "$parsed" | awk -F '\t' '{print $1}')
    master_path=$(printf '%s' "$parsed" | awk -F '\t' '{print $2}')
    worker_path=$(printf '%s' "$parsed" | awk -F '\t' '{print $3}')
    role=$(printf '%s' "$parsed" | awk -F '\t' '{print $4}')
    launch=$(printf '%s' "$parsed" | awk -F '\t' '{print $5}')
    count=$(printf '%s' "$parsed" | awk -F '\t' '{print $6}')
    master_paths+=("$master_path")
    worker_hosts+=("$host")
    worker_paths+=("$worker_path")
    worker_roles+=("$role")
    worker_launches+=("$launch")
    worker_counts+=("$count")
    prepare_remote_host "$host" "${remote_repo:-$repo}"
  done

  if [[ "$schedule" == role-static ]]; then
    ((${#worker_hosts[@]} == 2)) || die "role-static requires exactly two workers"
    [[ "${worker_roles[0]}" == map && "${worker_roles[1]}" == reduce ]] ||
      die "role-static worker roles must be map then reduce"
    [[ "${worker_counts[0]}" == "${worker_counts[1]}" ]] ||
      die "role-static requires equal fixed worker counts"
    workers_per_agent=${worker_counts[0]}
  fi

  worker_arg=$(IFS=,; printf '%s' "${master_paths[*]}")
  end=$(now_ms)

  if ((remote_build)); then
    build_ms=$((end - start))
  else
    build_ms=0
  fi
}

auto_template_prepared=0
auto_template_binary=""

cleanup_auto_template() {
  if ((auto_template_prepared)); then
    local privilege=""
    ((use_sudo)) && privilege="sudo -E "
    remote_cmd "$master_host" \
      "${privilege}'$auto_template_binary' cxlfork-release '$reduce_template_key' '$reduce_template_generation'" \
      >> "$output_dir/logs/reduce_template_release.log" 2>&1 || true
    auto_template_prepared=0
  fi
}

prepare_reduce_template_if_requested() {
  ((prepare_reduce_template_key != 0)) || return 0
  if [[ "$mode" != ssh ]]; then
    die "--prepare-reduce-template-key is only supported in ssh mode"
  fi
  if [[ "$reduce_template_key" != 0 || "$reduce_template_generation" != 0 ||
        "$reduce_template_checksum" != 0 ]]; then
    die "automatic reduce template preparation cannot be combined with manual key/generation/checksum"
  fi

  local binary=${remote_binary:-${remote_repo:-$repo}/build/mr_hydra}
  local privilege=""
  local log="$output_dir/logs/reduce_template_prepare.log"
  local line token
  ((use_sudo)) && privilege="sudo -E "
  remote_cmd "$master_host" \
    "${privilege}'$binary' template --role=reduce --key='$prepare_reduce_template_key' --base='$template_base' --bytes='$template_bytes' --seed='$template_seed' --value-scale='$template_value_scale'" \
    > "$log" 2>&1 || die "reduce CXLfork template preparation failed; see $log"

  line=$(awk '$1 == "mr_template_v2" {last = $0} END {print last}' "$log")
  [[ -n "$line" ]] || die "template output did not contain mr_template_v2"
  for token in $line; do
    case "$token" in
      key=*) reduce_template_key=${token#*=} ;;
      generation=*) reduce_template_generation=${token#*=} ;;
      page_checksum=*) reduce_template_checksum=${token#*=} ;;
    esac
  done
  [[ "$reduce_template_key" == "$prepare_reduce_template_key" &&
     "$reduce_template_generation" =~ ^[1-9][0-9]*$ &&
     "$reduce_template_checksum" =~ ^[1-9][0-9]*$ ]] ||
    die "invalid template identity parsed from $log"
  auto_template_binary=$binary
  auto_template_prepared=1
  trap cleanup_auto_template EXIT
}

run_ssh_once() {
  local phase=$1
  local index=$2
  local local_output=$3
  local remote_output="$remote_dir/output/${phase}_${index}.out"
  local remote_timer="$remote_dir/logs/${phase}_${index}_guest_timer.log"
  local binary=${remote_binary:-${remote_repo:-$repo}/build/mr_hydra}
  local privilege=""
  ((use_sudo)) && privilege="sudo -E "

  worker_pids=()
  worker_ready_files=()
  worker_store_dirs=()
  local i global_base=0
  for ((i = 0; i < ${#worker_hosts[@]}; i++)); do
    local role=${worker_roles[$i]}
    local launch=${worker_launches[$i]}
    local count=${worker_counts[$i]}
    local ready_file="$remote_dir/logs/${phase}_${index}_worker${i}.ready"
    local store_dir="$remote_dir/store/agent${i}_${phase}_${index}"
    local key=0 generation=0 checksum=0
    if [[ "$role" == map ]]; then
      key=$map_template_key; generation=$map_template_generation; checksum=$map_template_checksum
    elif [[ "$role" == reduce ]]; then
      key=$reduce_template_key; generation=$reduce_template_generation; checksum=$reduce_template_checksum
    fi
    if [[ "$launch" == cxlfork ]] &&
       [[ "$key" == 0 || "$generation" == 0 || "$checksum" == 0 ]]; then
      die "cxlfork worker $i requires role-specific template key/generation/checksum"
    fi
    remote_cmd "${worker_hosts[$i]}" \
      "${privilege}rm -rf '$store_dir'; ${privilege}rm -f '$ready_file'; mkdir -p '$remote_dir/logs' '$remote_dir/store'"
    remote_cmd "${worker_hosts[$i]}" "${privilege}'$binary' agent --backend='$backend' --path='${worker_paths[$i]}' --node-id='$i' --agent-count='${#worker_hosts[@]}' --workers='$count' --global-worker-base='$global_base' --role='$role' --launch='$launch' --template-base='$template_base' --template-bytes='$template_bytes' --template-seed='$template_seed' --template-value-scale='$template_value_scale' --template-key='$key' --template-generation='$generation' --template-page-checksum='$checksum' --cxlfork-policy='$cxlfork_policy' --validation='$validation' --store-dir='$store_dir' --ready-file='$ready_file' --shuffle-backend='$shuffle_backend' --shuffle-connections='$shuffle_connections' --queue-policy='$queue_policy' --payload-max='$payload_max' --queue-capacity='$queue_capacity' --size='$map_size' --quiet > '$remote_dir/logs/${phase}_${index}_worker${i}.log' 2>&1" &
    worker_pids+=("$!")
    worker_ready_files+=("$ready_file")
    worker_store_dirs+=("$store_dir")
    global_base=$((global_base + count))
  done

  for ((i = 0; i < ${#worker_hosts[@]}; i++)); do
    wait_remote_file "${worker_hosts[$i]}" "${worker_ready_files[$i]}" ||
      die "worker $i did not become ready"
  done

  remote_cmd "$master_host" \
    "${privilege}rm -f '$remote_output' '$remote_timer'"
  start=$(now_ms)
  remote_cmd "$master_host" "begin_ns=\$(date +%s%N); ${privilege}'$binary' master --backend='$backend' --input='$remote_input' --output='$remote_output' --agents='$worker_arg' --payload-max='$payload_max' --queue-capacity='$queue_capacity' --size='$map_size' --schedule='$master_schedule_arg' --workers-per-agent='$workers_per_agent' --connections='$shuffle_connections' --queue-policy='$queue_policy' ${reducers:+--reducers='$reducers'} ${map_chunk:+--map-chunk='$map_chunk'} --quiet > '$remote_dir/logs/${phase}_${index}_master.log' 2>&1; rc=\$?; end_ns=\$(date +%s%N); printf 'guest_run_v1 status=%s begin_ns=%s end_ns=%s total_ns=%s\\n' \"\$rc\" \"\$begin_ns\" \"\$end_ns\" \"\$((end_ns - begin_ns))\" > '$remote_timer'; exit \"\$rc\"" &
  master_pid=$!

  local rc=0
  wait "$master_pid" || rc=$?
  end=$(now_ms)
  for pid in "${worker_pids[@]}"; do
    wait "$pid" || rc=$?
  done

  copy_remote_log "$master_host" \
    "$remote_dir/logs/${phase}_${index}_master.log" \
    "$output_dir/logs/${phase}_${index}_master.remote.log"
  copy_remote_log "$master_host" "$remote_timer" \
    "$output_dir/logs/${phase}_${index}_guest_timer.remote.log"
  for ((i = 0; i < ${#worker_hosts[@]}; i++)); do
    copy_remote_log "${worker_hosts[$i]}" \
      "$remote_dir/logs/${phase}_${index}_worker${i}.log" \
      "$output_dir/logs/${phase}_${index}_worker${i}.remote.log"
    remote_cmd "${worker_hosts[$i]}" \
      "${privilege}rm -rf '${worker_store_dirs[$i]}'" || rc=1
  done

  local guest_run_ns=0
  local guest_run_ms=0
  guest_run_ns=$(awk '
    $1 == "guest_run_v1" {
      for (i = 2; i <= NF; i++) {
        split($i, kv, "=")
        if (kv[1] == "total_ns") value = kv[2]
      }
    }
    END {print value + 0}
  ' "$output_dir/logs/${phase}_${index}_guest_timer.remote.log")
  if [[ ! "$guest_run_ns" =~ ^[1-9][0-9]*$ ]]; then
    printf 'missing or invalid guest timer for %s %s\n' "$phase" "$index" \
      > "$output_dir/logs/${phase}_${index}_missing_timer.log"
    rc=1
  else
    guest_run_ms=$(awk -v ns="$guest_run_ns" 'BEGIN {printf "%.3f", ns / 1000000.0}')
  fi

  if ((rc != 0)) || ! remote_file_exists "$master_host" "$remote_output"; then
    printf 'missing remote output: %s:%s\n' "$master_host" "$remote_output" \
      > "$output_dir/logs/${phase}_${index}_missing_output.log"
    rc=1
  else
    remote_copy_from "$master_host" "$remote_output" "$local_output" || rc=$?
  fi

  echo "$guest_run_ms:$rc:$((end - start)):$guest_run_ns"
}

if [[ "$mode" == ssh && ${#workers[@]} -eq 0 ]]; then
  die "--mode=ssh requires at least one --worker"
fi

build_start=$(now_ms)
if [[ "$mode" == local ]]; then
  build_ms=$(build_local)
elif [[ "$mode" == ssh ]]; then
  prepare_ssh_environment
else
  build_ms=0
fi
build_end=$(now_ms)
if [[ "$mode" == local && "$build_ms" == 0 ]]; then
  build_ms=$((build_end - build_start))
fi

prepare_reduce_template_if_requested

run_csv="$output_dir/runs.csv"
warmup_csv="$output_dir/warmups.csv"
task_csv="$output_dir/tasks.csv"
stage_csv="$output_dir/stages.csv"
printf 'index,run_ms,exit_status,orchestrator_ms,guest_run_ns\n' > "$run_csv"
printf 'index,run_ms,exit_status,orchestrator_ms,guest_run_ns\n' > "$warmup_csv"
printf 'phase,run_index,task_id,kind,node,worker,pid,status,roundtrip_ns,porter_total_ns,dispatch_ns,fork_ns,wakeup_ns,restore_ioctl_ns,template_validate_ns,task_ns,reap_ns,refill_ns,restore_id,path_mask,kernel_total_ns,shared_leaf_pages,fallback_pages,local_copy_pages,leaf_copy_pages,template_state_checksum,template_access_checksum,template_access_count,template_state_pages,template_binding_flags\n' > "$task_csv"
printf 'phase,run_index,status,total_ns,input_setup_ns,map_stage_ns,reduce_stage_ns,output_write_ns,other_ns,map_tasks,reducers\n' > "$stage_csv"

cat > "$output_dir/run-manifest.env" <<EOF
format=mr-e2e-aligned-v1
created_utc=$(date -u +%FT%TZ)
mr_commit=$(git -C "$repo" rev-parse HEAD 2>/dev/null || printf unknown)
measurement_boundary=guest_master_launch_to_exit
input_bytes=$input_bytes
input_files=${#input_files[@]}
input_sha256=$combined_input_sha256
expected_sha256=$expected_sha256
warmup=$warmup
repeat=$repeat
workers_per_agent=$workers_per_agent
reducers=${reducers:-0}
map_chunk=${map_chunk:-0}
shuffle_connections=$shuffle_connections
queue_policy=$queue_policy
cxlfork_policy=$cxlfork_policy
validation=$validation
reduce_template_key=$reduce_template_key
reduce_template_generation=$reduce_template_generation
reduce_template_checksum=$reduce_template_checksum
EOF

append_task_metrics() {
  local phase=$1 index=$2 log=$3
  [[ -f "$log" ]] || return 0
  awk -v phase="$phase" -v run="$index" '
    $1 == "task_result_v4" {
      delete v
      for (i = 2; i <= NF; i++) {
        split($i, kv, "=")
        v[kv[1]] = kv[2]
      }
      printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
        phase, run, v["task"], v["kind"], v["node"], v["worker"],
        v["pid"], v["status"], v["roundtrip_ns"],
        v["agent_collect_ns"] - v["porter_submit_ns"],
        v["supervisor_recv_ns"] - v["porter_submit_ns"],
        v["fork_return_ns"] - v["fork_begin_ns"],
        v["worker_entry_ns"] - v["slot_publish_ns"],
        v["restore_wrapper_ns"], v["bind_end_ns"] - v["bind_begin_ns"],
        v["task_done_ns"] - v["business_entry_ns"],
        v["reap_end_ns"] - v["reap_begin_ns"],
        v["refill_end_ns"] - v["refill_begin_ns"],
        v["restore_id"], v["path_mask"],
        v["kernel_total_ns"], v["shared_data_pages"], v["fallback_pages"],
        v["local_copy_pages"], v["leaf_copy_pages"],
        v["template_state_checksum"], v["template_access_checksum"],
        v["template_access_count"], v["template_state_pages"],
        v["template_binding_flags"]
    }
  ' "$log" >> "$task_csv"
}

append_job_metrics() {
  local phase=$1 index=$2 log=$3
  [[ -f "$log" ]] || return 0
  awk -v phase="$phase" -v run="$index" '
    $1 == "master_job_v2" {
      delete v
      for (i = 2; i <= NF; i++) {
        split($i, kv, "=")
        v[kv[1]] = kv[2]
      }
      printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
        phase, run, v["status"], v["total_ns"],
        v["input_setup_ns"], v["map_stage_ns"],
        v["reduce_stage_ns"], v["output_write_ns"],
        v["other_ns"], v["map_tasks"], v["reducers"]
    }
  ' "$log" >> "$stage_csv"
}

run_values=()
status=ok
correctness=skipped

for ((i = 0; i < warmup; i++)); do
  raw="$output_dir/warmup_${i}.out"
  if [[ "$mode" == local ]]; then
    result=$(run_local_once warmup "$i" "$raw")
  else
    result=$(run_ssh_once warmup "$i" "$raw")
  fi
  IFS=: read -r run_ms rc orchestrator_ms guest_run_ns <<< "$result"

  if [[ "$rc" != 0 ]]; then
    status=failed
    exit_status=$rc
  else
    exit_status=0
  fi
  printf '%s,%s,%s,%s,%s\n' "$i" "$run_ms" "$exit_status" \
    "$orchestrator_ms" "$guest_run_ns" >> "$warmup_csv"
  if [[ "$mode" == local ]]; then
    append_task_metrics warmup "$i" "$output_dir/logs/warmup_${i}_master.log"
  else
    append_task_metrics warmup "$i" "$output_dir/logs/warmup_${i}_master.remote.log"
  fi
  if [[ "$mode" == local ]]; then
    append_job_metrics warmup "$i" "$output_dir/logs/warmup_${i}_master.log"
  else
    append_job_metrics warmup "$i" "$output_dir/logs/warmup_${i}_master.remote.log"
  fi
done

for ((i = 0; i < repeat; i++)); do
  raw="$output_dir/repeat_${i}.out"
  if [[ "$mode" == local ]]; then
    result=$(run_local_once repeat "$i" "$raw")
  else
    result=$(run_ssh_once repeat "$i" "$raw")
  fi
  IFS=: read -r run_ms rc orchestrator_ms guest_run_ns <<< "$result"

  if [[ "$rc" != 0 ]]; then
    status=failed
    exit_status=$rc
  else
    exit_status=0
  fi
  printf '%s,%s,%s,%s,%s\n' "$i" "$run_ms" "$exit_status" \
    "$orchestrator_ms" "$guest_run_ns" >> "$run_csv"
  if [[ "$mode" == local ]]; then
    append_task_metrics repeat "$i" "$output_dir/logs/repeat_${i}_master.log"
  else
    append_task_metrics repeat "$i" "$output_dir/logs/repeat_${i}_master.remote.log"
  fi
  if [[ "$mode" == local ]]; then
    append_job_metrics repeat "$i" "$output_dir/logs/repeat_${i}_master.log"
  else
    append_job_metrics repeat "$i" "$output_dir/logs/repeat_${i}_master.remote.log"
  fi
  run_values+=("$run_ms")
done

if [[ "$verify" != none ]]; then
  correctness=ok
  verify_indices=()
  if [[ "$verify" == all ]]; then
    for ((i = 0; i < repeat; i++)); do
      verify_indices+=("$i")
    done
  else
    verify_indices=("$((repeat - 1))")
  fi

  for i in "${verify_indices[@]}"; do
    raw="$output_dir/repeat_${i}.out"
    norm="$output_dir/repeat_${i}.norm"
    if [[ ! -f "$raw" ]]; then
      status=failed
      correctness=failed
      printf 'output file not produced: %s\n' "$raw" \
        > "$output_dir/logs/repeat_${i}.diff"
      continue
    fi

    normalize_wordcount "$raw" "$norm"
    if ! diff -u "$expected" "$norm" > "$output_dir/logs/repeat_${i}.diff"; then
      status=failed
      correctness=failed
    fi
  done
fi

read -r run_ms_avg run_ms_min run_ms_max run_ms_stddev < <(
  printf '%s\n' "${run_values[@]}" |
    awk 'NR == 1 {min = max = $1} {values[NR] = $1; if ($1 < min) min = $1; if ($1 > max) max = $1; sum += $1} END {if (NR == 0) print "0 0 0 0"; else {mean = sum / NR; for (i = 1; i <= NR; i++) {d = values[i] - mean; ss += d * d} printf "%.3f %.3f %.3f %.3f\n", mean, min, max, sqrt(ss / NR)}}'
)

run_ms_median=$(
  printf '%s\n' "${run_values[@]}" |
    LC_ALL=C sort -n |
    awk '{values[NR] = $1} END {if (NR == 0) print "0"; else if (NR % 2 == 1) printf "%.3f", values[(NR + 1) / 2]; else printf "%.3f", (values[NR / 2] + values[NR / 2 + 1]) / 2}'
)

run_ms_p95=$(
  printf '%s\n' "${run_values[@]}" |
    LC_ALL=C sort -n |
    awk '{values[NR] = $1} END {if (NR == 0) print "0"; else {idx = int(NR * 0.95); if (idx < NR * 0.95) idx++; if (idx < 1) idx = 1; printf "%.3f", values[idx]}}'
)

throughput_mib_s=$(awk -v bytes="$input_bytes" -v ms="$run_ms_avg" 'BEGIN { if (ms <= 0) print "0"; else printf "%.3f", bytes / 1048576 / (ms / 1000) }')
agent_count=$([[ "$mode" == local ]] && printf '%s' "$local_workers" || printf '%s' "${#workers[@]}")
worker_count=$((agent_count * workers_per_agent))
details="mode=${mode};backend=${backend};agents=${agent_count};workers=${worker_count};workers_per_agent=${workers_per_agent};reducers=${reducers:-$worker_count};schedule=${schedule};payload_max=${payload_max};map_chunk=${map_chunk:-auto};queue_capacity=${queue_capacity};size=${map_size};shuffle_connections=${shuffle_connections};queue_policy=${queue_policy};template_value_scale=${template_value_scale};verify=${verify}"

cat > "$output_dir/result.env" <<EOF
impl_name=distributed
status=$status
correctness=$correctness
build_ms=$build_ms
run_ms_avg=$run_ms_avg
run_ms_min=$run_ms_min
run_ms_max=$run_ms_max
run_ms_median=$run_ms_median
run_ms_p95=$run_ms_p95
run_ms_stddev=$run_ms_stddev
throughput_mib_s=$throughput_mib_s
input_bytes=$input_bytes
input_sha256=$combined_input_sha256
measurement_boundary=guest_master_launch_to_exit
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
  "run_ms_median": $run_ms_median,
  "run_ms_p95": $run_ms_p95,
  "run_ms_stddev": $run_ms_stddev,
  "throughput_mib_s": $throughput_mib_s,
  "measurement_boundary": "guest_master_launch_to_exit",
  "input_sha256": "$combined_input_sha256",
  "mode": "$mode",
  "backend": "$backend",
  "agents": $agent_count,
  "workers": $worker_count,
  "workers_per_agent": $workers_per_agent,
  "reducers": ${reducers:-$worker_count},
  "schedule": "$schedule",
  "payload_max": $payload_max,
  "map_chunk": "${map_chunk:-auto}",
  "queue_capacity": $queue_capacity,
  "size": $map_size,
  "shuffle_connections": $shuffle_connections,
  "queue_policy": "$queue_policy",
  "template_value_scale": $template_value_scale,
  "verify": "$verify",
  "warmup": $warmup,
  "repeat": $repeat,
  "input_bytes": $input_bytes
}
EOF

cat > "$output_dir/summary.csv" <<EOF
impl,mode,status,correctness,run_ms_avg,run_ms_min,run_ms_max,run_ms_median,run_ms_stddev,throughput_mib_s,input_bytes,measurement_boundary,details
distributed,$mode,$status,$correctness,$run_ms_avg,$run_ms_min,$run_ms_max,$run_ms_median,$run_ms_stddev,$throughput_mib_s,$input_bytes,guest_master_launch_to_exit,"$details"
EOF

task_summary_csv="$output_dir/task_summary.csv"
printf 'kind,metric,count,mean_ns,p50_ns,p95_ns,stddev_ns\n' > "$task_summary_csv"
summarize_task_metric() {
  local kind=$1 metric=$2 column=$3
  awk -F, -v kind="$kind" -v column="$column" \
    '$1 == "repeat" && $4 == kind {print $column}' "$task_csv" |
    LC_ALL=C sort -n |
    awk -v kind="$kind" -v metric="$metric" '
      {value[NR] = $1; sum += $1; sumsq += $1 * $1}
      END {
        if (NR > 0) {
          p50 = int(NR * 0.50); if (p50 < NR * 0.50) p50++; if (p50 < 1) p50 = 1
          p95 = int(NR * 0.95); if (p95 < NR * 0.95) p95++; if (p95 < 1) p95 = 1
          mean = sum / NR
          variance = sumsq / NR - mean * mean
          if (variance < 0) variance = 0
          printf "%s,%s,%d,%.3f,%d,%d,%.3f\n", kind, metric, NR,
                 mean, value[p50], value[p95], sqrt(variance)
        }
      }
    ' >> "$task_summary_csv"
}
for kind in 1 2; do
  summarize_task_metric "$kind" roundtrip 9
  summarize_task_metric "$kind" porter_total 10
  summarize_task_metric "$kind" dispatch 11
  summarize_task_metric "$kind" fork 12
  summarize_task_metric "$kind" restore_ioctl 14
  summarize_task_metric "$kind" template_validate 15
  summarize_task_metric "$kind" task 16
  summarize_task_metric "$kind" reap 17
  summarize_task_metric "$kind" refill 18
  summarize_task_metric "$kind" kernel_total 21
done

stage_summary_csv="$output_dir/stage_summary.csv"
printf 'stage,count,mean_ns,p50_ns,min_ns,max_ns,stddev_ns\n' > "$stage_summary_csv"
summarize_stage_metric() {
  local stage=$1 column=$2
  awk -F, -v column="$column" '$1 == "repeat" {print $column}' "$stage_csv" |
    LC_ALL=C sort -n |
    awk -v stage="$stage" '
      NR == 1 {min = max = $1}
      {value[NR] = $1; sum += $1; if ($1 < min) min = $1; if ($1 > max) max = $1}
      END {
        if (NR > 0) {
          p50 = int(NR * 0.50); if (p50 < NR * 0.50) p50++; if (p50 < 1) p50 = 1
          mean = sum / NR
          for (i = 1; i <= NR; i++) {d = value[i] - mean; ss += d * d}
          printf "%s,%d,%.3f,%d,%d,%d,%.3f\n", stage, NR, mean,
                 value[p50], min, max, sqrt(ss / NR)
        }
      }
    ' >> "$stage_summary_csv"
}
summarize_stage_metric master_internal_total 4
summarize_stage_metric input_setup 5
summarize_stage_metric map_stage 6
summarize_stage_metric reduce_stage 7
summarize_stage_metric output_write 8
summarize_stage_metric other 9

[[ "$status" == ok ]] || exit 1
