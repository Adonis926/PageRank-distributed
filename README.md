# mr-distributed

基于 HydraRPC 的多机 MapReduce 示例实现，通信方式使用 `hydrarpc-poll`。控制面为 `master <-> node agent`，node agent 在本 VM 内 fork 多个 worker 子进程；数据面继续复用每个 VM 的 node-level HydraRPC endpoint，通过 `shared_pair` 多逻辑连接拉取远端 intermediate partition。

当前示例应用是 word count。分布式 runtime 保留了接近单机多线程版的结构：应用只实现 `map` / `reduce`，map 和 reduce 都通过 `MR_Emit` 产出 KV，runtime 负责 partition、排序、`Getter get_next`、HydraRPC 调度和结果写出。

## 运行时结构

当前数据流分为三个阶段：

1. master 将输入按 `--map-chunk` 切片，只把 map task descriptor 和输入片段发给 node agent。
2. node agent 通过本地共享内存 slot 把任务交给 worker 子进程。map worker 执行 map、partition、排序和 count 型 combiner，并把每个 reducer partition 写成本地 KV wire-format 文件；返回给 master 的只有 `IntermediateLocation` metadata。
3. master 按 reducer 汇总 locations，并把 reduce task descriptor 发给 node agent。reduce worker 对本地 location 直接读文件；对远端 location 通过远端 node agent endpoint 拉取文件 chunk，append 到本地 spill file，最后 parse/sort/reduce。

外部进程角色是：

```text
mr_hydra master
mr_hydra agent --workers=N
```

`agent` 会 fork 本地 worker 子进程。worker 子进程不作为独立 CLI role 暴露，也不会实例化 HydraRPC endpoint；本地 agent/worker 之间使用共享内存 descriptor queue 传递任务和结果，避免 UDS 大 payload copy。

HydraRPC endpoint 是 node-level 的：一个 agent endpoint 同时接收 master 控制请求和远端 reducer 的 `MR_TASK_SHUFFLE_FETCH` 请求。`--shuffle-connections=N` 配合 `--queue-policy=shared_pair` 用于在同一对节点之间复用 request/response queue，并用 `connection_id` 区分本机 worker 的逻辑 shuffle 流。

`ShuffleChunkHeader` 当前字段如下：

```c
typedef struct ShuffleChunkHeader {
    uint64_t job_id;
    uint32_t mapper_id;
    uint32_t reducer_id;

    uint64_t partition_seq;
    uint32_t chunk_seq;
    uint32_t flags;

    uint32_t record_count;
    uint32_t payload_bytes;
} ShuffleChunkHeader;
```

`ShuffleChunkHeader` 用于 shuffle fetch response。HydraRPC payload 生命周期只覆盖单次 req/resp，因此 reducer 收到 chunk 后立即 append 到 spill file，不长期保存 payload 指针。

map/reduce response 的业务 payload 前带固定 232-byte `mr_task_result_v2`
envelope。它包含 magic、size、version、任务/worker/PID、porter 分项以及本次
CXLfork restore 的 path、kernel time、page counters 和 template-state binding
证据。master 严格校验后移除
envelope，再把业务结果交给原来的 WordCount 解析，因此测量元数据不会改变应用
输出。`task_ns` 只包围应用回调；restore、template validation、reap 和 refill
单独计时，`porter_total_ns` 才是包含这些阶段的外层区间。

## 应用状态强绑定

CXLfork template v2 不再只是随机 arena 加 header。它包含 versioned
`MR_WorkerStateV1`：ASCII word-character 表、小写转换表、数字解析表、partition
hash 参数和 WordCount value scale。Mapper、partitioner 和 Reducer 都必须通过
`MR_GetWorkerState()` 取得并实际消费这些状态；没有成功 bind、角色不匹配、状态
损坏或 callback 没有读取状态时，porter 会让任务明确失败。

这仍然不是任意 IP/SP/栈的透明进程恢复。代码页继续来自相同 `mr_hydra` binary，
HydraRPC endpoint 继续由 node agent 持有；恢复的是应用执行所必需的、只读私有
worker 初始化状态。因而跳过 `RESUME_V2`，或者只伪造成功而不提供内容一致的
worker state，都不能得到一个合法的 CXLfork 任务。

逐任务证据包括：

- `template_state_checksum`：实际 bind 的 worker-state checksum；
- `template_access_checksum`、`template_access_count`：callback 的状态获取证据；
- `template_state_pages`：状态覆盖页数；
- `template_binding_flags=15`：validated、role-match、app-consumed、required 全部成立。

`--template-value-scale=N` 是显式、可校验的状态配置。正常实验固定为 1；验收测试
发布 scale=2 的合法 Reducer template，并验证 WordCount 输出确实整体翻倍，以证明
业务输出依赖恢复状态。`template --test-corrupt-state` 只用于负向测试。

调度默认是静态、可重复的：

- map task：`task_id % agent_count` 选择 agent，`task_id / agent_count % workers_per_agent` 选择本地 worker。
- reduce task：`reducer_id % agent_count` 选择 agent，`reducer_id / agent_count % workers_per_agent` 选择本地 worker。

为固定的两 VM Mapper/Reducer 实验提供 `--schedule=role-static`：该模式要求
恰好两个 agent，并将全部 map task 固定发往 agent 0、全部 reduce task 固定
发往 agent 1；`--workers-per-agent=N` 决定 master 在每个 agent 内使用的固定
worker id 范围。agent 侧用 `--role=map|reduce` 做第二层校验，错误角色的任务
会明确失败。

`role-static` 还提供固定宽度的并发窗口。master 每批最多向一个 agent 的
`workers-per-agent` 个 worker 各派发一个任务，再按 response 中的 task/partition
id 回收乱序完成结果；agent 的 porter 采用异步 submit/collect，单个 supervisor
同时观察全部 worker slot，因此不会把多个 ghost 重新串行化。并发 Reducer
必须配置 `--shuffle-connections >= --workers`，每个 reducer worker 固定绑定一个
response connection；配置不足会在 agent 启动时明确失败。

## 构建

```bash
cmake -S . -B build
cmake --build build
```

默认按源码相对路径查找 HydraRPC：`../../HydraRPC`。如果目录布局不同，可以显式指定：

```bash
cmake -S . -B build -DHYDRARPC_ROOT=/path/to/HydraRPC
```

## 本地两 agent 烟测

默认由 master 创建并初始化控制面 file backend，所以先启动 master，让它创建共享文件并等待 agent 响应：

```bash
./build/mr_hydra master \
  --input=input.txt \
  --output=wc.out \
  --agents=/tmp/mr-agent0.raw,/tmp/mr-agent1.raw
```

然后在另外两个终端启动 agent，每个 agent fork 两个本地 worker：

```bash
./build/mr_hydra agent \
  --path=/tmp/mr-agent0.raw \
  --node-id=0 \
  --agent-count=2 \
  --workers=2 \
  --store-dir=/tmp/mr-store0 \
  --shuffle-connections=2 \
  --queue-policy=shared_pair

./build/mr_hydra agent \
  --path=/tmp/mr-agent1.raw \
  --node-id=1 \
  --agent-count=2 \
  --workers=2 \
  --global-worker-base=2 \
  --store-dir=/tmp/mr-store1 \
  --shuffle-connections=2 \
  --queue-policy=shared_pair
```

master 默认创建并初始化 file backend；agent 默认 attach。master 结束时会向所有 agent 发送 shutdown，agent 再关闭本地 worker。

## 多机运行

每个 agent 使用一段能被 master 和该 agent 同时访问的 HydraRPC node-level endpoint 后端；同一个 endpoint 也服务远端 shuffle fetch。当前 QEMU 测试默认使用 devdax：

```bash
# master 节点
./build/mr_hydra master \
  --backend=devdax \
  --input=input.txt \
  --output=wc.out \
  --agent=/dev/dax0.0 \
  --size=2097152000 \
  --connections=2 \
  --queue-policy=shared_pair

# agent 节点
./build/mr_hydra agent \
  --backend=devdax \
  --path=/dev/dax0.0 \
  --node-id=1 \
  --workers=1 \
  --size=2097152000 \
  --shuffle-connections=2 \
  --queue-policy=shared_pair
```

### file backend 参数

`--backend=file` 时，`PATH` 是 HydraRPC 用来 `mmap` 的共享文件路径。master 负责创建并初始化该文件，agent 默认 attach，因此同一个 agent endpoint 在 master 侧和 agent 侧必须指向同一份共享文件。

本机或同一文件系统内运行时，两端路径通常相同：

```bash
# master
./build/mr_hydra master \
  --backend=file \
  --input=input.txt \
  --output=wc.out \
  --agents=/tmp/mr-agent0.raw,/tmp/mr-agent1.raw \
  --size=67108864 \
  --connections=2 \
  --queue-policy=shared_pair

# agent 0
./build/mr_hydra agent \
  --backend=file \
  --path=/tmp/mr-agent0.raw \
  --node-id=0 \
  --agent-count=2 \
  --workers=2 \
  --store-dir=/tmp/mr-store0 \
  --size=67108864 \
  --shuffle-connections=2 \
  --queue-policy=shared_pair

# agent 1
./build/mr_hydra agent \
  --backend=file \
  --path=/tmp/mr-agent1.raw \
  --node-id=1 \
  --agent-count=2 \
  --workers=2 \
  --global-worker-base=2 \
  --store-dir=/tmp/mr-store1 \
  --size=67108864 \
  --shuffle-connections=2 \
  --queue-policy=shared_pair
```

跨 VM 使用 `file` backend 时，`PATH` 不能是各 VM 私有的 `/tmp` 文件；它必须位于 master 和对应 agent 都能访问到的共享文件系统上，例如 NFS、virtio-fs、9p，或其他真正共享的文件映射。脚本里的 `--dist-worker=HOST,MASTER_ENDPOINT_PATH,AGENT_ENDPOINT_PATH` 用来描述两端看到同一 endpoint 文件时可能不同的挂载路径：

```bash
scripts/mr-bench.sh \
  --impl=distributed \
  --dist-mode=ssh \
  --dist-master-host=127.0.0.1:2401 \
  --dist-worker=127.0.0.1:2401,/mnt/mr/agent0.raw,/mnt/mr/agent0.raw \
  --dist-worker=127.0.0.1:2402,/mnt/mr/agent1.raw,/mnt/mr/agent1.raw \
  --dist-backend=file \
  --dist-size=67108864 \
  --dist-workers-per-agent=2 \
  --dist-shuffle-connections=2 \
  --dist-queue-policy=shared_pair \
  --dist-reducers=4
```

如果 master VM 中挂载路径和 agent VM 中挂载路径不同，就分别填写第二、第三列，例如 `--dist-worker=vm2,/mnt/master/agent1.raw,/mnt/agent/agent1.raw`。`--dist-size` 必须与 master/agent 传入的 `--size` 一致；file backend 会按这个大小创建/截断共享文件。

常用参数：

- `--backend=file|devdax|ivshmem`：HydraRPC 后端。
- `--agent=PATH`：master 添加一个 agent endpoint 后端路径，可重复。
- `--agents=A,B,C`：master 一次添加多个 agent endpoint 后端路径。
- `--path=PATH`：agent 使用的 node-level endpoint 后端路径。
- `--workers=N`：agent fork 的本地 worker 子进程数量。
- `--agent-count=N`：全局 agent 数量，用于静态本地 worker 调度。
- `--schedule=static|role-static|file:PATH`：master 调度策略，默认 `static`；`role-static` 固定 agent 0 为 Mapper、agent 1 为 Reducer；`file:PATH` 从调度文件读取 map/reduce task 到 `node_id,worker_id` 的绑定。
- `--workers-per-agent=N`：master 在 `role-static` 模式中使用的固定 worker 数，必须和两个 agent 的 `--workers` 一致。
- `--connections=N`：master 打开 agent endpoint 时使用的 HydraRPC 多连接数量，应与 agent 的 `--shuffle-connections` 一致。
- `--shuffle-connections=N`：agent endpoint 的 HydraRPC 多连接数量。
- 固定并发 Reducer 使用 `shared_pair` 时，`--shuffle-connections` 至少等于该 agent
  的 `--workers`；master 的 `--connections` 使用同一数值。
- `--queue-policy=shared_pair|per_connection`：endpoint queue policy，默认 `shared_pair`。
- `--reducers=N`：reducer partition 数量，默认等于 agent 数量。
- `--payload-max=N`：单条 HydraRPC payload 上限，默认 `1048576`。
- `--map-chunk=N`：map 输入分片大小，默认约为 `payload-max / 4`。
- `--queue-capacity=N`：HydraRPC 队列容量，默认 `4096`。
- `--size=N`：HydraRPC 后端映射大小；file backend 会按该大小创建/截断共享文件，devdax 场景应与设备映射大小一致，例如 `2097152000`。
- `--create`：agent 侧也创建/初始化后端，主要用于调试；正常多机运行由 master 创建。

通信轮询固定使用 HydraRPC 的 busy poll 等待路径，即 `hydrarpc-poll`。

## 固定拓扑 benchmark runner

`scripts/run-bench.sh` 支持把每个 agent 的固定角色和生命周期写进 worker spec：

```text
--worker=HOST,MASTER_PATH,AGENT_PATH,ROLE,LAUNCH,WORKERS
```

例如 `...,map,localfork,4` 与 `...,reduce,cxlfork,4`。CXLfork worker 还需传
对应 role 的 template key、generation 和 checksum。runner 输出：

- `runs.csv`、`warmups.csv`：端到端每轮耗时；
- `tasks.csv`：逐任务、逐调用的原始 envelope 字段；
- `task_summary.csv`：按 map/reduce 和字段汇总 mean/P50/P95/标准差；
- `result.json`、`result.env`：正确性、吞吐和端到端 mean/P50/P95/标准差；
- `logs/`：master/agent 原始日志和校验 diff。

30 次样本不会输出 P99；如论文实验需要 P99，应单独把 repeat 提高到数百或约
1,000 次。默认 `verify=all` 可让每个计时样本都先通过 WordCount 输出校验。

应用状态强绑定的一键验收脚本是：

```bash
scripts/test-state-binding.sh
```

它要求 91 环境的 VM0/VM1 已启动，默认使用专用测试 key 2002，依次验证正常
scale=1、输出依赖 scale=2、readonly/MoA/Hybrid 状态绑定、损坏状态拒绝、错误角色
拒绝、stale generation 拒绝、reader 回收和两台 VM dmesg。原始结果默认写入
`/home/xtl/cxlfork/mr-static-results/state-binding-acceptance`。若 key 2002 已由其他
实验占用，应先改用 `TEST_KEY=N`，不要释放不属于本测试的对象。

## 调度文件

默认 `static` 调度仍使用公式：

- map task：`task_id % agent_count` 选择 agent，`task_id / agent_count % workers_per_agent` 选择本地 worker。
- reduce task：`reducer_id % agent_count` 选择 agent，`reducer_id / agent_count % workers_per_agent` 选择本地 worker。

如果传 `--schedule=file:PATH`，master 会从 CSV 文件读取完整绑定。格式是：

```csv
kind,id,node_id,worker_id
map,0,0,0
map,1,1,0
reduce,0,0,0
reduce,1,1,0
```

说明：

- `kind` 支持 `map`/`m` 和 `reduce`/`r`。
- `id` 对 map 表示 map task id，对 reduce 表示 reducer partition id。
- `node_id` 是 agent 的 `--node-id`，同时对应 master `--agents` 中的索引。
- `worker_id` 是该 agent 内部的 local worker id，范围是 `0..workers-1`。
- 文件支持表头、空行和 `#` 注释。
- `file:PATH` 策略要求每个 map task 和每个 reducer 都有且只有一条绑定；缺失、重复、node 越界会直接失败。worker 越界会由目标 agent 在执行时拒绝。

## Benchmark 脚本

推荐从仓库根目录使用通用脚本：

```bash
scripts/mr-bench.sh \
  --impl=distributed \
  --dist-mode=ssh \
  --dist-master-host=127.0.0.1:2401 \
  --dist-worker=127.0.0.1:2401,/dev/dax0.0,/dev/dax0.0 \
  --dist-worker=127.0.0.1:2402,/dev/dax0.0,/dev/dax0.0 \
  --dist-backend=devdax \
  --dist-size=2097152000 \
  --dist-workers-per-agent=1 \
  --dist-schedule=static \
  --dist-shuffle-connections=1 \
  --dist-queue-policy=shared_pair \
  --dist-reducers=4 \
  --dist-payload-max=4194304 \
  --dist-map-chunk=524288 \
  --verify=last
```

`--dist-worker` 当前表示一个 node agent：`HOST,MASTER_ENDPOINT_PATH,AGENT_ENDPOINT_PATH`。逗号分隔格式便于保留不同 VM 中设备名不一致时的路径覆盖。两 VM 场景建议传两个 `--dist-worker`，分别启动 VM1 和 VM2 的 node agent；worker 身份通过 task metadata、`worker_id` 和 HydraRPC `connection_id` 区分。

根目录的 `scripts/example.sh` 封装了两台普通 QEMU VM 的默认场景：

```bash
VM1=127.0.0.1:2401 \
VM2=127.0.0.1:2402 \
SSH_USER=xtl \
REMOTE_REPO=/home/xtl/mr/mr-distributed \
REMOTE_HYDRARPC_ROOT=/home/xtl/HydraRPC \
BACKEND=devdax \
MASTER_TO_VM2_PATH=/dev/dax0.0 \
VM2_WORKER_PATH=/dev/dax0.0 \
VM1_AGENT_MASTER_PATH=/dev/dax0.0 \
VM1_AGENT_PATH=/dev/dax0.0 \
VM2_AGENT_MASTER_PATH=/dev/dax0.0 \
VM2_AGENT_PATH=/dev/dax0.0 \
SIZE=2097152000 \
SCHEDULE=static \
WORKERS_PER_AGENT=1 \
SHUFFLE_CONNECTIONS=1 \
QUEUE_POLICY=shared_pair \
VERIFY=last \
scripts/example.sh
```

脚本不会启动虚拟机；QEMU、devdax/ivshmem 后端和 SSH 端口转发需要提前准备好。SSH 只用于远端 build、启动 master/agent、复制输入和结果；HydraRPC 的共享内存后端才是被测通信路径。远端 configure/build 输出会写入结果目录中的 `runs/distributed/logs/prepare_<host>.log`，每次运行结束都会生成结果目录和同名 `.tar.gz` 压缩包。

默认 benchmark 口径是完整 MapReduce job 的 elapsed wall time，并用输入字节数计算吞吐率。计时不包含 build、输入发现、结果 sort/diff 校验、JSON/summary 生成和结果打包；job 输出写文件包含在计时内。warmup 只运行并记录到 `warmups.csv`，不参与统计也不校验；repeat 每轮只记录 `run_ms` 和退出码。正确性校验在计时循环之后执行，`--verify=last` 默认只校验最后一轮 repeat，`--verify=all` 校验所有 repeat，`--verify=none` 跳过校验。breakdown/profiling 应作为单独诊断模式，不混入默认 benchmark。

两 VM 场景建议先使用如下基线：

- `REDUCERS=4`：降低单个 reducer partition 的峰值 payload，同时保持请求数量可控。
- `PAYLOAD_MAX=4194304`：给 chunk header 和 KV payload 留出余量，避免普通 wordcount 输入过早碰到单请求上限。
- `MAP_CHUNK=524288`：map 输入切片足够细，便于观察通信开销，又不会制造过多调度请求。

如果 workload 的词表很小、热点 key 很集中，可以继续增大 `REDUCERS` 或减小 `MAP_CHUNK`；如果关注纯吞吐并且已经确认不会触发 payload 上限，可以增大 `MAP_CHUNK` 减少 map 请求数量。
