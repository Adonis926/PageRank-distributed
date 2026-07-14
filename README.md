# mr-distributed

基于 HydraRPC 的多机 MapReduce 示例实现，通信方式使用 `hydrarpc-poll`：master 和每个 worker 通过一段共享内存后端建立 HydraRPC request/response 队列，worker 轮询请求队列处理 map/reduce/shutdown 任务。

当前示例应用是 word count。分布式 runtime 保留了接近单机多线程版的结构：应用只实现 `map` / `reduce`，map 和 reduce 都通过 `MR_Emit` 产出 KV，runtime 负责 partition、排序、`Getter get_next`、HydraRPC 调度和结果写出。

## 运行时结构

当前数据流分为三个阶段：

1. master 将输入按 `--map-chunk` 切片，向 worker 发送 map 任务。
2. worker 在本地执行 map、partition、排序，并执行 count 型 combiner：同一 partition 内相同 key 且 value 均为十进制无符号整数时会先累加成一条 KV；不能安全累加的 KV 会原样保留。
3. master 汇总中间结果并按 reducer partition 进行 shuffle。shuffle 使用 `ShuffleChunkHeader` 分块发送，每个 partition 至少包含一个带 `SHUFFLE_CHUNK_FIRST` 和 `SHUFFLE_CHUNK_LAST` 标志的 chunk；大 partition 会拆成多个 chunk，worker 端按 `job_id`、`reducer_id`、`partition_seq`、`chunk_seq` 顺序重组后再执行 reduce。

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

`mapper_id` 当前由 master 侧 shuffle 填 0；`record_count` 用于描述 chunk 内 KV 条数，便于后续统计和协议扩展。

## 构建

```bash
cmake -S . -B build
cmake --build build
```

默认按源码相对路径查找 HydraRPC：`../../HydraRPC`。如果目录布局不同，可以显式指定：

```bash
cmake -S . -B build -DHYDRARPC_ROOT=/path/to/HydraRPC
```

## 本地两 worker 烟测

默认由 master 创建并初始化 file backend，所以先启动 master，让它创建共享文件并等待 worker 响应：

```bash
./build/mr_hydra master \
  --input=input.txt \
  --output=wc.out \
  --workers=/tmp/mr-worker0.raw,/tmp/mr-worker1.raw
```

然后在另外两个终端启动 worker：

```bash
./build/mr_hydra worker --path=/tmp/mr-worker0.raw --worker-id=0
./build/mr_hydra worker --path=/tmp/mr-worker1.raw --worker-id=1
```

master 默认创建并初始化 file backend；worker 默认 attach。master 结束时会向所有 worker 发送 shutdown。

## 多机运行

每个 worker 使用一段能被 master 和该 worker 同时访问的 HydraRPC 后端，例如 devdax、ivshmem resource，或实验环境中的共享文件后端。当前 QEMU 测试默认使用 devdax：

```bash
# master 节点
./build/mr_hydra master \
  --backend=devdax \
  --input=input.txt \
  --output=wc.out \
  --worker=/dev/dax0.0 \
  --size=2097152000

# worker 节点
./build/mr_hydra worker \
  --backend=devdax \
  --path=/dev/dax0.0 \
  --worker-id=0 \
  --size=2097152000
```

常用参数：

- `--backend=file|devdax|ivshmem`：HydraRPC 后端。
- `--worker=PATH`：master 添加一个 worker 后端路径，可重复。
- `--workers=A,B,C`：master 一次添加多个 worker 后端路径。
- `--path=PATH`：worker 使用的后端路径。
- `--reducers=N`：reducer partition 数量，默认等于 worker 数量。
- `--payload-max=N`：单条 HydraRPC payload 上限，默认 `1048576`。
- `--map-chunk=N`：map 输入分片大小，默认约为 `payload-max / 4`。
- `--queue-capacity=N`：HydraRPC 队列容量，默认 `4096`。
- `--size=N`：HydraRPC 后端映射大小；devdax 场景应与设备映射大小一致，例如 `2097152000`。
- `--create`：worker 侧也创建/初始化后端，主要用于调试；正常多机运行由 master 创建。

通信轮询固定使用 HydraRPC 的 busy poll 等待路径，即 `hydrarpc-poll`。

## Benchmark 脚本

推荐从仓库根目录使用通用脚本：

```bash
scripts/mr-bench.sh \
  --impl=distributed \
  --dist-mode=ssh \
  --dist-master-host=127.0.0.1:2401 \
  --dist-worker=127.0.0.1:2402,/dev/dax0.0,/dev/dax0.0 \
  --dist-backend=devdax \
  --dist-size=2097152000 \
  --dist-reducers=4 \
  --dist-payload-max=4194304 \
  --dist-map-chunk=524288
```

`--dist-worker` 推荐使用逗号分隔格式 `HOST,MASTER_BACKEND_PATH,WORKER_BACKEND_PATH`；对 devdax 来说，master 和 worker 侧路径通常都是 `/dev/dax0.0`，但仍保留两端路径字段，便于不同 VM 中设备名不一致时覆盖。

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
SIZE=2097152000 \
scripts/example.sh
```

脚本不会启动虚拟机；QEMU、ivshmem 后端和 SSH 端口转发需要提前准备好。SSH 只作为控制面，用于远端 build、启动 master/worker、复制输入和结果；HydraRPC 的共享内存后端才是被测数据面。远端 configure/build 输出会写入结果目录中的 `runs/distributed/logs/prepare_<host>.log`，每次运行结束都会生成结果目录和同名 `.tar.gz` 压缩包。

两 VM 场景建议先使用如下基线：

- `REDUCERS=4`：降低单个 reducer partition 的峰值 payload，同时保持请求数量可控。
- `PAYLOAD_MAX=4194304`：给 chunk header 和 KV payload 留出余量，避免普通 wordcount 输入过早碰到单请求上限。
- `MAP_CHUNK=524288`：map 输入切片足够细，便于观察通信开销，又不会制造过多调度请求。

如果 workload 的词表很小、热点 key 很集中，可以继续增大 `REDUCERS` 或减小 `MAP_CHUNK`；如果关注纯吞吐并且已经确认不会触发 payload 上限，可以增大 `MAP_CHUNK` 减少 map 请求数量。
