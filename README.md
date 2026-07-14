# mr-distributed

基于 HydraRPC 的多机 MapReduce 示例实现，通信方式使用 `hydrarpc-poll`：master 和每个 worker 通过一段共享内存后端建立 HydraRPC request/response 队列，worker 轮询请求队列处理 map/reduce/shutdown 任务。

当前内置任务是 word count。

## 构建

```bash
cmake -S . -B build -DHYDRARPC_ROOT=/home/liulingling/GVM/HydraRPC
cmake --build build
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

每个 worker 使用一段能被 master 和该 worker 同时访问的 HydraRPC 后端，例如 CXL/DAX、ivshmem resource，或实验环境中的共享文件后端：

```bash
# master 节点
./build/mr_hydra master \
  --backend=ivshmem \
  --input=input.txt \
  --output=wc.out \
  --worker=/sys/bus/pci/devices/0000:00:05.0/resource2

# worker 节点
./build/mr_hydra worker \
  --backend=ivshmem \
  --path=/sys/bus/pci/devices/0000:00:05.0/resource2 \
  --worker-id=0
```

常用参数：

- `--backend=file|devdax|ivshmem`：HydraRPC 后端。
- `--worker=PATH`：master 添加一个 worker 后端路径，可重复。
- `--workers=A,B,C`：master 一次添加多个 worker 后端路径。
- `--path=PATH`：worker 使用的后端路径。
- `--payload-max=N`：单条 HydraRPC payload 上限，默认 `1048576`。
- `--map-chunk=N`：map 输入分片大小，默认约为 `payload-max / 4`。
- `--queue-capacity=N`：HydraRPC 队列容量，默认 `4096`。
- `--create`：worker 侧也创建/初始化后端，主要用于调试；正常多机运行由 master 创建。

通信轮询固定使用 HydraRPC 的 busy poll 等待路径，即 `hydrarpc-poll`。
