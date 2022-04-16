## Direct

仅仅作为测试使用

### 0. Environment
需要有IB网卡

```shell
apt-get install libibverbs-dev
apt-get install libnuma-dev
apt-get install libmemcached-dev
apt install numactl
```

* master node（也是第一个compute node）
    * 开启memcached，并且设置对外网可见（修改sudo vim /etc/memcached.conf）
    * 要使用tmux，一个窗口运行master，另一个运行compute client

* compute node
    * echo 3072 > /proc/sys/vm/nr_hugepages

* memory node
    * echo 2048 > /proc/sys/vm/nr_hugepages

### 1. Config
在`main.h`中，列举几个重要的，详情看client.c的具体代码（简单的分支结构）：

* NUM_CLIENTS: 每个结点的运行的clients数量，固定为8
* NUM_CLIENT_NODE：compute node数量
* WORKLOAD_DIR：workloads路径，某些测试需要
* WORKLOAD_ID：workload ID, 比如a-d, upd90, ...
* NUM_MEMORY: memory node数量
* NUM_REPLICATION：副本数量
* BENCHMARK_TYPE：执行的测试：0-micro_benchmark, 1-macro_benchmark, 2-rdma_testing, 3-lock-testing
* TEST_TYPE：测试的类型：0=throughput, 1=lattency; 0=one-size, 1=two-size(for rdma-testing)

在`hrd.h`中，设置要使用的RDMA网卡设备名：
* DEV_NAME

### 2. Run
* 修改脚本中的ip
   * run-servers.sh、run-machine.sh、run-memory中的HRD_REGISTRY_IP


* 编译
```
sh do.sh
```

* master node
    * tmux
    * tmux split
        其中一个运行
        ```
        ./run-servers.sh
        ```
        另一个运行
        ```
        ./run-machine.sh 0
        ```

* compute node
    ```
    ./run-machine.sh [machine_id]
    ```
    machine_id从0开始，master就是那个0

* memory node
    ```
    ./run-memory.sh [memory_id]
    ```
    memory_id从0开始
