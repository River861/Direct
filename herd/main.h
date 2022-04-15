#include "hrd.h"

#include <stdint.h>

/*
 * The polling logic in HERD requires the following:
 * 1. 0 < MICA_OP_GET < MICA_OP_PUT < HERD_OP_GET < HERD_OP_PUT
 * 2. HERD_OP_GET = MICA_OP_GET + HERD_MICA_OFFSET
 * 3. HERD_OP_PUT = MICA_OP_PUT + HERD_MICA_OFFSET
 *
 * This allows us to detect HERD requests by checking if the request region
 * opcode is more than MICA_OP_PUT. And then we can convert a HERD opcode to
 * a MICA opcode by subtracting HERD_MICA_OFFSET from it.
 */
#define HERD_MICA_OFFSET 10
#define HERD_OP_GET (MICA_OP_GET + HERD_MICA_OFFSET)
#define HERD_OP_PUT (MICA_OP_PUT + HERD_MICA_OFFSET)

#define HERD_NUM_BKTS (2 * 1024 * 1024)
#define HERD_LOG_CAP (1024 * 1024 * 1024)

//#define HERD_NUM_KEYS (8 * 1024 * 1024)
#define HERD_NUM_KEYS  100000 // MICA_NUM_KEYS
#define HERD_KEYS_LINK_LENGTH 20
#define HERD_OP_KEY_LEN 8
#define HERD_STR_KEY_LEN 300
#define TEST_TIMES 10000000
#define LATENCY_TEST_TIMES 10000


#define HERD_VALUE_SIZE 1024 // MICA_HERD_VALUE_SIZE
#define HERD_SPACE_SIZE (HERD_VALUE_SIZE + 64)
/* Request sizes */
#define HERD_GET_REQ_SIZE (16 + 1) /* 16 byte key + opcode */

/* Key, op, len, val */
#define HERD_PUT_REQ_SIZE (16 + 1 + 1 + HERD_VALUE_SIZE)

/* Configuration options */
#define MAX_SERVER_PORTS 4
#define NUM_WORKERS 4
#define NUM_CLIENTS 8  // 32
#define NUM_CLIENT_NODE 16
#define WORKLOAD_DIR "upd-workloads"
#define WORKLOAD_ID "upd90"
#define NUM_MEMORY 2  // MICA_NUM_MEMORY
#define NUM_REPLICATION 2  //MICA_NUM_REPLICATION
#define BENCHMARK_TYPE 3  // 0-micro_benchmark, 1-macro_benchmark, 2-rdma_testing, 3-lock-testing
#define TEST_TYPE 0  // 0=throughput, 1=lattency; 0=one-size, 1=two-size(for rdma-testing)
#define RDMA_TEST_SIZE 1024
#define TPT_INSERT_TEST_TIME 0.5
#define TPT_TEST_TIME 10.0
#define TPT_TEST_OPNUM 10000
#define TWOSIDE_PREPOST_RECV_NUM 100
#define TWOSIDE_SIGNAL_GAP 10000
#define ONESIDE_POST_SIZE 100

/* Performance options */
#define WINDOW_SIZE                                                            \
  MITSUME_MICA_WINDOW_SIZE /* Outstanding requests kept by each client */
#define NUM_UD_QPS 1       /* Number of UD QPs per port */
#define USE_POSTLIST 1

#define UNSIG_BATCH 64 /* XXX Check if increasing this helps */
#define UNSIG_BATCH_ (UNSIG_BATCH - 1)

/* SHM key for the 1st request region created by master. ++ for other RRs.*/
#define MASTER_SHM_KEY 24
//#define RR_SIZE (16 * 1024 * 1024) /* Request region size */
#define RR_SIZE (16 * 1024 * 1024) /* Request region size */
#define OFFSET(wn, cn, ws)                                                     \
  ((wn * NUM_CLIENTS * WINDOW_SIZE) + (cn * WINDOW_SIZE) + ws)

struct thread_params {
  int id;
  int base_port_index;
  int num_server_ports;
  int num_client_ports;
  int update_percentage;
  int postlist;
  int num_memory;
};

struct server_thread_params {
  int id;
  struct hrd_ctrl_blk *cb;
};

void *run_master(void *arg);
void *run_worker(void *arg);
void *run_client(void *arg);
void *run_memory(void *arg);

void *test_lock_throughput(int clt_gid, struct hrd_ctrl_blk *mem_cb, struct hrd_qp_attr ****mr_qp_list, struct hrd_qp_attr ****backupmr_qp_list,
                         struct ibv_mr *input_mr, struct ibv_mr *output_mr, void *read_write_addr);

void *test_micro_throughput(int clt_gid, struct hrd_ctrl_blk *mem_cb, struct hrd_qp_attr ****mr_qp_list, struct hrd_qp_attr ****backupmr_qp_list,
                         struct ibv_mr *input_mr, struct ibv_mr *output_mr, void *read_write_addr);

void *test_micro_latency(int clt_gid, struct hrd_ctrl_blk *mem_cb, struct hrd_qp_attr ****mr_qp_list, struct hrd_qp_attr ****backupmr_qp_list,
                         struct ibv_mr *input_mr, struct ibv_mr *output_mr, void *read_write_addr);

int macro_preload(int clt_gid, void *output_space, void *input_space, char ***op_key, char ***write_key,
                   struct hrd_ctrl_blk *mem_cb, struct hrd_qp_attr ****mr_qp_list, struct hrd_qp_attr ****backupmr_qp_list,
                   struct ibv_mr *input_mr, struct ibv_mr *output_mr, void *read_write_addr);

void *test_macro_throughput(int clt_gid, int test_times, char **op_key, char **write_key,
                         struct hrd_ctrl_blk *mem_cb, struct hrd_qp_attr ****mr_qp_list, struct hrd_qp_attr ****backupmr_qp_list,
                         struct ibv_mr *input_mr, struct ibv_mr *output_mr, void *read_write_addr);

void *test_macro_latency(int clt_gid, int test_times, char **op_key, char **write_key,
                         struct hrd_ctrl_blk *mem_cb, struct hrd_qp_attr ****mr_qp_list, struct hrd_qp_attr ****backupmr_qp_list,
                         struct ibv_mr *input_mr, struct ibv_mr *output_mr, void *read_write_addr);

void syn_thread(int clt_gid);
int stick_this_thread_to_core(int core_id);

void *test_rdma_throughput(int clt_gid, struct hrd_ctrl_blk *mem_cb, struct hrd_qp_attr ****mr_qp_list, struct hrd_qp_attr ****backupmr_qp_list,
                         struct ibv_mr *input_mr, struct ibv_mr *output_mr);
