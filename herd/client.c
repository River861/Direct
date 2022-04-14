#include <stdio.h>
#define __USE_GNU
#include <sched.h>
#include <pthread.h>
#include "hrd.h"
#include "main.h"
#include "mica.h"
#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>


#define DGRAM_BUF_SIZE 4096

#define ONE_LOCK 3378

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

long long send_traffic[NUM_CLIENTS];
long long recv_traffic[NUM_CLIENTS];
static uint32_t crc32_tab[] = {
    0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
    0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
    0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
    0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
    0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9,
    0xfa0f3d63, 0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
    0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa, 0x42b2986c,
    0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
    0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,
    0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
    0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106,
    0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
    0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,
    0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
    0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,
    0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
    0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,
    0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
    0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9, 0x5005713c, 0x270241aa,
    0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
    0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
    0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
    0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,
    0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
    0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,
    0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
    0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,
    0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
    0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,
    0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
    0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
    0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
    0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,
    0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
    0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,
    0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
    0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,
    0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
    0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,
    0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
    0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693,
    0x54de5729, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
    0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d};

uint32_t crc32(uint32_t crc, const void *buf, size_t size);
uint32_t crc32(uint32_t crc, const void *buf, size_t size) {
  const uint8_t *p;
  p = buf;
  crc = crc ^ ~0U;

  while (size--)
    crc = crc32_tab[(crc ^ *p++) & 0xFF] ^ (crc >> 8);

  return crc ^ ~0U;
}

#define NUMBER64_1 11400714785074694791ULL
#define NUMBER64_2 14029467366897019727ULL
#define NUMBER64_3 1609587929392839161ULL
#define NUMBER64_4 9650029242287828579ULL
#define NUMBER64_5 2870177450012600261ULL
#define hash_get64bits(x) hash_read64_align(x, align)
#define hash_get32bits(x) hash_read32_align(x, align)
#define shifting_hash(x, r) ((x << r) | (x >> (64 - r)))

#define TO64(x) (((U64_INT *)(x))->v)
#define TO32(x) (((U32_INT *)(x))->v)

typedef struct U64_INT {
    uint64_t v;
} U64_INT;

typedef struct U32_INT {
    uint32_t v;
} U32_INT;

static uint64_t hash_read64_align(const void * ptr, uint32_t align) {
    if (align == 0) {
        return TO64(ptr);
    }
    return *(uint64_t *)ptr;
}

static uint32_t hash_read32_align(const void * ptr, uint32_t align) {
    if (align == 0) {
        return TO32(ptr);
    }
    return *(uint32_t *)ptr;
}


static uint64_t string_key_hash_computation(const void * data, uint64_t length, 
        uint64_t seed, uint32_t align) {
    const uint8_t * p = (const uint8_t *)data;
    const uint8_t * end = p + length;
    uint64_t hash;

    if (length >= 32) {
        const uint8_t * const limitation  = end - 32;
        uint64_t v1 = seed + NUMBER64_1 + NUMBER64_2;
        uint64_t v2 = seed + NUMBER64_2;
        uint64_t v3 = seed + 0;
        uint64_t v4 = seed - NUMBER64_1;

        do {
            v1 += hash_get64bits(p) * NUMBER64_2;
            p += 8;
            v1 = shifting_hash(v1, 31);
            v1 *= NUMBER64_1;
            v2 += hash_get64bits(p) * NUMBER64_2;
            p += 8;
            v2 = shifting_hash(v2, 31);
            v2 *= NUMBER64_1;
            v3 += hash_get64bits(p) * NUMBER64_2;
            p += 8;
            v3 = shifting_hash(v3, 31);
            v3 *= NUMBER64_1;
            v4 += hash_get64bits(p) * NUMBER64_2;
            p += 8;
            v4 = shifting_hash(v4, 31);
            v4 *= NUMBER64_1;
        } while (p <= limitation);

        hash = shifting_hash(v1, 1) + shifting_hash(v2, 7) + shifting_hash(v3, 12) + shifting_hash(v4, 18);

        v1 *= NUMBER64_2;
        v1 = shifting_hash(v1, 31);
        v1 *= NUMBER64_1;
        hash ^= v1;
        hash = hash * NUMBER64_1 + NUMBER64_4;

        v2 *= NUMBER64_2;
        v2 = shifting_hash(v2, 31);
        v2 *= NUMBER64_1;
        hash ^= v2;
        hash = hash * NUMBER64_1 + NUMBER64_4;

        v3 *= NUMBER64_2;
        v3 = shifting_hash(v3, 31);
        v3 *= NUMBER64_1;
        hash ^= v3;
        hash = hash * NUMBER64_1 + NUMBER64_4;

        v4 *= NUMBER64_2;
        v4 = shifting_hash(v4, 31);
        v4 *= NUMBER64_1;
        hash ^= v4;
        hash = hash * NUMBER64_1 + NUMBER64_4;
    } else {
        hash = seed + NUMBER64_5;
    }

    hash += (uint64_t)length;

    while (p + 8 <= end) {
        uint64_t k1 = hash_get64bits(p);
        k1 *= NUMBER64_2;
        k1 = shifting_hash(k1, 31);
        k1 *= NUMBER64_1;
        hash ^= k1;
        hash = shifting_hash(hash, 27) * NUMBER64_1 + NUMBER64_4;
        p += 8;
    }

    if (p + 4 <= end) {
        hash ^= (uint64_t)(hash_get32bits(p)) * NUMBER64_1;
        hash = shifting_hash(hash, 23) * NUMBER64_2 + NUMBER64_3;
        p += 4;
    }

    while (p < end) {
        hash ^= (*p) * NUMBER64_5;
        hash = shifting_hash(hash, 11) * NUMBER64_1;
        p ++;
    }

    hash ^= hash >> 33;
    hash *= NUMBER64_2;
    hash ^= hash >> 29;
    hash *= NUMBER64_3;
    hash ^= hash >> 32;

    return hash;
}

uint64_t VariableLengthHash(const void * data, uint64_t length, uint64_t seed) {
    if ((((uint64_t)data) & 7) == 0) {
        return string_key_hash_computation(data, length, seed, 1);
    }
    return string_key_hash_computation(data, length, seed, 0);
}

int StrHash(char *str) {
  int len = strlen(str);
  //  const unsigned int fnv_prime = 0x811C9DC5;
  //  unsigned int hash      = 0;
  //  unsigned int i         = 0;
  //  for(i = 0; i < len; str++, i++)
  //  {  
  //     hash *= fnv_prime;
  //     hash ^= (*str);
  //  }  
  //  return (int)(hash % HERD_NUM_KEYS);
  return (int)(VariableLengthHash((void*)str, (uint64_t)len, 0) % HERD_NUM_KEYS);
}

// /* Generate a random permutation of [0, n - 1] for client @clt_gid */
// int *get_random_permutation(int n, int clt_gid, uint64_t *seed) {
//   int i, j, temp;
//   assert(n > 0);

//   /* Each client uses a different range in the cycle space of fastrand */
//   for (i = 0; i < clt_gid * HERD_NUM_KEYS; i++) {
//     hrd_fastrand(seed);
//   }

//   printf("client %d: creating a permutation of 0--%d. This takes time..\n",
//          clt_gid, n - 1);

//   int *log = (int *)malloc(n * sizeof(int));
//   assert(log != NULL);
//   for (i = 0; i < n; i++) {
//     log[i] = i;
//   }

//   printf("\tclient %d: shuffling..\n", clt_gid);
//   for (i = n - 1; i >= 1; i--) {
//     j = hrd_fastrand(seed) % (i + 1);
//     temp = log[i];
//     log[i] = log[j];
//     log[j] = temp;
//   }
//   printf("\tclient %d: done creating random permutation\n", clt_gid);

//   return log;
// }

int get_load_file(char ***key) {
  char **write_key;
  char *tmp;
  FILE *fp;
  char filepath[100];
  char *line = NULL;
  size_t len = 0;
  ssize_t read;
  // const char filenames[1][35] = {
  //   "workloads/workloada.spec_load"
  // };
  int i;
  i = 0;

  write_key = (char **)malloc(sizeof(char*) * (TEST_TIMES + 5));
  sprintf(filepath, "%s/workload%s.spec_load", WORKLOAD_DIR, WORKLOAD_ID);

  printf("start reading %s\n", filepath);
  fflush(stdout);
  fp = fopen(filepath, "r");
  if (!fp) {
    printf("Fail to open file\n");
    exit(0);
  }
  while ((read = getline(&line, &len, fp)) != -1) {
    if (i == TEST_TIMES)
      break;
    tmp = (char *)malloc(HERD_STR_KEY_LEN);
    sscanf(line, "INSERT usertable %s", tmp);
    write_key[i] = tmp;
    // printf("FUCK0: %s\n", write_key[i]);
    i++;
  }

  printf("thread-0 in client-node-0 finish reading %d load op.\n", i);
  fflush(stdout);
  *key = write_key;
  return i;
}

int get_file(char ***op, char ***key, int thread_id) {
  char **write_key;
  char *tmp1, *tmp2;
  char **op_key;
  FILE *fp;
  char filepath[100];
  char *line = NULL;
  size_t len = 0;
  ssize_t read;
  // const char filenames[4][35] = {
  //   "workloads/workloada.spec_trans", "workloads/workloadd.spec_trans",
  //   "workloads/workloadb.spec_trans", "workloads/workloadc.spec_trans",
  // };
  int i;
  i = 0;

  op_key = (char **)malloc(sizeof(char*) * (TEST_TIMES + 5));
  write_key = (char **)malloc(sizeof(char*) * (TEST_TIMES + 5));

  sprintf(filepath, "%s/workload%s.spec_trans%d", WORKLOAD_DIR, WORKLOAD_ID, thread_id);

  printf("start reading %s\n", filepath);
  fflush(stdout);
  fp = fopen(filepath, "r");
  if (!fp) {
    printf("Fail to open file\n");
    exit(0);
  }
  while ((read = getline(&line, &len, fp)) != -1) {
    if (i == TEST_TIMES)
      break;
    tmp1 = (char *)malloc(HERD_OP_KEY_LEN);
    tmp2 = (char *)malloc(HERD_STR_KEY_LEN);
    sscanf(line, "%s usertable %s", tmp1, tmp2);
    op_key[i] = tmp1;
    write_key[i] = tmp2;
    i++;
  }


  printf("thread-%d finish reading %d\n", thread_id, i);
  fflush(stdout);
  *op = op_key;
  *key = write_key;
  return i;
}

void userspace_one_poll(struct hrd_ctrl_blk *mem_cb, int tar_mem) {
  struct ibv_wc wc[MITSUME_MICA_WINDOW_SIZE];
  // printf("FUCK: tar_mem: %d\n", tar_mem);
  hrd_poll_cq(mem_cb->conn_cq[tar_mem], 1, wc);
}

int test_oneside_write_read(struct hrd_ctrl_blk *mem_cb,
                            struct hrd_qp_attr ****mr_qp_list,
                            struct ibv_mr *input_mr,
                            struct ibv_mr *output_mr,
                            int clt_gid, int *counter, int round_idx) {
  int ret, i;
  int request_size = RDMA_TEST_SIZE;
  struct hrd_qp_attr ***mr_qp = mr_qp_list[0];

  struct ibv_sge write_sges[ONESIDE_POST_SIZE+5], read_sges[ONESIDE_POST_SIZE+5];
  for(i = 0; i < ONESIDE_POST_SIZE; i ++) {
    write_sges[i].length = request_size;
    write_sges[i].addr = (uintptr_t)output_mr->addr + (uintptr_t)HERD_SPACE_SIZE * i;  // 本地地址
    write_sges[i].lkey = output_mr->lkey;

    read_sges[i].length = request_size;
    read_sges[i].addr = (uintptr_t)input_mr->addr + (uintptr_t)HERD_SPACE_SIZE * i;
    read_sges[i].lkey = input_mr->lkey;
  }

  struct ibv_send_wr wr_list[2*ONESIDE_POST_SIZE+5], *bad_wr;
  memset(wr_list, 0, 2 * ONESIDE_POST_SIZE * sizeof(struct ibv_send_wr));

  // 1.构造链表
  int pre_i = -1, head_i = -1, cnt = 0;
  for (i = 0; i < 2*ONESIDE_POST_SIZE; i += 2) {
    if (round_idx && strcmp((const char *)write_sges[i/2].addr, (const char *)read_sges[i/2].addr)) {  // 上一轮还没写完
      continue;
    }
    if(round_idx) cnt += 2;  // 只数已经被写好的
    sprintf((char *)write_sges[i/2].addr, "%d-%d-%d", clt_gid, round_idx, i);  // 这一轮要写的内容

    if (head_i < 0) head_i = i;  // 链表头
    else wr_list[pre_i].next = &wr_list[i];
    wr_list[i].next = &wr_list[i+1];
    pre_i = i+1;

    // write
    wr_list[i].sg_list = &write_sges[i/2];
    wr_list[i].num_sge = 1;
    wr_list[i].opcode = IBV_WR_RDMA_WRITE;

    // read
    wr_list[i+1].sg_list = &read_sges[i/2];
    wr_list[i+1].num_sge = 1;
    wr_list[i+1].opcode = IBV_WR_RDMA_READ;

    int offset = clt_gid * ONESIDE_POST_SIZE + i / 2;
    wr_list[i].wr.rdma.remote_addr = wr_list[i+1].wr.rdma.remote_addr = mr_qp[offset / HERD_KEYS_LINK_LENGTH][0]->buf_addr + (uintptr_t)HERD_SPACE_SIZE * (offset % HERD_KEYS_LINK_LENGTH);  // 目标地址
    wr_list[i].wr.rdma.rkey = wr_list[i+1].wr.rdma.rkey = mr_qp[offset / HERD_KEYS_LINK_LENGTH][0]->rkey;
  }
  if(head_i >= 0) {
    wr_list[pre_i].next = NULL;
    wr_list[head_i].send_flags = IBV_SEND_SIGNALED;
    wr_list[head_i].wr_id = clt_gid;

    // 2. 发送链表
    ret = ibv_post_send(mem_cb->conn_qp[0], &wr_list[head_i], &bad_wr);
    CPE(ret, "[ERROR] test rdma twoside write", ret);

    struct ibv_wc wc;
    hrd_poll_cq(mem_cb->conn_cq[0], 1, &wc);
    assert(wc.wr_id == clt_gid);
  }

  *counter = cnt;
  return ret;
}

void test_twoside_prepost_recv(struct ibv_qp *qp,
                               uintptr_t addr, uint32_t lkey, int qp_id) {
  int ret;
  int request_size = RDMA_TEST_SIZE;

  struct ibv_recv_wr head_rwr[TWOSIDE_PREPOST_RECV_NUM+5], *bad_rwr;
  memset(head_rwr, 0, TWOSIDE_PREPOST_RECV_NUM * sizeof(struct ibv_recv_wr));
  struct ibv_sge recv_sge;
  recv_sge.length = request_size;
  recv_sge.addr = addr;
  recv_sge.lkey = lkey;

  int i;
  for (i = 0; i < TWOSIDE_PREPOST_RECV_NUM; i ++) {
    if (i > 0) head_rwr[i - 1].next = &head_rwr[i];
    head_rwr[i].sg_list = &recv_sge;
    head_rwr[i].num_sge = 1;
    head_rwr[i].wr_id = qp_id * TWOSIDE_PREPOST_RECV_NUM + i;
  }
  head_rwr[i-1].next = NULL;
  ret = ibv_post_recv(qp, head_rwr, &bad_rwr);
  CPE(ret, "[ERROR] test rdma twoside prepost recv", ret);
}

int test_twoside_write_read(struct hrd_ctrl_blk *mem_cb,
                            struct ibv_mr *input_mr,
                            struct ibv_mr *output_mr,
                            int clt_gid, uint64_t nb_tx) {
  int ret;
  int request_size = RDMA_TEST_SIZE;
  memset(output_mr->addr, 0, request_size+1);
  sprintf(output_mr->addr, "%d", clt_gid);  // 内容填编号

  // 1. 先send
  struct ibv_sge test_sge;
  test_sge.length = request_size;
  test_sge.addr = (uintptr_t)output_mr->addr;  // 本地地址
  test_sge.lkey = output_mr->lkey;  // 本地内存key

  struct ibv_send_wr wr, *bad_send_wr;
  memset(&wr, 0, sizeof(struct ibv_send_wr));
  wr.opcode = IBV_WR_SEND;
  wr.num_sge = 1;
  wr.next = NULL;
  wr.sg_list = &test_sge;
  wr.wr_id = nb_tx;

  // wr.send_flags = IBV_SEND_SIGNALED;
  // ret = ibv_post_send(mem_cb->conn_qp[clt_gid % 8], &wr, &bad_send_wr);
  // CPE(ret, "[ERROR] test rdma twoside send", ret);
  // struct ibv_wc send_wc;
  // hrd_poll_cq(mem_cb->conn_cq[clt_gid % 8], 1, &send_wc);
  // // assert((send_wc.opcode == IBV_WC_SEND) && (send_wc.wr_id == nb_tx));

  if (nb_tx % TWOSIDE_SIGNAL_GAP == 0) {  // 间隔一定数量signal一次
    wr.send_flags = IBV_SEND_SIGNALED;
    ret = ibv_post_send(mem_cb->conn_qp[0], &wr, &bad_send_wr);
    CPE(ret, "[ERROR] test rdma twoside send", ret);
    struct ibv_wc send_wc;
    hrd_poll_cq(mem_cb->conn_cq[0], 1, &send_wc);
    // assert((send_wc.opcode == IBV_WC_SEND) && (send_wc.wr_id == nb_tx));
  }
  else {
    ret = ibv_post_send(mem_cb->conn_qp[0], &wr, &bad_send_wr);
    CPE(ret, "[ERROR] test rdma twoside send", ret);
  }

  // 2. poll一个recv
  struct ibv_wc recv_wc;
  hrd_poll_cq(mem_cb->conn_cq[0], 1, &recv_wc);
  // assert((recv_wc.opcode == IBV_WC_RECV) && (recv_wc.wr_id / TWOSIDE_PREPOST_RECV_NUM == clt_gid));

  // 3. 检验
  assert(!(strcmp((const char *)(output_mr->addr), (const char*)(input_mr->addr))));
  // if(strcmp((const char *)(output_mr->addr), (const char*)(input_mr->addr))) {
  //   printf("send: %s\n", (const char *)(output_mr->addr));
  //   printf("recv: %s\n", (const char*)(input_mr->addr));
  //   exit(-1);
  // }

  return ret;
}

void *test_twoside_server(void *arg) {
  struct server_thread_params params = *(struct server_thread_params *)arg;
  int tid = params.id;
  struct hrd_ctrl_blk *cb = params.cb;

  int ret = stick_this_thread_to_core(tid * 2);  // 每个server线程绑定到一个cpu上
  assert(ret == 0);

  int request_size = RDMA_TEST_SIZE;
  int recv_cnt[NUM_WORKERS + NUM_CLIENT_NODE * NUM_CLIENTS + 5];
  struct ibv_wc recv_wc;

  // 先prepost一堆recv
  for (int qp_id = NUM_WORKERS; qp_id < NUM_WORKERS + NUM_CLIENT_NODE * NUM_CLIENTS; qp_id ++) {
    if (qp_id % 8 == tid) {
      uintptr_t addr = (uintptr_t)(cb->conn_buf + (uintptr_t)HERD_SPACE_SIZE * qp_id);
      test_twoside_prepost_recv(cb->conn_qp[qp_id], addr, cb->conn_buf_mr->rkey, qp_id);
      recv_cnt[qp_id] =  TWOSIDE_PREPOST_RECV_NUM;
    }
  }

  // long long cnt = 0;
  while(1) {
    // 1. 先poll客户端的send
    hrd_poll_cq(cb->conn_cq[tid], 1, &recv_wc);  // 一次poll一个 wc判断是哪个client qp

    int qp_id = (int)recv_wc.wr_id / TWOSIDE_PREPOST_RECV_NUM;
    assert(qp_id % 8 == tid);
    uintptr_t addr = (uintptr_t)(cb->conn_buf + (uintptr_t)HERD_SPACE_SIZE * qp_id);

    recv_cnt[qp_id] --;
    if (recv_cnt[qp_id] <= 0) {  // 补充post recv
      test_twoside_prepost_recv(cb->conn_qp[qp_id], addr, cb->conn_buf_mr->rkey, qp_id);
      recv_cnt[qp_id] =  TWOSIDE_PREPOST_RECV_NUM;
    }

    // 2. send
    struct ibv_sge test_sge;
    struct ibv_send_wr wr, *bad_send_wr;

    test_sge.length = request_size;
    test_sge.addr = addr;
    test_sge.lkey = cb->conn_buf_mr->rkey;  // key值是一样的

    memset(&wr, 0, sizeof(struct ibv_send_wr));
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.next = NULL;
    wr.sg_list = &test_sge;
    wr.wr_id = recv_cnt[qp_id];

    wr.send_flags = IBV_SEND_SIGNALED;
    ret = ibv_post_send(cb->conn_qp[qp_id], &wr, &bad_send_wr);
    CPE(ret, "[ERROR] test rdma twoside send 1", ret);
    struct ibv_wc send_wc;
    hrd_poll_cq(cb->conn_cq[tid + 8], 1, &send_wc);

    // cnt = (cnt + 1) % TWOSIDE_SIGNAL_GAP;
    // if (cnt % TWOSIDE_SIGNAL_GAP == 0) {  // 间隔一定数量signal一次
    //   wr.send_flags = IBV_SEND_SIGNALED;
    //   ret = ibv_post_send(cb->conn_qp[qp_id], &wr, &bad_send_wr);
    //   CPE(ret, "[ERROR] test rdma twoside send 1", ret);
    //   struct ibv_wc send_wc;
    //   hrd_poll_cq(cb->conn_cq[tid + 8], 1, &send_wc);
    // }
    // else {
    //   ret = ibv_post_send(cb->conn_qp[qp_id], &wr, &bad_send_wr);
    //   CPE(ret, "[ERROR] test rdma twoside send 2", ret);
    // }
  }

  return NULL;
}

int userspace_one_write(unsigned long long tar_key, int str_key_idx, int tar_mem,
                        struct hrd_ctrl_blk *mem_cb, struct ibv_mr *mitsume_mr,
                        int request_size, struct hrd_qp_attr ****mr_qp_list,
                        int clt_gid) {
  struct ibv_sge test_sge;
  struct ibv_send_wr wr, *bad_send_wr;
  int ret;
  struct hrd_qp_attr ***mr_qp;
  // unsigned long long tar_key = op[I]->key.bkt;
  mr_qp = mr_qp_list[tar_mem];

  test_sge.length = request_size;
  test_sge.addr = (uintptr_t)mitsume_mr->addr;  // 本地地址
  test_sge.lkey = mitsume_mr->lkey;  // 本地内存key
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.num_sge = 1;
  wr.next = NULL;
  wr.sg_list = &test_sge;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = mr_qp[tar_key][0]->buf_addr + (long long)str_key_idx * HERD_SPACE_SIZE;  // 目标地址
  wr.wr.rdma.rkey = mr_qp[tar_key][0]->rkey;  // 目标内存key
  // printf("userspace_one_write: local_addr: %p, remote_addr: %p, tar_mem: %d, tar_key: %lld, value: %s\n", (int *)test_sge.addr, (int *)wr.wr.rdma.remote_addr, tar_mem, tar_key, (char *)(test_sge.addr + 8));
  ret = ibv_post_send(mem_cb->conn_qp[tar_mem], &wr, &bad_send_wr);
  CPE(ret, "ibv_post_send error", ret);
  send_traffic[clt_gid] += test_sge.length;
  return tar_mem;  // 更改返回值
}

void* userspace_one_read(unsigned long long tar_key, int str_key_idx, int tar_mem,
                       struct hrd_ctrl_blk *mem_cb, struct ibv_mr *mitsume_mr,
                       int request_size, struct hrd_qp_attr ****mr_qp_list,
                       int verification, int clt_gid) {
  struct ibv_sge test_sge;
  struct ibv_send_wr wr, *bad_send_wr;
  int ret;
  struct hrd_qp_attr ***mr_qp;
  // unsigned long long tar_key = op[I]->key.bkt;
  mr_qp = mr_qp_list[tar_mem];

  if (verification)
    test_sge.length = HERD_STR_KEY_LEN;
  else
    test_sge.length = request_size;
  test_sge.addr = (uintptr_t)mitsume_mr->addr;
  test_sge.lkey = mitsume_mr->lkey;
  wr.opcode = IBV_WR_RDMA_READ;
  wr.num_sge = 1;
  wr.next = NULL;
  wr.sg_list = &test_sge;
  wr.send_flags = IBV_SEND_SIGNALED;
  // if (verification)
  //   wr.wr.rdma.remote_addr = mr_qp[tar_key]->buf_addr + request_size - 8;
  // else
  //   wr.wr.rdma.remote_addr = mr_qp[tar_key]->buf_addr;
  wr.wr.rdma.remote_addr = mr_qp[tar_key][0]->buf_addr + (long long)str_key_idx * HERD_SPACE_SIZE;
  wr.wr.rdma.rkey = mr_qp[tar_key][0]->rkey;
  ret = ibv_post_send(mem_cb->conn_qp[tar_mem], &wr, &bad_send_wr);
  // if(verification)
  //   printf("userspace_one_read: local_addr: %p, remote_addr: %p, tar_mem: %d, tar_key: %lld, value: %s\n", (int *)test_sge.addr, (int *)wr.wr.rdma.remote_addr, tar_mem, tar_key, (char *)(test_sge.addr + 8));
  CPE(ret, "ibv_post_send error", ret);
  recv_traffic[clt_gid] += test_sge.length;
  // return tar_mem;
  return (void *)test_sge.addr;
}

int userspace_one_compare_and_swp(unsigned long long tar_key, int tar_mem,
                                  struct hrd_ctrl_blk *mem_cb,
                                  struct ibv_mr *mitsume_mr,
                                  struct hrd_qp_attr ****mr_qp_list,
                                  uint64_t expect_value, uint64_t assign_value,
                                  int clt_gid) {
  struct ibv_sge test_sge;
  struct ibv_send_wr wr, *bad_send_wr;
  int ret;
  struct hrd_qp_attr ***mr_qp;

  mr_qp = mr_qp_list[tar_mem];
  test_sge.length = 8;
  test_sge.addr = (uintptr_t)mitsume_mr->addr;
  test_sge.lkey = mitsume_mr->lkey;

  wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
  wr.num_sge = 1;
  wr.next = NULL;
  wr.sg_list = &test_sge;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.atomic.remote_addr = mr_qp[tar_key][0]->buf_addr;
  wr.wr.atomic.rkey = mr_qp[tar_key][0]->rkey;  // lock the while num link
  wr.wr.atomic.compare_add = expect_value;
  wr.wr.atomic.swap = assign_value;

  ret = ibv_post_send(mem_cb->conn_qp[tar_mem], &wr, &bad_send_wr);
  CPE(ret, "ibv_post_send error", ret);
  send_traffic[clt_gid] += test_sge.length;
  return tar_mem;
}

int mitsume_insert_one_key_post(char *str_key, struct hrd_ctrl_blk *mem_cb,
                               struct hrd_qp_attr ****mr_qp_list,
                               struct hrd_qp_attr ****backupmr_qp_list,
                               struct ibv_mr *input_mr,
                               struct ibv_mr *output_mr, void *addr,
                               int clt_gid) {
  uint64_t unlock = 0;
  uint64_t lock = ONE_LOCK;
  int res;
  long long unsigned *read_lock;
  long long unsigned *write_lock;
  int count = 0;
  uint64_t crc = 0;
  int request_size = MICA_HERD_VALUE_SIZE;
  int tar_key = StrHash(str_key);
  int tar_mem = tar_key % NUM_MEMORY;
  int j;

  if (CRC_MODE) // enable crc
    crc = crc32(~0, addr, request_size);
  write_lock = output_mr->addr;
  read_lock = input_mr->addr;
  *write_lock = lock;
  memcpy(output_mr->addr + 8, addr, request_size);
  memcpy(output_mr->addr + 8 + request_size, &crc, sizeof(uint64_t));

  // 1. lock
  do {
    *read_lock = 0xffffffff;
    // printf("FUCK: tar_key: %d  tar_mem: %d\n", tar_key, tar_mem);
    // userspace_one_compare_and_swp(0, 0, mem_cb, input_mr,
    //                               mr_qp_list, unlock, lock, clt_gid);
    // userspace_one_poll(mem_cb, 0);
    userspace_one_compare_and_swp(tar_key, tar_mem, mem_cb, input_mr,
                                  mr_qp_list, unlock, lock, clt_gid);
    userspace_one_poll(mem_cb, tar_mem);
    count++;
  } while (*read_lock != unlock);

  // 2. read the link length
  void* ret_addr = userspace_one_read(tar_key, 0, tar_mem, mem_cb, input_mr, request_size+16, mr_qp_list, 0, clt_gid);
  userspace_one_poll(mem_cb, tar_mem);
  int *link_len_addr = (int *)(ret_addr + 8);
  int link_len = *link_len_addr;
  // printf("[INSERT-DEBUG]: tar_key = %d, link_len = %d\n", tar_key, link_len);

  int link_key_idx = 1;
  for (; link_key_idx <= link_len; link_key_idx ++) {
    // 3. read the str key and compare
    void* ret_addr = userspace_one_read(tar_key, link_key_idx, tar_mem, mem_cb, input_mr, request_size+16, mr_qp_list, 1, clt_gid);
    userspace_one_poll(mem_cb, tar_mem);
    if (!strcmp((const char *)(ret_addr + 8), str_key)) {
      break;
    }
  }

  // 4. check space limitation
  if (link_key_idx >= HERD_KEYS_LINK_LENGTH) {
    printf("[INSERT-DEBUG] hash link space is not enough!\n");
    res = -1;
    // exit(-1);
  }
  // 4. find exists handle
  else if (link_key_idx <= link_len) {
    // printf("[INSERT-DEBUG] insert key %s fail: already exists!\n", str_key);
    res = -1;
  }
  else {
    // 3. write to backup
    for (j = 0; j < NUM_REPLICATION; j++)
      userspace_one_write(tar_key, link_key_idx, (tar_mem + j) % NUM_MEMORY, mem_cb, output_mr,
                          request_size, backupmr_qp_list, clt_gid);
    for (j = 0; j < NUM_REPLICATION; j++)
      userspace_one_poll(mem_cb, (tar_mem + j) % NUM_MEMORY);

    // 4. write to old place
    for (j = 0; j < NUM_REPLICATION; j++)
      userspace_one_write(tar_key, link_key_idx, (tar_mem + j) % NUM_MEMORY, mem_cb, output_mr,
                          request_size + 16, mr_qp_list, clt_gid);
    for (j = 0; j < NUM_REPLICATION; j++)
      userspace_one_poll(mem_cb, (tar_mem + j) % NUM_MEMORY);

    // 5. update link length
    *link_len_addr = link_len + 1;
    memcpy(output_mr->addr + 8, link_len_addr, request_size);
    userspace_one_write(tar_key, 0, tar_mem, mem_cb, output_mr,
                        request_size + 16, mr_qp_list, clt_gid);
    userspace_one_poll(mem_cb, tar_mem);

    // printf("[INSERT-DEBUG] insert key %s success!\n", str_key);
    res = 0;
  }

  // 5. write to unlock
  *write_lock = unlock;
  // userspace_one_write(0, 0, 0, mem_cb, output_mr, 8, mr_qp_list,
  //                     clt_gid);
  // userspace_one_poll(mem_cb, 0);
  userspace_one_write(tar_key, 0, tar_mem, mem_cb, output_mr, 8, mr_qp_list,
                      clt_gid);
  userspace_one_poll(mem_cb, tar_mem);
  return res;
}

int mitsume_delete_one_key_post(char *str_key, struct hrd_ctrl_blk *mem_cb,
                               struct hrd_qp_attr ****mr_qp_list,
                               struct hrd_qp_attr ****backupmr_qp_list,
                               struct ibv_mr *input_mr,
                               struct ibv_mr *output_mr,
                               int clt_gid) {
  uint64_t unlock = 0;
  uint64_t lock = ONE_LOCK;
  int res;
  long long unsigned *read_lock;
  long long unsigned *write_lock;
  // uint64_t crc = 0;
  int request_size = MICA_HERD_VALUE_SIZE;
  int tar_key = StrHash(str_key);
  int tar_mem = tar_key % NUM_MEMORY;
  int j;

  write_lock = output_mr->addr;
  read_lock = input_mr->addr;
  *write_lock = lock;
  // memcpy(output_mr->addr + 8, addr, request_size);
  // memcpy(output_mr->addr + 8 + request_size, &crc, sizeof(uint64_t));

  // 1. lock
  do {
    *read_lock = 0xffffffff;
    // userspace_one_compare_and_swp(0, 0, mem_cb, input_mr,
    //                               mr_qp_list, unlock, lock, clt_gid);
    // userspace_one_poll(mem_cb, 0);
    userspace_one_compare_and_swp(tar_key, tar_mem, mem_cb, input_mr,
                                  mr_qp_list, unlock, lock, clt_gid);
    userspace_one_poll(mem_cb, tar_mem);
  } while (*read_lock != unlock);

  // 2. read the link length
  void* ret_addr = userspace_one_read(tar_key, 0, tar_mem, mem_cb, input_mr, request_size+16, mr_qp_list, 0, clt_gid);
  userspace_one_poll(mem_cb, tar_mem);
  int *link_len_addr = (int *)(ret_addr + 8);
  int link_len = *link_len_addr;

  int link_key_idx = 1;
  for (; link_key_idx <= link_len; link_key_idx ++) {
    // 3. read the str key and compare
    void* ret_addr = userspace_one_read(tar_key, link_key_idx, tar_mem, mem_cb, input_mr, request_size+16, mr_qp_list, 1, clt_gid);
    userspace_one_poll(mem_cb, tar_mem);
    if (!strcmp((const char *)(ret_addr + 8), str_key)) {
      break;
    }
  }

  // 4. not found handle
  if (link_key_idx > link_len) {
    // printf("[DELETE-DEBUG] delete key %s fail: not found!\n", str_key);
    res = -1;
  }
  else {
    // 5. read the following key and move them forward
    link_key_idx += 1;
    for (; link_key_idx <= link_len; link_key_idx ++) {
      void* ret_addr = userspace_one_read(tar_key, link_key_idx, tar_mem, mem_cb, input_mr, request_size+16, mr_qp_list, 1, clt_gid);
      userspace_one_poll(mem_cb, tar_mem);

      memcpy(output_mr->addr, ret_addr, request_size+16);

      // write to backup
      for (j = 0; j < NUM_REPLICATION; j++)
        userspace_one_write(tar_key, link_key_idx-1, (tar_mem + j) % NUM_MEMORY, mem_cb, output_mr,
                            request_size, backupmr_qp_list, clt_gid);
      for (j = 0; j < NUM_REPLICATION; j++)
        userspace_one_poll(mem_cb, (tar_mem + j) % NUM_MEMORY);

      // write to old place
      for (j = 0; j < NUM_REPLICATION; j++)
        userspace_one_write(tar_key, link_key_idx-1, (tar_mem + j) % NUM_MEMORY, mem_cb, output_mr,
                            request_size + 16, mr_qp_list, clt_gid);
      for (j = 0; j < NUM_REPLICATION; j++)
        userspace_one_poll(mem_cb, (tar_mem + j) % NUM_MEMORY);
    }

    // 5. update link length
    *link_len_addr = link_len - 1;
    memcpy(output_mr->addr + 8, link_len_addr, request_size);
    userspace_one_write(tar_key, 0, tar_mem, mem_cb, output_mr,
                        request_size + 16, mr_qp_list, clt_gid);
    userspace_one_poll(mem_cb, tar_mem);

    // printf("[DELETE-DEBUG] delete key %s success!\n", str_key);
    res = 0;
  }

  // 5. write to unlock
  *write_lock = unlock;
  // userspace_one_write(0, 0, 0, mem_cb, output_mr, 8, mr_qp_list,
  //                     clt_gid);
  // userspace_one_poll(mem_cb, 0);
  userspace_one_write(tar_key, 0, tar_mem, mem_cb, output_mr, 8, mr_qp_list,
                      clt_gid);
  userspace_one_poll(mem_cb, tar_mem);
  return res;
}

int mitsume_update_one_key_post(char *str_key, struct hrd_ctrl_blk *mem_cb,
                               struct hrd_qp_attr ****mr_qp_list,
                               struct hrd_qp_attr ****backupmr_qp_list,
                               struct ibv_mr *input_mr,
                               struct ibv_mr *output_mr, void *addr,
                               int clt_gid) {
  uint64_t unlock = 0;
  uint64_t lock = ONE_LOCK;
  int res;
  long long unsigned *read_lock;
  long long unsigned *write_lock;
  uint64_t crc = 0;
  int request_size = MICA_HERD_VALUE_SIZE;
  int tar_key = StrHash(str_key);
  int tar_mem = tar_key % NUM_MEMORY;
  int j;

  if (CRC_MODE) // enable crc
    crc = crc32(~0, addr, request_size);
  write_lock = output_mr->addr;
  read_lock = input_mr->addr;
  *write_lock = lock;
  memcpy(output_mr->addr + 8, addr, request_size);
  memcpy(output_mr->addr + 8 + request_size, &crc, sizeof(uint64_t));
  // 1. lock

  do {
    *read_lock = 0xffffffff;
    // userspace_one_compare_and_swp(0, 0, mem_cb, input_mr,
    //                               mr_qp_list, unlock, lock, clt_gid);
    // userspace_one_poll(mem_cb, 0);
    userspace_one_compare_and_swp(tar_key, tar_mem, mem_cb, input_mr,
                                  mr_qp_list, unlock, lock, clt_gid);
    userspace_one_poll(mem_cb, tar_mem);
  } while (*read_lock != unlock);

  // // 2. read from space
  // userspace_one_read(tar_key, tar_mem, mem_cb, input_mr, request_size+16,
  // mr_qp_list, 0, clt_gid); userspace_one_poll(mem_cb, tar_mem);

  // 2. read the link length
  void* ret_addr = userspace_one_read(tar_key, 0, tar_mem, mem_cb, input_mr, request_size+16, mr_qp_list, 0, clt_gid);
  userspace_one_poll(mem_cb, tar_mem);
  int link_len = *(int *)(ret_addr + 8);

  int link_key_idx = 1;
  for (; link_key_idx <= link_len; link_key_idx ++) {
    // 3. read the str key and compare
    void* ret_addr = userspace_one_read(tar_key, link_key_idx, tar_mem, mem_cb, input_mr, request_size+16, mr_qp_list, 1, clt_gid);
    userspace_one_poll(mem_cb, tar_mem);
    if (!strcmp((const char *)(ret_addr + 8), str_key)) {
      break;
    }
  }

  // 4. not find handle
  if (link_key_idx > link_len) {
    // printf("[UPDATE-DEBUG] update key %s not found!\n", str_key);
    res = -1;
  }
  else {
    // 3. write to backup
    for (j = 0; j < NUM_REPLICATION; j++)
      userspace_one_write(tar_key, link_key_idx, (tar_mem + j) % NUM_MEMORY, mem_cb, output_mr,
                          request_size, backupmr_qp_list, clt_gid);
    for (j = 0; j < NUM_REPLICATION; j++)
      userspace_one_poll(mem_cb, (tar_mem + j) % NUM_MEMORY);

  // #ifdef MICA_READ_VERIFICATION
  //   // 3.1 read to check backup
  //   for (j = 0; j < NUM_REPLICATION; j++)
  //     userspace_one_read(tar_key, (tar_mem + j) % NUM_MEMORY, mem_cb, input_mr,
  //                        request_size, backupmr_qp_list, 0, clt_gid);
  //   for (j = 0; j < NUM_REPLICATION; j++)
  //     userspace_one_poll(mem_cb, (tar_mem + j) % NUM_MEMORY);
  // #endif

    // 4. write to old place
    for (j = 0; j < NUM_REPLICATION; j++)
      userspace_one_write(tar_key, link_key_idx, (tar_mem + j) % NUM_MEMORY, mem_cb, output_mr,
                          request_size + 16, mr_qp_list, clt_gid);
    for (j = 0; j < NUM_REPLICATION; j++)
      userspace_one_poll(mem_cb, (tar_mem + j) % NUM_MEMORY);

  // #ifdef MICA_READ_VERIFICATION
  //   // 4.1 read to check backup
  //   for (j = 0; j < NUM_REPLICATION; j++)
  //     userspace_one_read(tar_key, (tar_mem + j) % NUM_MEMORY, mem_cb, input_mr,
  //                        request_size + 16, mr_qp_list, 0, clt_gid);
  //   for (j = 0; j < NUM_REPLICATION; j++)
  //     userspace_one_poll(mem_cb, (tar_mem + j) % NUM_MEMORY);
  // #endif
    // printf("[UPDATE-DEBUG] update key %s success!\n", str_key);
    res = 0;
  }

  // 5. write to unlock
  *write_lock = unlock;
  // userspace_one_write(0, 0, 0, mem_cb, output_mr, 8, mr_qp_list,
  //                     clt_gid);
  // userspace_one_poll(mem_cb, 0);
  userspace_one_write(tar_key, 0, tar_mem, mem_cb, output_mr, 8, mr_qp_list,
                      clt_gid);
  userspace_one_poll(mem_cb, tar_mem);
  return res;
}

int mitsume_read_one_key_post(char *str_key, struct hrd_ctrl_blk *mem_cb,
                              struct hrd_qp_attr ****mr_qp_list,
                              struct hrd_qp_attr ****backupmr_qp_list,
                              struct ibv_mr *input_mr, void *addr,
                              int clt_gid) {
  int res;
  // uint64_t crc_compute;
  // uint64_t *crc_check;
  uint64_t *check;
  uint64_t unlock = 0;
  uint64_t lock = ONE_LOCK;
  int request_size = MICA_HERD_VALUE_SIZE;
  int tar_mem;
  check = (uint64_t *)input_mr->addr;
  // if(!disable_crc)
  int tar_key = StrHash(str_key);
  tar_mem = tar_key % NUM_MEMORY;
  // if (CRC_MODE) {  // TODO CRC check修改
  //   do {
  //     userspace_one_read(tar_key, tar_mem, mem_cb, input_mr, request_size + 16,
  //                        mr_qp_list, 0, clt_gid);
  //     userspace_one_poll(mem_cb, tar_mem);
  //     // userspace_liteapi_rdma_read(undo_key_map[key].primary_key[0],
  //     // temp_read, request_size+16, 0, LITE_TEST_PW);

  //     crc_check = (uint64_t *)(input_mr->addr + request_size + 8);
  //     crc_compute = crc32(~0, input_mr->addr + 8, request_size);

  //     // enable following two lines and disable above two lines can disable
  //     // read-crc crc_check = &unlock; crc_compute = 0;
  //     count++;
  //   } while (*crc_check != crc_compute || *check == ONE_LOCK);
  //   // memcpy(addr, input_mr->addr+8, request_size);
  // } else {
    // 1. lock
  do {
    *check = 0xffffffff;
    // userspace_one_compare_and_swp(0, 0, mem_cb, input_mr,
    //                               mr_qp_list, unlock, lock, clt_gid);
    // userspace_one_poll(mem_cb, 0);
    userspace_one_compare_and_swp(tar_key, tar_mem, mem_cb, input_mr,
                                  mr_qp_list, unlock, lock, clt_gid);
    userspace_one_poll(mem_cb, tar_mem);
    // userspace_liteapi_compare_swp(undo_key_map[key].primary_key[0], &read,
    // unlock, lock);
  } while (*check != unlock);

  // 2. read the link length
  void* ret_addr = userspace_one_read(tar_key, 0, tar_mem, mem_cb, input_mr, request_size+16, mr_qp_list, 0, clt_gid);
  userspace_one_poll(mem_cb, tar_mem);
  int link_len = *(int *)(ret_addr + 8);

  int link_key_idx = 1;
  for (; link_key_idx <= link_len; link_key_idx ++) {
    // 3. read the str key and compare
    void* ret_addr = userspace_one_read(tar_key, link_key_idx, tar_mem, mem_cb, input_mr, request_size+16, mr_qp_list, 1, clt_gid);
    userspace_one_poll(mem_cb, tar_mem);
    // printf("FUCK: target_str: %s, cur_str: %s\n", str_key, (const char *)(ret_addr +  8));
    if (!strcmp((const char *)(ret_addr + 8), str_key)) {
      break;
    }
  }

  // 4. not find handle
  if (link_key_idx > link_len) {
    // printf("[READ-DEBUG] read key %s not found!\n", str_key);
    res = -1;
  }
  else {
    // 2. rdma-read
    userspace_one_read(tar_key, link_key_idx, tar_mem, mem_cb, input_mr, request_size + 16,
                      mr_qp_list, 0, clt_gid);
    userspace_one_poll(mem_cb, tar_mem);

    // printf("[READ-DEBUG] read key %s success!\n", str_key);
    res = 0;
    // userspace_liteapi_rdma_read(undo_key_map[key].primary_key[0], temp_read,
    // request_size+16, 0, LITE_TEST_PW);

    // memcpy(addr, input_mr->addr+8, request_size);
    // memcpy(addr, temp_read+8, request_size);
  }
  // 3. write to unlock
  *check = unlock;
  // userspace_one_write(0, 0, 0, mem_cb, input_mr, sizeof(uint64_t),
  //                     mr_qp_list, clt_gid);
  // userspace_one_poll(mem_cb, 0);
  userspace_one_write(tar_key, 0, tar_mem, mem_cb, input_mr, sizeof(uint64_t),
                      mr_qp_list, clt_gid);
  userspace_one_poll(mem_cb, tar_mem);
  // userspace_liteapi_rdma_write(undo_key_map[key].primary_key[0], &unlock,
  // sizeof(uint64_t), 0, LITE_TEST_PW);
  // }
  // userspace_liteapi_rdma_read(key_map[key], addr, request_size, 8,
  // LITE_TEST_PW); if(count>1)
  //        printf("read %d\n", count);
  // printf("%llu\n", (unsigned long long) crc_compute);
  return res;
}

// int mitsume_lock_and_update_one_key_post(
//     int tar_key, struct hrd_ctrl_blk *mem_cb, struct hrd_qp_attr ***mr_qp_list,
//     struct hrd_qp_attr ***backupmr_qp_list, struct ibv_mr *input_mr,
//     struct ibv_mr *output_mr, void *addr, int clt_gid) {
//   uint64_t unlock = 0;
//   uint64_t lock = ONE_LOCK;
//   long long unsigned *read_lock;
//   long long unsigned *write_lock;
//   int count = 0;
//   int request_size = MICA_HERD_VALUE_SIZE;
//   int tar_mem = tar_key % NUM_MEMORY;
//   int j;
//   write_lock = output_mr->addr;
//   read_lock = input_mr->addr;
//   *write_lock = lock;
//   // 1. lock

//   do {
//     *read_lock = 0xffffffff;
//     userspace_one_compare_and_swp(tar_key, tar_mem, mem_cb, input_mr,
//                                   mr_qp_list, unlock, lock, clt_gid);
//     userspace_one_poll(mem_cb, tar_mem);
//     count++;
//   } while (*read_lock != unlock);

//   // 4. write to old place
//   for (j = 0; j < NUM_REPLICATION; j++)
//     userspace_one_write(tar_key, (tar_mem + j) % NUM_MEMORY, mem_cb, output_mr,
//                         request_size + 8, mr_qp_list, clt_gid);
//   for (j = 0; j < NUM_REPLICATION; j++)
//     userspace_one_poll(mem_cb, (tar_mem + j) % NUM_MEMORY);

//   // 5. write to unlock
//   *write_lock = unlock;
//   userspace_one_write(tar_key, tar_mem, mem_cb, output_mr, 8, mr_qp_list,
//                       clt_gid);
//   userspace_one_poll(mem_cb, tar_mem);
//   return 0;
// }

struct hrd_ctrl_blk *
mitsume_setup_connection(int clt_gid, int ib_port_index,
                         struct hrd_qp_attr ****mr_qp_list,
                         struct hrd_qp_attr ****backupmr_qp_list, bool is_rdma_testing) {

  struct hrd_ctrl_blk *mem_cb;
  struct hrd_qp_attr ***mr_qp;
  struct hrd_qp_attr ***backupmr_qp;
  struct hrd_qp_attr **item_qp;
  struct hrd_qp_attr **backupitem_qp;

  int per_memory, per_mr;
  char mem_conn_qp_name[HRD_QP_NAME_SIZE];
  mem_cb = hrd_ctrl_blk_init(
      clt_gid,                /* local_hid */
      ib_port_index, -1,      /* port_index, numa_node_id */
      NUM_MEMORY, 0,          /* #conn qps, uc */
      NULL, 4096, -1,         /* prealloc conn buf, buf size, key */
      1, DGRAM_BUF_SIZE, -1, is_rdma_testing, 0); /* num_dgram_qps, dgram_buf_size, key */

  for (per_memory = 0; per_memory < NUM_MEMORY; per_memory++) {
    sprintf(mem_conn_qp_name, "qp-client-%d-memory-%d", clt_gid, per_memory);
    hrd_publish_conn_qp(mem_cb, per_memory, mem_conn_qp_name);
    printf("%s()-%d: publish %s\n", __func__, __LINE__, mem_conn_qp_name);
  }
  printf("main: client %d published conn\n", clt_gid);

  struct hrd_qp_attr *mstr_qp = NULL;
  for (per_memory = 0; per_memory < NUM_MEMORY; per_memory++) {
    sprintf(mem_conn_qp_name, "qp-memory-%d-client-%d", per_memory, clt_gid);
    mstr_qp = NULL;
    do {
      mstr_qp = hrd_get_published_qp(mem_conn_qp_name);
      if (mstr_qp == NULL) {
        usleep(200000);
      }
    } while (mstr_qp == NULL);

    printf("main: Client %d found memory %s ! Connecting..\n", clt_gid,
           mem_conn_qp_name);
    hrd_connect_qp(mem_cb, per_memory, mstr_qp);
    printf("main: Client %d connect memory %s\n", clt_gid, mem_conn_qp_name);
  }

  for (per_memory = 0; per_memory < NUM_MEMORY; per_memory++) {
    char memory_ready_name[HRD_QP_NAME_SIZE] = {};
    sprintf(memory_ready_name, "memory-%d-ready", per_memory);
    hrd_wait_till_ready(memory_ready_name);
  }

  // 添加mr_qp_list[per_memory][per_mr][0]; 第0块记录链表长度
  for (per_memory = 0; per_memory < NUM_MEMORY; per_memory++) {
    mr_qp_list[per_memory] = malloc(sizeof(struct hrd_qp_attr **) * HERD_NUM_KEYS);
    mr_qp = mr_qp_list[per_memory];
    backupmr_qp_list[per_memory] = malloc(sizeof(struct hrd_qp_attr **) * HERD_NUM_KEYS);
    backupmr_qp = backupmr_qp_list[per_memory];
    printf("main: client %d start fetching mr. Connecting..\n", clt_gid);
    for (per_mr = 0; per_mr < HERD_NUM_KEYS; per_mr ++) {
      mr_qp[per_mr] = malloc(sizeof(struct hrd_qp_attr *));
      item_qp = mr_qp[per_mr];
      backupmr_qp[per_mr] = malloc(sizeof(struct hrd_qp_attr *));
      backupitem_qp = backupmr_qp[per_mr];
      if (per_mr % 1000 == 0) {
        char item_name[HRD_QP_NAME_SIZE];
        char backupitem_name[HRD_QP_NAME_SIZE];
        memset(item_name, 0, HRD_QP_NAME_SIZE);
        sprintf(item_name, "M-%d-mr-%d", per_memory, per_mr);

        memset(backupitem_name, 0, HRD_QP_NAME_SIZE);
        sprintf(backupitem_name, "BUM-%d-mr-%d", per_memory, per_mr);
        do {
          item_qp[0] = hrd_get_published_qp(item_name);
          // printf("%s\n", item_name);
          if (item_qp[0] == NULL) {
            usleep(200000);
          }
        } while (item_qp[0] == NULL);
        do {
          backupitem_qp[0] = hrd_get_published_qp(backupitem_name);
          // printf("%s\n", backupitem_name);
          if (backupitem_qp[0] == NULL) {
            usleep(1000000);
          }
        } while (backupitem_qp[0] == NULL);
      }
      else {
        item_qp[0] = malloc(sizeof(struct hrd_qp_attr));
        backupitem_qp[0] = malloc(sizeof(struct hrd_qp_attr));

        item_qp[0]->rkey = mr_qp[per_mr - 1][0]->rkey;
        backupitem_qp[0]->rkey = backupmr_qp[per_mr - 1][0]->rkey;

        item_qp[0]->buf_addr = mr_qp[per_mr - 1][0]->buf_addr + (long long)HERD_SPACE_SIZE * HERD_KEYS_LINK_LENGTH;
        backupitem_qp[0]->buf_addr = backupmr_qp[per_mr - 1][0]->buf_addr + (long long)HERD_SPACE_SIZE * HERD_KEYS_LINK_LENGTH;
      }
    }
  }
  printf("client %d finish mitsume setup\n", clt_gid);

  return mem_cb;
}

void *run_client(void *arg) {
  struct thread_params params = *(struct thread_params *)arg;
  int clt_gid = params.id; /* Global ID of this client thread */
  int num_client_ports = params.num_client_ports;
  int num_server_ports = params.num_server_ports;
  // int update_percentage = params.update_percentage;

  /* This is the only port used by this client */
  int ib_port_index = params.base_port_index + clt_gid % num_client_ports;

  /*
   * The virtual server port index to connect to. This index is relative to
   * the server's base_port_index (that the client does not know).
   */
  int srv_virt_port_index = clt_gid % num_server_ports;

  /*
   * TODO: The client creates a connected buffer because the libhrd API
   * requires a buffer when creating connected queue pairs. This should be
   * fixed in the API.
   */
  struct hrd_ctrl_blk *cb = hrd_ctrl_blk_init(
      clt_gid,                /* local_hid */
      ib_port_index, -1,      /* port_index, numa_node_id */
      1, 1,                   /* #conn qps, uc */
      NULL, 4096, -1,         /* prealloc conn buf, buf size, key */
      1, DGRAM_BUF_SIZE, -1, BENCHMARK_TYPE == 2 && TEST_TYPE == 1, 0); /* num_dgram_qps, dgram_buf_size, key */

  char mstr_qp_name[HRD_QP_NAME_SIZE];
  sprintf(mstr_qp_name, "master-%d-%d", srv_virt_port_index, clt_gid);

  char clt_conn_qp_name[HRD_QP_NAME_SIZE];
  sprintf(clt_conn_qp_name, "client-conn-%d", clt_gid);
  char clt_dgram_qp_name[HRD_QP_NAME_SIZE];
  sprintf(clt_dgram_qp_name, "client-dgram-%d", clt_gid);

  hrd_publish_conn_qp(cb, 0, clt_conn_qp_name);
  hrd_publish_dgram_qp(cb, 0, clt_dgram_qp_name);
  printf("main: Client %s published conn and dgram. Waiting for master %s\n",
         clt_conn_qp_name, mstr_qp_name);

  struct hrd_qp_attr *mstr_qp = NULL;
  while (mstr_qp == NULL) {
    mstr_qp = hrd_get_published_qp(mstr_qp_name);
    if (mstr_qp == NULL) {
      usleep(200000);
    }
  }

  printf("main: Client %s found master! Connecting..\n", clt_conn_qp_name);
  hrd_connect_qp(cb, 0, mstr_qp);
  hrd_wait_till_ready(mstr_qp_name);

  struct hrd_ctrl_blk *mem_cb;
  struct hrd_qp_attr ****mr_qp_list =
      malloc(sizeof(struct hrd_qp_attr ***) * NUM_MEMORY);
  struct hrd_qp_attr ****backupmr_qp_list =
      malloc(sizeof(struct hrd_qp_attr ***) * NUM_MEMORY);
  bool is_rdma_testing = 0;
  if (BENCHMARK_TYPE == 2) {
    is_rdma_testing = 1;
  }
  mem_cb = mitsume_setup_connection(clt_gid, ib_port_index, mr_qp_list,
                                    backupmr_qp_list, is_rdma_testing);

  int mr_size = HERD_SPACE_SIZE;
  if (BENCHMARK_TYPE == 2 && TEST_TYPE == 0) mr_size = HERD_SPACE_SIZE * (ONESIDE_POST_SIZE+5);
  void *input_space = memalign(4096, mr_size);
  void *output_space = memalign(4096, mr_size);
  void *read_write_addr = memalign(4096, mr_size);
  memset(input_space, 0, mr_size);
  memset(output_space, 0, mr_size);
  memset(read_write_addr, 0x31 + clt_gid, mr_size);
  struct ibv_mr *input_mr = ibv_reg_mr(mem_cb->pd, input_space, mr_size,
                                       IBV_ACCESS_LOCAL_WRITE);
  struct ibv_mr *output_mr = ibv_reg_mr(mem_cb->pd, output_space, mr_size,
                                        IBV_ACCESS_LOCAL_WRITE);


  if (BENCHMARK_TYPE == 0) {
    if (TEST_TYPE == 0) {
      /*
      * [TEST] micro_benchmark throughput
      */
      return test_micro_throughput(clt_gid, mem_cb, mr_qp_list, backupmr_qp_list,
                          input_mr, output_mr, read_write_addr);
    }
    else if (TEST_TYPE == 1) {
      /*
      * [TEST] micro_benchmark latency
      */
      return test_micro_latency(clt_gid, mem_cb, mr_qp_list, backupmr_qp_list,
                          input_mr, output_mr, read_write_addr);
    }
  }
  else if (BENCHMARK_TYPE == 1) {
    char **write_key;
    char **op_key;
    int test_times = macro_preload(clt_gid, output_space, input_space, &op_key, &write_key,
                   mem_cb, mr_qp_list, backupmr_qp_list, input_mr, output_mr, read_write_addr);
    if (TEST_TYPE == 0) {
      /*
      * [TEST] macro_benchmark throughput
      */
      return test_macro_throughput(clt_gid, test_times, op_key, write_key, mem_cb, mr_qp_list, backupmr_qp_list,
                         input_mr, output_mr, read_write_addr);
    }
    else if (TEST_TYPE == 1) {
      /*
      * [TEST] macro_benchmark latency
      */
      return test_macro_latency(clt_gid, test_times, op_key, write_key, mem_cb, mr_qp_list, backupmr_qp_list,
                         input_mr, output_mr, read_write_addr);
    }
  }
  else if (BENCHMARK_TYPE == 2) {
    /*
     * [TEST] simple rdma-testing
     */

    int ret = stick_this_thread_to_core(clt_gid % NUM_CLIENTS * 2);  // 每个线程绑定到一个cpu上
    assert(ret == 0);

    syn_thread(clt_gid);

    return test_rdma_throughput(clt_gid, mem_cb, mr_qp_list, backupmr_qp_list,
                        input_mr, output_mr);
  }
  else if (BENCHMARK_TYPE == 3) {
    /*
    * [TEST] simple lock throughput testing
    */
    return test_lock_throughput(clt_gid, mem_cb, mr_qp_list, backupmr_qp_list,
                        input_mr, output_mr, read_write_addr);

  }
  return NULL;
}

void syn_thread(int clt_gid) {
    // post ready 同步
    char ready_name[HRD_QP_NAME_SIZE] = {};
    if (clt_gid == 0) {
      int per_client;
      sprintf(ready_name, "%d-ready-start", clt_gid);
      hrd_publish_ready(ready_name);
      for (per_client = 0; per_client < NUM_CLIENT_NODE * NUM_CLIENTS; per_client++) {  // changed
        sprintf(ready_name, "%d-ready-start", per_client);
        hrd_wait_till_ready(ready_name);
      }
      sprintf(ready_name, "%s", "ready-go");
      hrd_publish_ready(ready_name);
    } else {
      sprintf(ready_name, "%d-ready-start", clt_gid);
      hrd_publish_ready(ready_name);
      sprintf(ready_name, "%s", "ready-go");
      hrd_wait_till_ready(ready_name);
    }

    printf("%d finish all setup\n", clt_gid);
    fflush(stdout);
}

int stick_this_thread_to_core(int core_id) {
    int num_cores = sysconf(_SC_NPROCESSORS_CONF);
    if (core_id < 0 || core_id >= num_cores) {
        return -1;
    }

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    pthread_t current_thread = pthread_self();
    return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

int all_finished() {
    char exit_flag[HRD_QP_NAME_SIZE] = {};
    int per_client = 0;
    for (per_client = 0; per_client < NUM_CLIENT_NODE * NUM_CLIENTS; per_client++) {
      sprintf(exit_flag, "%d-ready-exit", per_client);
      char* value;
      char new_name[2 * HRD_QP_NAME_SIZE];
      sprintf(new_name, "%s", HRD_RESERVED_NAME_PREFIX);
      strcat(new_name, exit_flag);
      if(hrd_get_published(new_name, (void**)&value) < 0) break;
    }
    if(per_client == NUM_CLIENT_NODE * NUM_CLIENTS) return 1;
    return 0;
}

void *test_lock_throughput(int clt_gid, struct hrd_ctrl_blk *mem_cb, struct hrd_qp_attr ****mr_qp_list, struct hrd_qp_attr ****backupmr_qp_list,
                         struct ibv_mr *input_mr, struct ibv_mr *output_mr, void *read_write_addr) {
  struct timespec start, end;
  int read_err_num = 0, update_err_num = 0, insert_err_num = 0, delete_err_num = 0;

  char string[128];
  int fd = open("./lock_throughput.txt"  , O_CREAT | O_RDWR | O_APPEND, 0644);
  if (fd < 0) {
    printf("Fail to open file\n");
    return 0;
  }

  char key_i[100];
  sprintf(key_i, "lock_testing");
  if (clt_gid == 0) {
    // 1. 插入key: "lock_testing"
    sprintf((char *)read_write_addr, "%s", key_i);
    if(mitsume_insert_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                              input_mr, output_mr, read_write_addr, clt_gid) < 0) insert_err_num ++;
  }
  // post ready 同步
  syn_thread(clt_gid);

  long long nb_tx = 0;
  double seconds = 0, ru_threshold = TPT_TEST_TIME;

  // 2. update循环进行10s
  while(1) {
    // char key_i[100];
    // sprintf(key_i, "lock_testing");
    clock_gettime(CLOCK_REALTIME, &start);
    sprintf((char *)read_write_addr, "%s", key_i);
    if(mitsume_update_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                              input_mr, output_mr, read_write_addr, clt_gid) < 0) update_err_num ++;

    clock_gettime(CLOCK_REALTIME, &end);
    nb_tx ++;
    seconds += (end.tv_sec - start.tv_sec) + (double)(end.tv_nsec - start.tv_nsec) / 1000000000;
    // unsigned long ns = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
    if (seconds > ru_threshold)
      break;
  }

  int avg = (int)(nb_tx / seconds);
  printf("[RESULT] client thread %d execute %lld %s ops in %lf seconds. AVG: %d OPS.\n", clt_gid, nb_tx, "UPDATE", seconds, avg);
  int nr;
  nr = sprintf(string, "%d:%d\n", clt_gid, avg);
  pthread_mutex_lock(&mutex);
  write(fd, string, nr);
  pthread_mutex_unlock(&mutex);

  printf("[RESULT] client thread %d fail nums: read: %d, update: %d, insert: %d, delete: %d.\n", clt_gid, read_err_num, update_err_num, insert_err_num, delete_err_num);
  fsync(fd);

  return NULL;
}

void *test_rdma_throughput(int clt_gid, struct hrd_ctrl_blk *mem_cb, struct hrd_qp_attr ****mr_qp_list, struct hrd_qp_attr ****backupmr_qp_list,
                         struct ibv_mr *input_mr, struct ibv_mr *output_mr) {
    /*
     * test throughput
     */
    long long nb_tx = 0, recv_cnt = TWOSIDE_PREPOST_RECV_NUM;        /* Total requests performed or queued */
    struct timespec start, end;
    double seconds = 0;
    double time_threshold = TPT_TEST_TIME;
    // int opnum_threshold = TPT_TEST_OPNUM;
    int err_num = 0, cnt = 0, round_idx = 0;
    bool has_record = 0;

    if (TEST_TYPE == 1) {
      // prepost recv
      test_twoside_prepost_recv(mem_cb->conn_qp[0], (uintptr_t)input_mr->addr, input_mr->lkey, clt_gid);
    }

    while(1) {
      clock_gettime(CLOCK_REALTIME, &start);

      if (TEST_TYPE == 0) {
        if(test_oneside_write_read(mem_cb, mr_qp_list, input_mr, output_mr, clt_gid, &cnt, round_idx) < 0) err_num ++;
      }
      else if (TEST_TYPE == 1) {
        if(test_twoside_write_read(mem_cb, input_mr, output_mr, clt_gid, nb_tx) < 0) err_num ++;
      }
      clock_gettime(CLOCK_REALTIME, &end);
      recv_cnt --;
      round_idx ++;
      if (TEST_TYPE == 1 && recv_cnt <= 0) {  // 补充post recv
        test_twoside_prepost_recv(mem_cb->conn_qp[0], (uintptr_t)input_mr->addr, input_mr->lkey, clt_gid);
        recv_cnt = TWOSIDE_PREPOST_RECV_NUM;
      }
      if (seconds < time_threshold) {
        nb_tx += (TEST_TYPE == 0 ? cnt : 1);
        // printf("%d: FUCK! %lld\n", clt_gid, nb_tx);
        seconds += (end.tv_sec - start.tv_sec) + (double)(end.tv_nsec - start.tv_nsec) / 1000000000;
      }
      else if (!has_record) {
        int avg = (int)(nb_tx / seconds);
        printf("[RESULT] client thread %d execute %lld ops in %lf seconds. AVG: %d OPS.\n", clt_gid, nb_tx, seconds, avg);
        printf("[RESULT] client thread %d fail nums: %d.\n", clt_gid, err_num);

        char string[128];
        int fd = open("./rdma_throughput.txt"  , O_CREAT | O_RDWR | O_APPEND, 0644);
        if (fd < 0) {
          printf("Fail to open file\n");
          return 0;
        }
        int nr;
        nr = sprintf(string, "%d:%d\n", clt_gid, avg);
        pthread_mutex_lock(&mutex);
        write(fd, string, nr);
        pthread_mutex_unlock(&mutex);
        fsync(fd);
        has_record = 1;
        printf("thread %d finish!\n", clt_gid);

        // 发送可以结束信号
        char exit_flag[HRD_QP_NAME_SIZE] = {};
        sprintf(exit_flag, "%d-ready-exit", clt_gid);
        hrd_publish_ready(exit_flag);
      }
      // 由每个结点的第一个线程来exit
      if (has_record && all_finished()) break;
    }
    return NULL;
}

void *test_micro_throughput(int clt_gid, struct hrd_ctrl_blk *mem_cb, struct hrd_qp_attr ****mr_qp_list, struct hrd_qp_attr ****backupmr_qp_list,
                         struct ibv_mr *input_mr, struct ibv_mr *output_mr, void *read_write_addr) {
  struct timespec start, end;
  int read_err_num = 0, update_err_num = 0, insert_err_num = 0, delete_err_num = 0;

  char string[128];
  int fd = open("./micro_throughput.txt"  , O_CREAT | O_RDWR | O_APPEND, 0644);
  if (fd < 0) {
    printf("Fail to open file\n");
    return 0;
  }

  int seq[4] = {2, 1, 0, 3};
  char op[4][10] = {"READ", "UPDATE", "INSERT", "DELETE"};
  int max_test_times = TEST_TIMES;

  for (int k = 0; k < 4; k ++) {
    int flag = seq[k];
    long long nb_tx = 0;
    double seconds = 0, id_threshold = TPT_INSERT_TEST_TIME, ru_threshold = TPT_TEST_TIME;

    if (flag == 2) { // insert只跑1s来测吞吐量
      for (int i = 0; ; i ++) {
        char key_i[100];
        sprintf(key_i, "%d-%d", clt_gid, i);  // micro_benchmark
        clock_gettime(CLOCK_REALTIME, &start);
        sprintf((char *)read_write_addr, "%s", key_i);
        if(mitsume_insert_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                  input_mr, output_mr, read_write_addr, clt_gid) < 0) insert_err_num ++;
        clock_gettime(CLOCK_REALTIME, &end);
        if (seconds < id_threshold) {
          nb_tx ++;
          seconds += (end.tv_sec - start.tv_sec) + (double)(end.tv_nsec - start.tv_nsec) / 1000000000;
        }
        else {
          max_test_times = nb_tx;
          break;
        }
      }
    }
    else if (flag == 3) {  // delete只跑1s来测吞吐量
      for (int i = 0; i < max_test_times; i ++) {
        char key_i[100];
        sprintf(key_i, "%d-%d", clt_gid, i);  // micro_benchmark
        clock_gettime(CLOCK_REALTIME, &start);
        if(mitsume_delete_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                  input_mr, output_mr, clt_gid) < 0) delete_err_num ++;
        clock_gettime(CLOCK_REALTIME, &end);
        if (seconds < id_threshold) {
          nb_tx ++;
          seconds += (end.tv_sec - start.tv_sec) + (double)(end.tv_nsec - start.tv_nsec) / 1000000000;
        }
      }
    }
    else {
      for (int i = 0; ; i = (i + 1) % max_test_times) {  // read和update循环进行10s
        char key_i[100];
        sprintf(key_i, "%d-%d", clt_gid, i);  // micro_benchmark
        clock_gettime(CLOCK_REALTIME, &start);
        if (flag == 0) {
          if(mitsume_read_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                            input_mr, read_write_addr, clt_gid) < 0) read_err_num ++;
        }
        else if (flag == 1) {
          sprintf((char *)read_write_addr, "%s", key_i);
          if(mitsume_update_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                    input_mr, output_mr, read_write_addr, clt_gid) < 0) update_err_num ++;
        }
        clock_gettime(CLOCK_REALTIME, &end);
        nb_tx ++;
        seconds += (end.tv_sec - start.tv_sec) + (double)(end.tv_nsec - start.tv_nsec) / 1000000000;
        // unsigned long ns = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
        if (seconds > ru_threshold)
          break;
      }
    }
    int avg = (int)(nb_tx / seconds);
    printf("[RESULT] client thread %d execute %lld %s ops in %lf seconds. AVG: %d OPS.\n", clt_gid, nb_tx, op[flag], seconds, avg);
    int nr;
    nr = sprintf(string, "%d:%s:%d\n", clt_gid, op[flag], avg);
    pthread_mutex_lock(&mutex);
    write(fd, string, nr);
    pthread_mutex_unlock(&mutex);
  }
  printf("[RESULT] client thread %d fail nums: read: %d, update: %d, insert: %d, delete: %d.\n", clt_gid, read_err_num, update_err_num, insert_err_num, delete_err_num);
  fsync(fd);

  return NULL;
}

void *test_micro_latency(int clt_gid, struct hrd_ctrl_blk *mem_cb, struct hrd_qp_attr ****mr_qp_list, struct hrd_qp_attr ****backupmr_qp_list,
                         struct ibv_mr *input_mr, struct ibv_mr *output_mr, void *read_write_addr) {
  if (clt_gid > 0) {
    printf("[ERROR] thread num error. Only 1 thread is needed.\n");
    return 0;
  }

  struct timespec start, end;
  int read_err_num = 0, update_err_num = 0, insert_err_num = 0, delete_err_num = 0;
  int read_fd = open("./micro_latency/read.txt", O_CREAT | O_RDWR, 0644);
  int update_fd = open("./micro_latency/update.txt", O_CREAT | O_RDWR, 0644);
  int insert_fd = open("./micro_latency/insert.txt", O_CREAT | O_RDWR, 0644);
  int delete_fd = open("./micro_latency/delete.txt", O_CREAT | O_RDWR, 0644);

  if (read_fd < 0 || update_fd < 0 || insert_fd < 0 || delete_fd < 0) {
    printf("[ERROR] Fail to open file\n");
    return 0;
  }

  int seq[4] = {2, 1, 0, 3};
  for (int k = 0; k < 4; k ++) {
    int flag = seq[k];
    for (int i = 0; i < LATENCY_TEST_TIMES; i ++) {
      // printf("FUCK: flag=%d  i=%d\n", flag, i);
      char key_i[100];
      sprintf(key_i, "%d-%d", clt_gid, i);  // micro_benchmark
      clock_gettime(CLOCK_REALTIME, &start);
      if (flag == 0) {
        if(mitsume_read_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                  input_mr, read_write_addr, clt_gid) < 0) read_err_num ++;
      }
      else if (flag == 1) {
        sprintf((char *)read_write_addr, "%s", key_i);
        if(mitsume_update_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                  input_mr, output_mr, read_write_addr, clt_gid) < 0) update_err_num ++;
      }
      else if (flag == 2) {
        sprintf((char *)read_write_addr, "%s", key_i);
        if(mitsume_insert_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                  input_mr, output_mr, read_write_addr, clt_gid) < 0) insert_err_num ++;
      }
      else if (flag == 3) {
        if(mitsume_delete_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                  input_mr, output_mr, clt_gid) < 0) delete_err_num ++;
      }
      clock_gettime(CLOCK_REALTIME, &end);
      long long us = (end.tv_sec - start.tv_sec) * 1000000 + (double)(end.tv_nsec - start.tv_nsec) / 1000;

      char string[128];
      int nr;
      nr = sprintf(string, "%lld\n", us);
      // if (nr < 2) printf("FUCK: %s", string);

      if (flag == 0) write(read_fd, string, nr);
      else if (flag == 1) write(update_fd, string, nr);
      else if (flag == 2) write(insert_fd, string, nr);
      else if (flag == 3) write(delete_fd, string, nr);
    }
  }
  printf("[RESULT] client thread %d fail nums: read: %d, update: %d, insert: %d, delete: %d.\n", clt_gid, read_err_num, update_err_num, insert_err_num, delete_err_num);
  fsync(read_fd);
  fsync(update_fd);
  fsync(insert_fd);
  fsync(delete_fd);
  printf("Press Ctrl+C to finish.\n");
  return NULL;
}

int macro_preload(int clt_gid, void *output_space, void *input_space, char ***op_key, char ***write_key,
                   struct hrd_ctrl_blk *mem_cb, struct hrd_qp_attr ****mr_qp_list, struct hrd_qp_attr ****backupmr_qp_list,
                   struct ibv_mr *input_mr, struct ibv_mr *output_mr, void *read_write_addr) {
  /*
   * FOR macro_benchmark
   */
  // first node clean output space and run .load
  if (clt_gid == 0) {
    memset(output_space, 0, HERD_SPACE_SIZE);
    memset(input_space, 0, HERD_SPACE_SIZE);
    char **insert_key;
    int test_times;
    test_times = get_load_file(&insert_key);
    // printf("FUCK1: %d\n", test_times);
    // 由线程1来load所有insert key
     for (int i = 0; i < test_times; i ++) {
      // printf("FUCK2: %s\n", insert_key[i]);
      sprintf((char *)read_write_addr, "%s", insert_key[i]);
      mitsume_insert_one_key_post(insert_key[i], mem_cb, mr_qp_list, backupmr_qp_list,
                             input_mr, output_mr, read_write_addr, clt_gid);
      // free(insert_key[i]);
      }
    // free(insert_key);
  }

  // 加载
  int test_times = get_file(op_key, write_key, clt_gid);
  printf("client %d finished get_file.\n", clt_gid);

  // post ready 同步
  syn_thread(clt_gid);

  printf("start working %d\n", clt_gid);
  return test_times;
}

void *test_macro_throughput(int clt_gid, int test_times, char **op_key, char **write_key,
                         struct hrd_ctrl_blk *mem_cb, struct hrd_qp_attr ****mr_qp_list, struct hrd_qp_attr ****backupmr_qp_list,
                         struct ibv_mr *input_mr, struct ibv_mr *output_mr, void *read_write_addr) {
    /*
     * test throughput
     */
    long long nb_tx = 0;        /* Total requests performed or queued */
    struct timespec start, end;
    double seconds = 0;
    double time_threshold = TPT_TEST_TIME;
    int read_err_num = 0, update_err_num = 0, insert_err_num = 0, delete_err_num = 0;

    while(nb_tx < test_times) {
      clock_gettime(CLOCK_REALTIME, &start);

      int flag = 0;
      if (!strcmp(op_key[nb_tx % test_times], "READ"))
        flag = 0;
      else if (!strcmp(op_key[nb_tx % test_times], "UPDATE"))
        flag = 1;
      else if (!strcmp(op_key[nb_tx % test_times], "INSERT"))
        flag = 2;
      else if (!strcmp(op_key[nb_tx % test_times], "DELETE"))
        flag = 3;

      char* key_i = write_key[nb_tx % test_times];

      if (flag == 0) {
        if(mitsume_read_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                  input_mr, read_write_addr, clt_gid) < 0) read_err_num ++;
      }
      else if (flag == 1) {
        sprintf((char *)read_write_addr, "%s", key_i);
        if(mitsume_update_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                  input_mr, output_mr, read_write_addr, clt_gid) < 0) update_err_num ++;
      }
      else if (flag == 2) {
        sprintf((char *)read_write_addr, "%s", key_i);
        if(mitsume_insert_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                  input_mr, output_mr, read_write_addr, clt_gid) < 0) insert_err_num ++;
      }
      else if (flag == 3) {
        if(mitsume_delete_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                  input_mr, output_mr, clt_gid) < 0) delete_err_num ++;
      }
      clock_gettime(CLOCK_REALTIME, &end);
      nb_tx += 1;
      seconds += (end.tv_sec - start.tv_sec) + (double)(end.tv_nsec - start.tv_nsec) / 1000000000;
      // unsigned long ns = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
      if (seconds > time_threshold)
        break;
    }
    int avg = (int)(nb_tx / seconds);
    printf("[RESULT] client thread %d execute %lld ops in %lf seconds. AVG: %d OPS.\n", clt_gid, nb_tx, seconds, avg);
    printf("[RESULT] client thread %d fail nums: read: %d, update: %d, insert: %d, delete: %d.\n", clt_gid, read_err_num, update_err_num, insert_err_num, delete_err_num);

    char string[128];
    int fd = open("./macro_throughput.txt"  , O_CREAT | O_RDWR | O_APPEND, 0644);
    if (fd < 0) {
      printf("Fail to open file\n");
      return 0;
    }
    int nr;
    nr = sprintf(string, "%d:%d\n", clt_gid, avg);
    pthread_mutex_lock(&mutex);
    write(fd, string, nr);
    pthread_mutex_unlock(&mutex);
    fsync(fd);
    return NULL;
}

void *test_macro_latency(int clt_gid, int test_times, char **op_key, char **write_key,
                         struct hrd_ctrl_blk *mem_cb, struct hrd_qp_attr ****mr_qp_list, struct hrd_qp_attr ****backupmr_qp_list,
                         struct ibv_mr *input_mr, struct ibv_mr *output_mr, void *read_write_addr) {
    /*
     * test latency
     */
    long long nb_tx = 0;        /* Total requests performed or queued */
    int read_num = 0, update_num = 0, insert_num = 0, delete_num = 0;
    const char* workload_type = WORKLOAD_ID;

    char filepath[100];
    sprintf(filepath, "./macro_latency/workload%s_read_%d.txt", WORKLOAD_ID, clt_gid);
    int read_fd = open(filepath , O_CREAT | O_RDWR, 0644);
    sprintf(filepath, "./macro_latency/workload%s_update_%d.txt", WORKLOAD_ID, clt_gid);
    int update_fd = open(filepath , O_CREAT | O_RDWR, 0644);
    sprintf(filepath, "./macro_latency/workload%s_insert_%d.txt", WORKLOAD_ID, clt_gid);
    int insert_fd = open(filepath , O_CREAT | O_RDWR, 0644);
    sprintf(filepath, "./macro_latency/workload%s_delete_%d.txt", WORKLOAD_ID, clt_gid);
    int delete_fd = open(filepath , O_CREAT | O_RDWR, 0644);

    if (read_fd < 0 || update_fd < 0 || insert_fd < 0 || delete_fd < 0) {
      printf("[ERROR] Fail to open file\n");
      return 0;
    }

    switch(workload_type[0]) {
      case 'a': {
        read_num = update_num = LATENCY_TEST_TIMES / 2;
        break;
      }
      case 'b': {
        read_num = update_num = LATENCY_TEST_TIMES / 2;
        break;
      }
      case 'c': {
        read_num = LATENCY_TEST_TIMES;
        break;
      }
      case 'd': {
        read_num = insert_num = LATENCY_TEST_TIMES / 2;
        break;
      }
      default:
        printf("[ERROR] Unknown workload type.\n");
        return 0;
    }

    int read_cur_num = 0, update_cur_num = 0, insert_cur_num = 0, delete_cur_num = 0;
    int read_err_num = 0, update_err_num = 0, insert_err_num = 0, delete_err_num = 0;

    while(read_cur_num < read_num || update_cur_num < update_num || insert_cur_num < insert_num || delete_cur_num < delete_num) {

      int flag = 0;
      if (!strcmp(op_key[nb_tx % test_times], "READ"))
        flag = 0;
      else if (!strcmp(op_key[nb_tx % test_times], "UPDATE"))
        flag = 1;
      else if (!strcmp(op_key[nb_tx % test_times], "INSERT"))
        flag = 2;
      else if (!strcmp(op_key[nb_tx % test_times], "DELETE"))
        flag = 3;

      char *key_i = write_key[nb_tx % test_times];

      struct timespec start, end;

      clock_gettime(CLOCK_REALTIME, &start);
      if (flag == 0) {
        if(mitsume_read_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                  input_mr, read_write_addr, clt_gid) < 0) read_err_num ++;
      }
      else if (flag == 1) {
        sprintf((char *)read_write_addr, "%s", key_i);
        if(mitsume_update_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                  input_mr, output_mr, read_write_addr, clt_gid) < 0) update_err_num ++;
      }
      else if (flag == 2) {
        sprintf((char *)read_write_addr, "%s", key_i);
        if(mitsume_insert_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                  input_mr, output_mr, read_write_addr, clt_gid) < 0) insert_err_num ++;
      }
      else if (flag == 3) {
        if(mitsume_delete_one_key_post(key_i, mem_cb, mr_qp_list, backupmr_qp_list,
                                  input_mr, output_mr, clt_gid) < 0) delete_err_num ++;
      }
      clock_gettime(CLOCK_REALTIME, &end);
      nb_tx += 1;

      long long us = (end.tv_sec - start.tv_sec) * 1000000 + (double)(end.tv_nsec - start.tv_nsec) / 1000;

      char string[128];
      int nr;
      nr = sprintf(string, "%lld\n", us);

      if (flag == 0 && read_cur_num < read_num) write(read_fd, string, nr);
      if (flag == 1 && update_cur_num < update_num) write(update_fd, string, nr);
      if (flag == 2 && insert_cur_num < insert_num) write(insert_fd, string, nr);
      if (flag == 3 && delete_cur_num < delete_num) write(delete_fd, string, nr);

      if (flag == 0) read_cur_num ++;
      else if (flag == 1) update_cur_num ++;
      else if (flag == 2) insert_cur_num ++;
      else if (flag == 3) delete_cur_num ++;
    }
    fsync(read_fd);
    fsync(update_fd);
    fsync(insert_fd);
    fsync(delete_fd);
    return NULL;
}

void *run_memory(void *arg) {
  // int i;
  struct thread_params params = *(struct thread_params *)arg;
  int mem_gid = params.id; /* Global ID of this client thread */
  int num_client_ports = params.num_client_ports;

  /* This is the only port used by this client */
  int ib_port_index = params.base_port_index + mem_gid % num_client_ports;

  /*
   * The virtual server port index to connect to. This index is relative to
   * the server's base_port_index (that the client does not know).
   */
  /*
   * TODO: The client creates a connected buffer because the libhrd API
   * requires a buffer when creating connected queue pairs. This should be
   * fixed in the API.
   */
  printf("%d %lld\n", HERD_NUM_KEYS, (long long)HERD_SPACE_SIZE * (HERD_KEYS_LINK_LENGTH * HERD_NUM_KEYS + 1) * 2);
  struct hrd_ctrl_blk *cb = hrd_ctrl_blk_init(
      mem_gid,                      /* local_hid */
      ib_port_index, -1,            /* port_index, numa_node_id */
      NUM_WORKERS + NUM_CLIENT_NODE * NUM_CLIENTS, 0, /* #conn qps, uc */
      NULL, (long long)HERD_SPACE_SIZE * (HERD_KEYS_LINK_LENGTH * HERD_NUM_KEYS + 1) * 2,  // 记得改这里！控制shared mamory大小
      -1,                     /* prealloc conn buf, buf size, key */
      1, DGRAM_BUF_SIZE, -1, BENCHMARK_TYPE == 2 && TEST_TYPE == 1, 1); /* num_dgram_qps, dgram_buf_size, key */

  char mem_conn_qp_name[HRD_QP_NAME_SIZE];
  int per_worker, per_client;
  for (per_worker = 0; per_worker < NUM_WORKERS; per_worker++) {
    sprintf(mem_conn_qp_name, "qp-memory-%d-worker-%d", mem_gid, per_worker);
    hrd_publish_conn_qp(cb, per_worker, mem_conn_qp_name);
    printf("publish %s\n", mem_conn_qp_name);
  }
  for (per_client = 0; per_client < NUM_CLIENT_NODE * NUM_CLIENTS; per_client++) {
    sprintf(mem_conn_qp_name, "qp-memory-%d-client-%d", mem_gid, per_client);
    hrd_publish_conn_qp(cb, per_client + NUM_WORKERS, mem_conn_qp_name);
    printf("publish %s\n", mem_conn_qp_name);
  }

  printf("main: Memory %d published conn\n", mem_gid);

  volatile uint8_t *shared_memory_space[HERD_NUM_KEYS][HERD_KEYS_LINK_LENGTH];
  // struct ibv_mr **shared_memory_region;
  volatile uint8_t *backup_memory_space[HERD_NUM_KEYS][HERD_KEYS_LINK_LENGTH];
  // struct ibv_mr **backup_memory_region;
  int i, j;
  // char *pp = (char *)cb->conn_buf;
  // for (long long i = 0; i < (long long)HERD_SPACE_SIZE * (HERD_KEYS_LINK_LENGTH * HERD_NUM_KEYS + 1) * 2; i++) {  // 初始化shared memory space为0
  //   if(i % 10000000 == 0)
  //     printf("FUCK: %lld, %p\n", i, &pp[i]);
  //   pp[i] = 0x0;
  // }

  struct hrd_qp_attr *mstr_qp = NULL;
  for (per_worker = 0; per_worker < NUM_WORKERS; per_worker++) {
    sprintf(mem_conn_qp_name, "qp-worker-%d-memory-%d", per_worker, mem_gid);
    mstr_qp = NULL;
    do {
      mstr_qp = hrd_get_published_qp(mem_conn_qp_name);
      if (mstr_qp == NULL) {
        usleep(200000);
      }
    } while (mstr_qp == NULL);
    printf("main: Memory %s found worker! Connecting..\n", mem_conn_qp_name);
    hrd_connect_qp(cb, per_worker, mstr_qp);
    printf("main: Memory %s connect worker.\n", mem_conn_qp_name);
  }
  char shared_memory_name[HRD_QP_NAME_SIZE];
  char backup_memory_name[HRD_QP_NAME_SIZE];
  {
    // shared_memory_space = malloc(sizeof(uintptr_t *) * HERD_NUM_KEYS * HERD_KEYS_LINK_LENGTH);
    // backup_memory_space = malloc(sizeof(uintptr_t *) * HERD_NUM_KEYS * HERD_KEYS_LINK_LENGTH);
    printf("main: Memory %d start publishing %d * %d mr\n", mem_gid, HERD_NUM_KEYS, HERD_KEYS_LINK_LENGTH);
    for (i = 0; i < HERD_NUM_KEYS; i++) {
      for (j = 0; j < HERD_KEYS_LINK_LENGTH; j++) {
        shared_memory_space[i][j] = cb->conn_buf + ((long long)HERD_SPACE_SIZE * HERD_KEYS_LINK_LENGTH * i) + ((long long)HERD_SPACE_SIZE * j);  // 给每一个string key留位置
        backup_memory_space[i][j] = cb->conn_buf + ((long long)HERD_SPACE_SIZE * HERD_KEYS_LINK_LENGTH * i) + ((long long)HERD_SPACE_SIZE * j) + ((long long)HERD_SPACE_SIZE * HERD_KEYS_LINK_LENGTH * HERD_NUM_KEYS);

        if (shared_memory_space[i][j] && backup_memory_space[i][j]) {
          if (i % 1000 == 0 && j == 0) { // 只发送链表头的qp信息即可
            struct hrd_qp_attr pub_qp;
            memset(shared_memory_name, 0, HRD_QP_NAME_SIZE);
            sprintf(shared_memory_name, "M-%d-mr-%d", mem_gid, i);
            pub_qp.buf_addr = (uintptr_t)shared_memory_space[i][j];  // 在这里给remote buf addr赋值，就是shared_memory_space
            pub_qp.rkey = cb->conn_buf_mr->rkey;
            hrd_publish(shared_memory_name, &pub_qp, sizeof(struct hrd_qp_attr));  // client收到：M-0-mr-i: pub_qp

            memset(backup_memory_name, 0, HRD_QP_NAME_SIZE);
            sprintf(backup_memory_name, "BUM-%d-mr-%d", mem_gid, i);
            pub_qp.buf_addr = (uintptr_t)backup_memory_space[i][j];
            pub_qp.rkey = cb->conn_buf_mr->rkey;
            hrd_publish(backup_memory_name, &pub_qp, sizeof(struct hrd_qp_attr));
            // if (!strcmp(backup_memory_name, "BUM-0-mr-0-item-0")) printf("Match!! %s\n", backup_memory_name);
            
            printf("i = %d, j = %d\n", i, j);
          }
        } else {
          printf("main: failed to publish num key %d item %d\n", i, j);
        }
      }
    }
    printf("main: Memory %d publish %d * %d mr\n", mem_gid, HERD_NUM_KEYS, HERD_KEYS_LINK_LENGTH);
  }
#ifdef MICA_IF_TEST_LATENCY
  printf("wakeup client now\n");
  sleep(5);
#endif
  for (per_client = 0; per_client < NUM_CLIENT_NODE * NUM_CLIENTS; per_client++) {
    sprintf(mem_conn_qp_name, "qp-client-%d-memory-%d", per_client, mem_gid);
    printf("main: Memory %d looks for client %d %s..\n", mem_gid, per_client,
           mem_conn_qp_name);
    mstr_qp = NULL;
    do {
      mstr_qp = hrd_get_published_qp(mem_conn_qp_name);
      if (mstr_qp == NULL) {
        usleep(200000);
      }
    } while (mstr_qp == NULL);
    printf("main: Memory %s found client! Connecting..\n", mem_conn_qp_name);
    hrd_connect_qp(cb, per_client + NUM_WORKERS, mstr_qp);
    printf("main: Memory %s connect client.\n", mem_conn_qp_name);
  }
  char memory_ready_name[HRD_QP_NAME_SIZE] = {};
  sprintf(memory_ready_name, "memory-%d-ready", mem_gid);
  hrd_publish_ready(memory_ready_name);
  /*memset(shared_memory_space[0], 0x32, 24);
  printf("enter while loop\n");
  while(1)
  {
          printf("current - %s\n", (char *)shared_memory_space[0]);
          sleep(1);
  }*/
  if (BENCHMARK_TYPE == 2 && TEST_TYPE == 1) {
    // 8个server线程来进行rdma twoside testing
    struct server_thread_params *param_arr;
    pthread_t *thread_arr;
    param_arr = malloc((8+2) * sizeof(struct server_thread_params));
    thread_arr = malloc((8+2) * sizeof(pthread_t));
    for (i = 0; i < 8; i++) {
      param_arr[i].id = i;
      param_arr[i].cb = cb;
      pthread_create(&thread_arr[i], NULL, test_twoside_server, &param_arr[i]);
    }
    for (i = 0; i < 8; i++) {
      pthread_join(thread_arr[i], NULL);
    }
  }
  else {
    while (1)
      ;
  }
  return NULL;
}
