//
// Created by ginux on 2020/5/28.
//

#ifndef NGXFS_DFS_UTILS_H
#define NGXFS_DFS_UTILS_H
#include <cstdio>
#include <dfs_types.h>
#include "dfs_task.h"
#include "dfs_node.h"

#define PENU(x) ({printf("%s\n",#x); (x);})
#define BUF_SZ      4096

int parseIpPort(char * ipstring, char *ip, int &port);


//
int net_connect(char *ip, int port, log_t *log , int timeout = 0); //connect to ip port
void keyEncode(uchar_t *path, uchar_t *key);//encode with base64
void keyDecode(uchar_t *key, uchar_t *path);


int send_cmd_task(cmd_t cmd, int sockfd, log_t *log);
int send_data_task(cmd_t cmd, int sockfd, int data_len ,void *data ,log_t *log);
int send_key_data_task(cmd_t cmd, int sockfd, char *src, int data_len ,void *data ,log_t *log);

task_t  recv_task(int sockfd, log_t *log ,pool_s *pool);
task_t redirect_to_master(cmd_t cmd, char *masterIp, int masterPort, void* data,int data_len ,log_t * log, pool_s *pool);


int parse_ipport_list(const char * pcStr,
                             std::vector<DfsNode> & vecNodeInfoList);
// get real bin exec dir
std::string getExecDir();

std::string expand_user(std::string path);
// get randint
int get1RandInt(int groupcnt); // 1~(groupcnt-1)

int get3RandInt(int *nums, int groupcnt);

int get_distinct_randInt(int *nums, int cnt, int groupcnt);

int get_random_randInt(int *nums, int cnt, int groupcnt);

// check ip illegal
bool isIpFormatRight(char * ipstr);

// readn and return
ssize_t readn(int fd, void *vptr, size_t n);

#endif //NGXFS_DFS_UTILS_H
