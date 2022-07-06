#ifndef NN_TASK_HANDLER_H
#define NN_TASK_HANDLER_H

#include "dfs_task.h"
#include "dfs_node.h"

void dn_task_handler(void *q);

int task_test(task_t *task);

int handle_ipscan(task_t *task);

int handle_node_want_join(task_t *task);

int rpc_handle_node_remove(task_t *task);

bool check_node_is_alive(const std::string& messfromleaderip,std::string nodeip);

bool ask_node_status(char* sBuf,int sLen,DfsNode askfor);

void hande_check_nodeself(task_t *task);

int handle_kill_node(task_t *task);
//
#endif

