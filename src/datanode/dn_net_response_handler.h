#ifndef NN_NET_RESPONSE_HANDLER_H
#define NN_NET_RESPONSE_HANDLER_H

#include "dn_thread.h"
#include "dn_task_queue.h"
#include "dfs_task_codec.h"
#include "nn_request.h"
#include "dn_request.h"

struct nn_wb_s // write back?
{
    nn_conn_t_    *mc;
    dfs_thread_t *thread;
};

typedef struct nn_wb_s nn_wb_t;

typedef struct wb_node_s
{
    task_queue_node_t qnode;
    nn_wb_t           wbt;
} wb_node_t;

int  write_back(task_queue_node_t *node);
void write_back_notice_call(void *data);
void net_response_handler(void *data);
void write_back_pack_queue(queue_t *q, int send);
int  dn_trans_task(task_t *task, dfs_thread_t *thread);
int  push_task_to_tq(task_t *task, dfs_thread_t *thread);
void dn_free_task_node(task_queue_node_t *node);
int write_back_task(task_t* t);

#endif

