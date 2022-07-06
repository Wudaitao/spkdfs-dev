#ifndef DN_WORKER_PROCESSS_H
#define DN_WORKER_PROCESSS_H

#include "dn_cycle.h"
#include "dn_thread.h"
void worker_processer(cycle_t *cycle, void *data);
void register_thread_initialized(void);
void register_thread_exit(void);

// add
void dispatch_task_(void *);
bool task_pre_check(task_t *t);
bool task_pre_handle(task_t *t,task_queue_node_t *replace_node);
int start_ns_server(const std::string& ns_ipport_string);
int create_new_ns_service_thread(cycle_t *cycle, dfs_thread_t *lthread, const char *newip, int port);
#endif

