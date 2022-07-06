#ifndef DN_PAXOS_H
#define DN_PAXOS_H

#include "dn_cycle.h"
#include "PhxElection.h"
#include "dn_thread.h"

extern dfs_thread_t *paxos_thread;

int dn_paxos_worker_init(cycle_t *cycle);
int dn_paxos_worker_release(cycle_t *cycle);
int do_paxos_test();
int dn_paxos_run();
PhxElection* dn_get_paxos_obj();
void set_checkpoint_instanceID(const uint64_t llInstanceID);
void do_paxos_task_handler(void *q);
int do_paxos_task(task_t *task , task_queue_node_t *node);
int grouplist_node_join(task_t *task);
int grouplist_node_remove(task_t *task);
int grouplist_node_init();
void group_list_paxos_init();
int handle_leaders_change(task_t *task);
int handl_master_change(task_t *task);
int grouplist_killnode(task_t *task);
void grouplist_shutdown();
#endif
