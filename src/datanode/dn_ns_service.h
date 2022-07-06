#ifndef DN_NS_SERVICE
#define DN_NS_SERVICE

#include "dn_thread.h"
#include "dn_data_storage.h"

int dn_register(dfs_thread_t *thread);
int offer_service(dfs_thread_t *thread, dfs_thread_t *local_thread);
int send_local_task(dfs_thread_t *thread);
int do_send_local_task(dfs_thread_t *thread, task_t *task);

int blk_report_queue_init();
int blk_report_queue_release();
int notify_nn_receivedblock(block_info_t *blk);
int notify_blk_report(block_info_t *blk);

int dn_kill_namenode();
#endif

