#ifndef DN_CYCLE_H
#define DN_CYCLE_H
#include <cstring>
#include <iostream>
#include "dfs_types.h"
#include "dfs_string.h"
#include "dfs_array.h"
#include "dfs_memory_pool.h"

typedef struct cycle_s  //作为一个全局变量指向nginx当前运行的上下文环境
{
    void      *sconf;
    pool_t    *pool; //内存池  
    log_t     *error_log;
    array_t    listening_for_cli; // listening array , init in cli_listen_rev_handler
    array_t    listening_for_open; // listening for open
	char       listening_ip[32]; // dn ip
	char       listening_open_ip[32];
	int 	   listening_open_port; // dn port
	int        listening_paxos_port; // paxos port
	int        listening_nssrv_port;
	std::string leaders_paxos_ipport_string;
    string_t   conf_file;  //配置文件
	void      *cfs; //配置上下文数组(含所有模块) ？// contain io processfunc in dfs_setup
    bool       stop_all = false;
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t  cond = PTHREAD_COND_INITIALIZER;
} cycle_t;

extern cycle_t *dfs_cycle;

cycle_t  *dn_cycle_create();
int       dn_cycle_init(cycle_t *cycle);
int       dn_cycle_free(cycle_t *cycle);
array_t  *cycle_get_listen_for_openport();
array_t  *cycle_get_listen_for_cli();
void      cycle_lock();
void      cycle_unlock();
void      cycle_wait();
void      cycle_signal();

int       cycle_check_sys_env(cycle_t *cycle);


#endif

