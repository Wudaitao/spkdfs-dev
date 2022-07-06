#include <dfs_utils.h>
#include <netinet/in.h>
#include "dn_cycle.h"
#include "dfs_conf.h"
#include "dfs_memory.h"
#include "dn_conf.h"
#include "dn_time.h"
#include "dn_error_log.h"
#include "../../etc/config.h"

#define CYCLE_POOL_SIZE 16384

cycle_t *dfs_cycle;
extern string_t config_file;

cycle_t *dn_cycle_create() {
    cycle_t *cycle = nullptr;
    cycle = (cycle_t *) memory_calloc(sizeof(cycle_t));

    if (!cycle) {
        printf("create cycle faild!\n");

        return cycle;
    }

    cycle->pool = pool_create(CYCLE_POOL_SIZE, CYCLE_POOL_SIZE, nullptr);
    if (!cycle->pool) {
        memory_free(cycle, sizeof(cycle_t));
        cycle = nullptr;
    }

    return cycle;
}

// conf 和error log init
int dn_cycle_init(cycle_t *cycle) {
    log_t *log = nullptr;
    conf_context_t *ctx = nullptr;
    conf_object_t *conf_objects = nullptr;
    string_t server = string_make("Server");
    conf_server_t *sconf = nullptr;
    char paxos_ip[INET_ADDRSTRLEN] = {0};
    int paxos_port = 0;

    if (cycle == nullptr) {
        return NGX_ERROR;
    }

    cycle->conf_file.data = string_xxpdup(cycle->pool, &config_file);
    cycle->conf_file.len = config_file.len;

    if (!dfs_cycle) {
        log = error_log_init_with_stderr(cycle->pool);
        if (!log) {
            goto error;
        }

        cycle->error_log = log;
        cycle->pool->log = log;
        dfs_cycle = cycle;
    }

    error_log_set_handle(log, (log_time_ptr) time_logstr, nullptr);

    ctx = conf_context_create(cycle->pool);
    if (!ctx) {
        goto error;
    }
    // 配置文件相关
    conf_objects = get_dn_conf_object();

    if (conf_context_init(ctx, &config_file, log, conf_objects) != NGX_OK) {
        goto error;
    }
    // parse conf file and make default
    if (conf_context_parse(ctx) != NGX_OK) {
        printf("configure parse failed at line %d\n", ctx->conf_line);

        goto error;
    }

    cycle->sconf = conf_get_parsed_obj(ctx, &server);
    if (!cycle->sconf) {
        printf("no Server conf\n");

        goto error;
    }

    // error log init
    // 这一块应该移动到 dfs module init
    dn_error_log_init(dfs_cycle);

    // add listening_paxos_port
    sconf = (conf_server_t *) cycle->sconf;
    parseIpPort((char *) sconf->my_paxos.data, paxos_ip, paxos_port);

    dfs_cycle->listening_paxos_port = paxos_port;
    dfs_cycle->listening_nssrv_port = sconf->ns_srv_port;
    return NGX_OK;

    error:
    return NGX_ERROR;
}

int dn_cycle_free(cycle_t *cycle) {
    if (!cycle) {
        return NGX_OK;
    }

    if (cycle->pool) {
        pool_destroy(cycle->pool);
    }

    memory_free(cycle, sizeof(cycle_t));

    cycle = nullptr;

    return NGX_OK;
}


array_t *cycle_get_listen_for_cli() {
    return &dfs_cycle->listening_for_cli;
}

array_t *cycle_get_listen_for_openport() {
    return &dfs_cycle->listening_for_open;
}

void cycle_lock() {
    pthread_mutex_lock(&dfs_cycle->mutex);
}

void cycle_unlock() {
    pthread_mutex_unlock(&dfs_cycle->mutex);
}

void cycle_wait() {
    pthread_cond_wait(&dfs_cycle->cond,&dfs_cycle->mutex);
}

void cycle_signal() {
    pthread_cond_signal(&dfs_cycle->cond);
}

