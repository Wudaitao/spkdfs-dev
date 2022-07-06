#include <sys/socket.h>
#include <sys/ioctl.h>
#include <net/if_arp.h>
#include <net/if.h>
#include <netinet/tcp.h>
#include <cstdint>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "dfs_epoll.h"
#include "dfs_event_timer.h"
#include "dfs_memory.h"
#include "dn_request.h"
#include "dn_thread.h"
#include "dn_data_storage.h"
#include "dn_conf.h"

#include <sys/socket.h>
#include <net/if_arp.h>
#include <dfs_dbg.h>

#include "dfs_epoll.h"
#include "dfs_event_timer.h"
#include "dn_thread.h"
#include "dn_worker_process.h"
#include "dn_net_response_handler.h"
#include "dn_group.h"
#include "dn_paxos.h"


//#define CONN_TIME_OUT        300000
#define TASK_TIME_OUT        100

#define NN_TASK_POOL_MAX_SIZE 64
#define NN_TASK_POOL_MIN_SIZE 8

extern dfs_thread_t *openport_thread;

static void nn_event_process_handler(event_t *ev);

static void nn_conn_read_handler(nn_conn_t_ *mc);

static void nn_conn_write_handler(nn_conn_t_ *mc);

static int nn_conn_out_buffer(conn_t *c, buffer_t *b);

static void nn_conn_free_queue(nn_conn_t_ *mc);

static void nn_conn_close(nn_conn_t_ *mc);

static int nn_conn_recv(nn_conn_t_ *mc);

static int nn_conn_decode(nn_conn_t_ *mc);

//
static void dn_empty_handler(event_t *ev);

static void dn_request_process_handler(event_t *ev);

static void dn_request_read_header(dn_request_t *r);

static void dn_request_close(dn_request_t *r, uint32_t err);

static void dn_request_parse_header(dn_request_t *r);

static void dn_request_block_reading(dn_request_t *r);

static void dn_request_block_writing(dn_request_t *r);

static void dn_request_read_file(dn_request_t *r);

static void dn_request_write_file(dn_request_t *r);

static void dn_request_header_response(dn_request_t *r);

static void dn_request_task_response(dn_request_t *r, task_t *task);

static void dn_request_send_header_response(dn_request_t *r);

static void dn_request_send_task_response(dn_request_t *r);

static void dn_request_check_connection(dn_request_t *r,
                                        event_t *ev);

static void dn_request_check_read_connection(dn_request_t *r);

static void dn_request_check_write_connection(dn_request_t *r);

static int send_header_response(dn_request_t *r);

static void dn_request_process_body(dn_request_t *r);

static void dn_request_send_block(dn_request_t *r);

static void fio_task_alloc_timeout(event_t *ev);

static int block_read_complete(void *data, void *task);

static void dn_request_send_block_again(dn_request_t *r);

static void dn_request_recv_block(dn_request_t *r);

static void recv_block_handler(dn_request_t *r);

static int block_write_complete(void *data, void *task);

static void dn_request_write_done_response(dn_request_t *r);

static void dn_request_send_write_done_response(dn_request_t *r);

static void dn_request_read_done_response(dn_request_t *r);

static void dn_request_send_read_done_response(dn_request_t *r);

static void dn_request_send_cli_dn_group(dn_request_t *r);

void dn_request_send_cli_dn_group(dn_request_t *r) {

}

// cli_listen_rev_handler
void dn_conn_init(conn_t *c) {
    event_t *rev = nullptr;
    event_t *wev = nullptr;
    dn_request_t *r = nullptr;
    dfs_thread_t *thread = nullptr;

    thread = get_local_thread();

    rev = c->read;
    // 连接完成后，设置读事件的处理函数，用于下一次epoll event 回调
    rev->handler = dn_request_init; //process request

    wev = c->write;
    wev->handler = dn_empty_handler;

    if (!c->conn_data) {
        c->conn_data = pool_calloc(c->pool, sizeof(dn_request_t));
        if (!c->conn_data) {
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                          "pool_calloc failed");

            conn_release(c);
            conn_pool_free_connection(&thread->conn_pool, c);

            return;
        }
    }

    r = (dn_request_t *) c->conn_data;
    r->conn = c;
    memset(&r->header, 0x00, sizeof(data_transfer_header_t));
    r->store_fd = -1;

    r->pool = pool_create(CONN_POOL_SZ, CONN_POOL_SZ, dfs_cycle->error_log);
    if (!r->pool) {
        dfs_log_error(dfs_cycle->error_log,
                      DFS_LOG_FATAL, 0, "pool_create failed");

        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

        return;
    }

    snprintf(r->ipaddr, sizeof(r->ipaddr), "%s", c->addr_text.data);

    c->ev_base = &thread->event_base;
    c->ev_timer = &thread->event_timer;

    // if not ready and not active then add it to epoll
    if (event_handle_read(c->ev_base, rev, 0) == NGX_ERROR) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "add read event failed");

        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);
    }
}

static void dn_empty_handler(event_t *ev) {
    if (ev->write) {
        dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                      "dn_empty_handler write event");
    } else {
        dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                      "dn_empty_handler read event");
    }
}

// 连接成功后，开始处理请求
void dn_request_init(event_t *rev) {
    conn_t *c = nullptr;
    dn_request_t *r = nullptr;
    event_t *wev = nullptr;

    c = (conn_t *) rev->data;
    r = (dn_request_t *) c->conn_data;
    wev = c->write;

    //这个函数执行后，dn_request_process_handler。这样下次再有事件时
    //将调用dn_request_process_handler函数来处理，而不会再调用ngx_http_process_request了
    rev->handler = dn_request_process_handler;
    wev->handler = dn_request_process_handler;

    r->read_event_handler = dn_request_read_header;

    // 处理头信息
    dn_request_read_header(r);
}

// 连接完成之后的读写事件处理函数
// 处理request
static void dn_request_process_handler(event_t *ev) {
    conn_t *c = nullptr;
    dn_request_t *r = nullptr;

    c = (conn_t *) ev->data;
    r = (dn_request_t *) c->conn_data;

    if (ev->write) {
        if (!r->write_event_handler) {
            dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                          "write handler nullptr, conn_fd: %d", c->fd);

            return;
        }

        r->write_event_handler(r);
    } else {
        if (!r->read_event_handler) {
            dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                          "read handler nullptr, conn_fd: %d", c->fd);

            return;
        }

        r->read_event_handler(r);
    }
}

// 处理头信息
static void dn_request_read_header(dn_request_t *r) {
    conn_t *c = nullptr;
    event_t *rev = nullptr;
    ssize_t rs = 0;

    c = r->conn;
    rev = c->read;

    if (rev->timedout) {
        dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                      "dn_request_read_header, rev timeout conn_fd: %d", c->fd);

        dn_request_close(r, DN_REQUEST_ERROR_TIMEOUT);

        return;
    }

    if (rev->timer_set) {
        event_timer_del(c->ev_timer, rev);
    }

    if (rev->ready) {
        // sysio_unix_recv in dfs_sysio.c
        rs = c->recv(c, (uchar_t *) &r->header, sizeof(data_transfer_header_t));
    } else {
        rs = NGX_AGAIN;
    }

    if (rs > 0) {
        // 解析头信息
        dn_request_parse_header(r);
    } else if (rs <= 0) {
        if (rs == NGX_AGAIN) {
            event_timer_add(c->ev_timer, rev, CONN_TIME_OUT);

            rev->ready = NGX_FALSE;
        } else {
            dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                          "dn_request_read_header, read header err, conn_fd: %d", c->fd);

            dn_request_close(r, DN_REQUEST_ERROR_READ_REQUEST);
        }
    }
}

static void dn_request_close(dn_request_t *r, uint32_t err) {
    conn_t *c = nullptr;
    dfs_thread_t *thread = nullptr;

    dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                  "dn_request_close err: %d", err);

    c = r->conn;
    thread = get_local_thread();

    if (r->fio) {
        cfs_fio_manager_free(r->fio, &thread->fio_mgr);
        r->fio = nullptr;
    }

    if (r->store_fd > 0) {
        cfs_close((cfs_t *) dfs_cycle->cfs, r->store_fd);
        r->store_fd = -1;
    }

    if (r->pool) {
        pool_destroy(r->pool);
        r->pool = nullptr;
    }

    conn_release(c);
    conn_pool_free_connection(&thread->conn_pool, c);
}

// 解析 header
static void dn_request_parse_header(dn_request_t *r) {
    int op_type = r->header.op_type;

    r->read_event_handler = dn_request_block_reading;

    switch (op_type) {
        case OP_WRITE_BLOCK:
            dn_request_write_file(r);
            break;

        case OP_READ_BLOCK:
            dn_request_read_file(r);
            break;

        //与其他dn交互操作
        case OP_CLI_UPDATE_GROUP_FROM_DN:{
            task_t task;
            bzero(&task,sizeof(task_t));
            if(dn_group->getGlobalStatus() == GROUP_FINISH){
                task.ret = OP_STATUS_SUCCESS;
                std::string group_string = dn_group->encodeToString();
                task.data = malloc(group_string.length()+1);
                memcpy(task.data,group_string.c_str(),group_string.length());
                task.data_len = group_string.length();
            } else{
                task.ret = OP_STATUS_ERROR;
            }
            dn_request_task_response(r, &task);
            if(task.data!=nullptr){
                free(task.data);
                task.data = nullptr;

            }
            break;
        }
        // cli request killall
        case OP_CLI_REQUEST_ALL:{
            dbg("dn recv OP_CLI_REQUEST_ALL");
            task_t task;
            bzero(&task,sizeof(task_t));
            task.cmd = GROUP_SHUTDOWN;
            dn_trans_task(&task,paxos_thread);
            // return to cli
            task.ret = OP_STATUS_SUCCESS;
            task.data = nullptr;
            task.data_len = 0;
            dn_request_task_response(r, &task);
            break;
        }

        default:
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                          "unknown op_type: %d", op_type);

            dn_request_close(r, DN_REQUEST_ERROR_CONN);

            return;
    }
}

static void dn_request_block_reading(dn_request_t *r) {
    conn_t *c = nullptr;
    event_t *rev = nullptr;

    c = r->conn;
    rev = c->read;

    if (event_delete(c->ev_base, rev, EVENT_READ_EVENT, EVENT_CLEAR_EVENT)
        == NGX_ERROR) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "del read event failed");

        dn_request_close(r, DN_REQUEST_ERROR_CONN);
    }
}

static void dn_request_block_writing(dn_request_t *r) {
    conn_t *c = nullptr;
    event_t *wev = nullptr;

    c = r->conn;
    wev = c->read;

    if (epoll_del_event(c->ev_base, wev, EVENT_WRITE_EVENT, EVENT_CLEAR_EVENT)
        == NGX_ERROR) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "del write event failed");

        dn_request_close(r, DN_REQUEST_ERROR_CONN);
    }
}

static void dn_request_read_file(dn_request_t *r) {
    block_info_t *blk = nullptr;
    int fd = -1;

    blk = block_object_get(r->header.block_id);
    if (!blk) {
        dfs_log_error(dfs_cycle->error_log,
                      DFS_LOG_FATAL, 0, "blk %d does't exist", r->header.block_id);

        dn_request_close(r, DN_REQUEST_ERROR_BLK_NO_EXIST);

        return;
    }

    if (r->store_fd < 0) {
        fd = cfs_open((cfs_t *) dfs_cycle->cfs, (uchar_t *) blk->path, O_RDONLY,
                      dfs_cycle->error_log);
        if (fd < 0) {
            dfs_log_error(dfs_cycle->error_log,
                          DFS_LOG_FATAL, errno, "open file %s err", blk->path);

            dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

            return;
        }

        r->store_fd = fd;
    }

    dn_request_header_response(r);
}

//
static void dn_request_write_file(dn_request_t *r) {
    conf_server_t *sconf = nullptr;
    int fd = -1;

    sconf = (conf_server_t *) dfs_cycle->sconf;

    r->input = buffer_create(r->pool, sconf->recv_buff_len * 2);
    if (!r->input) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "buffer_create failed");

        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

        return;
    }

    //
    if (get_block_temp_path(r) != NGX_OK) {
        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

        return;
    }

    if (r->store_fd < 0) {
        fd = cfs_open((cfs_t *) dfs_cycle->cfs, r->path,
                      O_CREAT | O_WRONLY | O_TRUNC, dfs_cycle->error_log);
        if (fd < 0) {
            dfs_log_error(dfs_cycle->error_log,
                          DFS_LOG_FATAL, errno, "dn_request_write_file open file %s err", r->path);

            dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

            return;
        }

        r->store_fd = fd;
    }

    dn_request_header_response(r);
}

static void dn_request_header_response(dn_request_t *r) {
    data_transfer_header_rsp_t header_rsp;
    chain_t *out = nullptr;
    buffer_t *b = nullptr;
    conn_t *c = nullptr;
    int header_sz = 0;

    header_rsp.op_status = OP_STATUS_SUCCESS;
    header_rsp.err = NGX_OK;

    c = r->conn;
    header_sz = sizeof(data_transfer_header_rsp_t);

    out = chain_alloc(r->pool);
    if (!out) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "chain_alloc failed");

        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

        return;
    }

    b = buffer_create(r->pool, header_sz);
    if (!b) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "buffer_create failed");

        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

        return;
    }

    // header_rsp 放在 chain的buffer里
    out->buf = b;
    b->last = memory_cpymem(b->last, &header_rsp, header_sz);

    if (!r->output) {
        r->output = (chain_output_ctx_t *) pool_alloc(r->pool,
                                                      sizeof(chain_output_ctx_t));
        if (!r->output) {
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                          "pool_alloc failed");

            dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

            return;
        }
    }

    r->output->out = nullptr;
    // out 链接到chain
    chain_append_all(&r->output->out, out);

    // request handler
    r->write_event_handler = dn_request_send_header_response;
    r->read_event_handler = dn_request_check_read_connection;

    // 第一次的时候为false，因为还没有添加write事件，
    if (c->write->ready) {
        dn_request_send_header_response(r);

        return;
    }

    if (event_handle_write(c->ev_base, c->write, 0) == NGX_ERROR) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "add write event failed");

        dn_request_close(r, DN_REQUEST_ERROR_SPECIAL_RESPONSE);

        return;
    }

    event_timer_add(c->ev_timer, c->write, CONN_TIME_OUT);
}

// added
static void dn_request_task_response(dn_request_t *r, task_t *task) {
//    data_transfer_header_rsp_t header_rsp;
    chain_t *out = nullptr;
    buffer_t *b = nullptr;
    conn_t *c = nullptr;
//    int header_sz = 0;

    char sBuf[4096] = {0};
    int sLen = task_encode2str(task, sBuf, sizeof(sBuf));

    c = r->conn;
//    header_sz = sizeof(data_transfer_header_rsp_t);

    out = chain_alloc(r->pool);
    if (!out) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "chain_alloc failed");

        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

        return;
    }

    b = buffer_create(r->pool, sLen);
    if (!b) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "buffer_create failed");

        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

        return;
    }

    // header_rsp 放在 chain的buffer里
    out->buf = b;
    b->last = memory_cpymem(b->last, sBuf, sLen);

    if (!r->output) {
        r->output = (chain_output_ctx_t *) pool_alloc(r->pool,
                                                      sizeof(chain_output_ctx_t));
        if (!r->output) {
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                          "pool_alloc failed");

            dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

            return;
        }
    }

    r->output->out = nullptr;
    // out 链接到chain
    chain_append_all(&r->output->out, out);

    // request handler
    r->write_event_handler = dn_request_send_task_response;
    r->read_event_handler = dn_request_check_read_connection;

    // 第一次的时候为false，因为还没有添加write事件，
    if (c->write->ready) {
        dn_request_send_task_response(r);

        return;
    }

    if (event_handle_write(c->ev_base, c->write, 0) == NGX_ERROR) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "add write event failed");

        dn_request_close(r, DN_REQUEST_ERROR_SPECIAL_RESPONSE);

        return;
    }

    event_timer_add(c->ev_timer, c->write, CONN_TIME_OUT);
}
//
static void dn_request_send_header_response(dn_request_t *r) {
    conn_t *c = nullptr;
    event_t *wev = nullptr;
    int rs = 0;

    c = r->conn;
    wev = c->write;

    if (wev->timedout) {
        dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                      "dn_request_send_header_response, wev timeout, conn_fd: %d", c->fd);

        dn_request_close(r, DN_REQUEST_ERROR_CONN);

        return;
    }

    if (wev->timer_set) {
        event_timer_del(c->ev_timer, wev);
    }

    // send res header
    rs = send_header_response(r);
    if (rs == NGX_OK) {
        dn_request_process_body(r);

        return;
    } else if (rs == NGX_AGAIN) {
        event_timer_add(c->ev_timer, wev, CONN_TIME_OUT);

        return;
    }

    dn_request_close(r, DN_REQUEST_ERROR_SPECIAL_RESPONSE);
}
//
static void dn_request_send_task_response(dn_request_t *r) {
    conn_t *c = nullptr;
    event_t *wev = nullptr;
    int rs = 0;

    c = r->conn;
    wev = c->write;

    if (wev->timedout) {
        dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                      "dn_request_send_header_response, wev timeout, conn_fd: %d", c->fd);

        dn_request_close(r, DN_REQUEST_ERROR_CONN);

        return;
    }

    if (wev->timer_set) {
        event_timer_del(c->ev_timer, wev);
    }

    // send res header
    rs = send_header_response(r);
    if (rs == NGX_OK) {
//        dn_request_process_body(r);

        return;
    } else if (rs == NGX_AGAIN) {
        event_timer_add(c->ev_timer, wev, CONN_TIME_OUT);

        return;
    }

    dn_request_close(r, DN_REQUEST_ERROR_SPECIAL_RESPONSE);
}

static void dn_request_check_read_connection(dn_request_t *r) {
    dn_request_check_connection(r, r->conn->read);
}

static void dn_request_check_write_connection(dn_request_t *r) {
    dn_request_check_connection(r, r->conn->write);
}

static void dn_request_check_connection(dn_request_t *r,
                                        event_t *ev) {
    conn_t *c = nullptr;
    char buf[1] = "";
    int rs = 0;

    c = r->conn;
    ev = c->read;

    if (ev->timedout) {
        dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                      "dn_request_check_connection, ev timeout, conn_fd: %d", c->fd);

        dn_request_close(r, DN_REQUEST_ERROR_TIMEOUT);

        return;
    }

    errno = 0;

    rs = recv(c->fd, buf, 1, MSG_PEEK);
    if (rs > 0) {
        return;
    }

    if (rs == 0) {
        if (ev->write) {
            return;
        }
    }

    if (errno == DFS_EAGAIN || errno == DFS_EINTR) {
        return;
    }

    dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, errno,
                  "client prematurely to close the connection");

    dn_request_close(r, DN_REQUEST_ERROR_CONN);
}

static int send_header_response(dn_request_t *r) {
    conn_t *c = nullptr;
    chain_output_ctx_t *ctx = nullptr;

    c = r->conn;
    ctx = r->output;

    while (c->write->ready && ctx->out) {
        //如果一次发送不完，需要将剩下的响应头部保存到r->out链表中，以备后续发送：
        ctx->out = c->send_chain(c, ctx->out, 0); // sysio_writev_chain

        if (ctx->out == DFS_CHAIN_ERROR || !c->write->ready) {
            break;
        }
    }

    if (ctx->out == DFS_CHAIN_ERROR) {
        return NGX_ERROR;
    }

    if (ctx->out) {
        return NGX_AGAIN;
    }

    return NGX_OK;
}

static void dn_request_process_body(dn_request_t *r) {
    dfs_thread_t *thread = nullptr;
    conn_t *c = nullptr;

    thread = get_local_thread();
    c = r->conn;

    if (!r->fio) {
        r->fio = cfs_fio_manager_alloc(&thread->fio_mgr);
        if (!r->fio) {
            memset(&r->ev_timer, 0x00, sizeof(event_t));
            r->ev_timer.handler = fio_task_alloc_timeout;
            r->ev_timer.data = r;

            event_timer_add(c->ev_timer, &r->ev_timer, WAIT_FIO_TASK_TIMEOUT);

            return;
        }
    }

    if (r->header.op_type == OP_WRITE_BLOCK) {
        dn_request_recv_block(r);
    } else if (r->header.op_type == OP_READ_BLOCK) {
        dn_request_send_block(r);
    }
}

static void dn_request_send_block(dn_request_t *r) {
    conn_t *c = nullptr;
    sendfile_chain_task_t *sf_chain_task = nullptr;

    c = r->conn;

    r->write_event_handler = dn_request_block_writing;

    if (!r->fio->sf_chain_task) {
        sf_chain_task = (sendfile_chain_task_t *) pool_alloc(r->pool,
                                                             sizeof(sendfile_chain_task_t));

        if (!sf_chain_task) {
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                          "pool_alloc failed");

            dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

            return;
        }
        printf("dn_request_send_block conn fd:%d file fd :%d\n", c->fd, r->store_fd);
        sf_chain_task->conn_fd = c->fd;
        sf_chain_task->store_fd = r->store_fd;

        r->fio->sf_chain_task = sf_chain_task;
    }

    // fix : no sf_chain_task conn_fd and store_fd

    sf_chain_task = static_cast<sendfile_chain_task_t *>(r->fio->sf_chain_task);
    sf_chain_task->conn_fd = c->fd;
    sf_chain_task->store_fd = r->store_fd;

    // end
    r->fio->fd = r->store_fd;
    r->fio->offset = r->header.start_offset;
    r->fio->need = r->header.len;
    r->fio->data = r;
    r->fio->h = block_read_complete;
    r->fio->io_event = &get_local_thread()->io_events;
    r->fio->faio_ret = NGX_ERROR;
    r->fio->faio_noty = &get_local_thread()->faio_notify;

    if (cfs_sendfile_chain((cfs_t *) dfs_cycle->cfs, r->fio,
                           dfs_cycle->error_log) != NGX_OK) {
        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);
    }
}

static void fio_task_alloc_timeout(event_t *ev) {
    dn_request_t *r = nullptr;
    conn_t *c = nullptr;

    r = (dn_request_t *) ev->data;
    c = r->conn;

    if (!ev->timedout) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "event error, not timer out, conn_fd: %d", c->fd);

        dn_request_close(r, DN_REQUEST_ERROR_CONN);

        return;
    }

    dn_request_process_body(r);
}

static int block_read_complete(void *data, void *task) {
    dn_request_t *r = nullptr;
    conn_t *c = nullptr;
    file_io_t *fio = nullptr;
    int rs = NGX_ERROR;

    r = (dn_request_t *) data;
    c = r->conn;
    fio = (file_io_t *) task;
    rs = fio->faio_ret;

    if (rs == DFS_EAGAIN) {
        r->write_event_handler = dn_request_send_block_again;

        if (c->write->ready) {
            dn_request_send_block_again(r);

            return NGX_OK;
        }

        if (event_handle_write(c->ev_base, c->write, 0) == NGX_ERROR) {
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                          "add write event failed");

            dn_request_close(r, DN_REQUEST_ERROR_CONN);

            return NGX_ERROR;
        }

        event_timer_add(c->ev_timer, c->write, CONN_TIME_OUT);

        return NGX_OK;
    } else if (rs == NGX_ERROR) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "send block failed");

        dn_request_close(r, DN_REQUEST_ERROR_CONN);

        return NGX_ERROR;
    }

    dn_request_read_done_response(r);

    dn_request_close(r, DN_REQUEST_ERROR_NONE);

    return NGX_OK;
}

static void dn_request_send_block_again(dn_request_t *r) {
    conn_t *c = nullptr;
    event_t *wev = nullptr;
    dfs_thread_t *thread = nullptr;

    c = r->conn;
    wev = c->write;
    thread = get_local_thread();

    if (wev->timedout) {
        dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                      "wev timeout, conn_fd: %d", c->fd);

        dn_request_close(r, DN_REQUEST_ERROR_CONN);

        return;
    }

    if (wev->timer_set) {
        event_timer_del(c->ev_timer, wev);
    }

    if (cfs_sendfile_chain((cfs_t *) dfs_cycle->cfs, r->fio,
                           dfs_cycle->error_log) != NGX_OK) {
        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);
    }
}

static void dn_request_recv_block(dn_request_t *r) {
    conn_t *c = nullptr;
    event_t *rev = nullptr;

    c = r->conn;
    rev = c->read;

    r->read_event_handler = recv_block_handler;
    r->write_event_handler = dn_request_block_writing;

    if (rev->ready) {
        recv_block_handler(r);

        return;
    }

    if (event_handle_read(c->ev_base, rev, 0) == NGX_ERROR) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "add read event failed");

        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

        return;
    }

    event_timer_add(c->ev_timer, rev, CONN_TIME_OUT);
}

static void recv_block_handler(dn_request_t *r) {
    int rs = 0;
    size_t blen = 0;
    conn_t *c = nullptr;
    event_t *rev = nullptr;

    c = r->conn;
    rev = c->read;

    if (rev->timedout) {
        dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                      "rev timeout, conn_fd: %d", c->fd);

        dn_request_close(r, DN_REQUEST_ERROR_CONN);

        return;
    }

    if (rev->timer_set) {
        event_timer_del(c->ev_timer, rev);
    }

    while (1) {
        buffer_shrink(r->input);// 紧缩buffer

        blen = buffer_free_size(r->input);
        if (!blen) {
            break;
        }
        // sysio_unix_recv
        //
        rs = c->recv(c, r->input->last, blen);
        if (rs > 0) {
            r->input->last += rs;

            continue;
        }

        if (rs == 0) {
            dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                          "client is closed, conn_fd: %d", c->fd);

            dn_request_close(r, DN_REQUEST_ERROR_CONN);

            return;
        }

        if (rs == NGX_ERROR) {
            dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, errno,
                          "net err, conn_fd: %d", c->fd);

            dn_request_close(r, DN_REQUEST_ERROR_CONN);

            return;
        }

        if (rs == NGX_AGAIN) {
            break;
        }
    }

    r->read_event_handler = dn_request_block_reading;

    r->fio->fd = r->store_fd;
    r->fio->b = r->input;
    r->fio->need = buffer_size(r->input);
    r->fio->offset = r->done;
    r->fio->data = r;
    r->fio->h = block_write_complete; // fio handler
    r->fio->io_event = &get_local_thread()->io_events;
    r->fio->faio_ret = NGX_ERROR;
    r->fio->faio_noty = &get_local_thread()->faio_notify;

    if (cfs_write((cfs_t *) dfs_cycle->cfs, r->fio,
                  dfs_cycle->error_log) != NGX_OK) {
        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);
    }
}

// param data is request , task is fio it self
static int block_write_complete(void *data, void *task) {
    dn_request_t *r = nullptr;
    file_io_t *fio = nullptr;
    int rs = NGX_ERROR;

    r = (dn_request_t *) data;
    fio = (file_io_t *) task;
    rs = fio->faio_ret;

    if (rs == NGX_ERROR) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "do fio task failed");

        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

        return NGX_ERROR;
    }

    if (rs != fio->need) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "write block failed, rs: %d, need: %d", rs, fio->need);

        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

        return NGX_ERROR;
    }

    r->done += rs;// 完成了多少
    if (r->done < r->header.len)  // 数据没有发送或者接受完就继续发送或者接收
    {
        buffer_reset(r->input);
        //dn_request_recv_block(r);
        recv_block_handler(r);

        return NGX_OK;
    }
//    dbg("blk_write_done");
    // close fd
    cfs_close((cfs_t *) dfs_cycle->cfs, r->store_fd);
    r->store_fd = -1;

    write_block_done(r);

    dn_request_write_done_response(r);

    dn_request_close(r, DN_REQUEST_ERROR_NONE);

    return NGX_OK;
}

static void dn_request_write_done_response(dn_request_t *r) {
    data_transfer_header_rsp_t header_rsp;
    chain_t *out = nullptr;
    buffer_t *b = nullptr;
    conn_t *c = nullptr;
    int header_sz = 0;

    header_rsp.op_status = OP_STATUS_SUCCESS;
    header_rsp.err = NGX_OK;

    c = r->conn;
    header_sz = sizeof(data_transfer_header_rsp_t);

    out = chain_alloc(r->pool);
    if (!out) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "chain_alloc failed");

        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

        return;
    }

    b = buffer_create(r->pool, header_sz);
    if (!b) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "buffer_create failed");

        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

        return;
    }

    out->buf = b;
    b->last = memory_cpymem(b->last, &header_rsp, header_sz);

    if (!r->output) {
        r->output = (chain_output_ctx_t *) pool_alloc(r->pool,
                                                      sizeof(chain_output_ctx_t));
        if (!r->output) {
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                          "pool_alloc failed");

            dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

            return;
        }
    }

    r->output->out = nullptr;
    chain_append_all(&r->output->out, out);

    r->write_event_handler = dn_request_send_write_done_response;
    r->read_event_handler = dn_request_check_read_connection;

    if (c->write->ready) {
        dn_request_send_write_done_response(r);

        return;
    }

    if (event_handle_write(c->ev_base, c->write, 0) == NGX_ERROR) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "add write event failed");

        dn_request_close(r, DN_REQUEST_ERROR_SPECIAL_RESPONSE);

        return;
    }

    event_timer_add(c->ev_timer, c->write, CONN_TIME_OUT);
}

static void dn_request_send_write_done_response(dn_request_t *r) {
    conn_t *c = nullptr;
    event_t *wev = nullptr;
    int rs = 0;

    c = r->conn;
    wev = c->write;

    if (wev->timedout) {
        dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                      "dn_request_send_header_response, wev timeout, conn_fd: %d", c->fd);

        dn_request_close(r, DN_REQUEST_ERROR_CONN);

        return;
    }

    if (wev->timer_set) {
        event_timer_del(c->ev_timer, wev);
    }

    rs = send_header_response(r);
    if (rs == NGX_OK) {
        return;
    } else if (rs == NGX_AGAIN) {
        event_timer_add(c->ev_timer, wev, CONN_TIME_OUT);

        return;
    }

    dn_request_close(r, DN_REQUEST_ERROR_SPECIAL_RESPONSE);
}

static void dn_request_read_done_response(dn_request_t *r) {
    data_transfer_header_rsp_t header_rsp;
    chain_t *out = nullptr;
    buffer_t *b = nullptr;
    conn_t *c = nullptr;
    int header_sz = 0;

    header_rsp.op_status = OP_STATUS_SUCCESS;
    header_rsp.err = NGX_OK;

    c = r->conn;
    header_sz = sizeof(data_transfer_header_rsp_t);

    out = chain_alloc(r->pool);
    if (!out) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "chain_alloc failed");

        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

        return;
    }

    b = buffer_create(r->pool, header_sz);
    if (!b) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "buffer_create failed");

        dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

        return;
    }

    out->buf = b;
    b->last = memory_cpymem(b->last, &header_rsp, header_sz);

    if (!r->output) {
        r->output = (chain_output_ctx_t *) pool_alloc(r->pool,
                                                      sizeof(chain_output_ctx_t));
        if (!r->output) {
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                          "pool_alloc failed");

            dn_request_close(r, DN_STATUS_INTERNAL_SERVER_ERROR);

            return;
        }
    }

    r->output->out = nullptr;
    chain_append_all(&r->output->out, out);

    r->write_event_handler = dn_request_send_read_done_response;
    r->read_event_handler = dn_request_check_read_connection;

    if (c->write->ready) {
        dn_request_send_read_done_response(r);

        return;
    }

    if (event_handle_write(c->ev_base, c->write, 0) == NGX_ERROR) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "add write event failed");

        dn_request_close(r, DN_REQUEST_ERROR_SPECIAL_RESPONSE);

        return;
    }

    event_timer_add(c->ev_timer, c->write, CONN_TIME_OUT);
}

static void dn_request_send_read_done_response(dn_request_t *r) {
    conn_t *c = nullptr;
    event_t *wev = nullptr;
    int rs = 0;

    c = r->conn;
    wev = c->write;

    if (wev->timedout) {
        dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                      "dn_request_send_header_response, wev timeout, conn_fd: %d", c->fd);

        dn_request_close(r, DN_REQUEST_ERROR_CONN);

        return;
    }

    if (wev->timer_set) {
        event_timer_del(c->ev_timer, wev);
    }

    rs = send_header_response(r);
    if (rs == NGX_OK) {
        return;
    } else if (rs == NGX_AGAIN) {
        event_timer_add(c->ev_timer, wev, CONN_TIME_OUT);

        return;
    }

    dn_request_close(r, DN_REQUEST_ERROR_SPECIAL_RESPONSE);
}



// add


static void nn_conn_t_imer_handler(event_t *ev) {
    nn_conn_t_ *mc = (nn_conn_t_ *) ev->data;

    event_timer_del(mc->connection->ev_timer, ev);
    if (event_handle_read(mc->connection->ev_base, mc->connection->read, 0)) {
        nn_conn_finalize(mc);

        return;
    }

    nn_conn_write_handler(mc);
    nn_conn_read_handler(mc);
}

// 入口。。。
// from cli_listen_rev_handler
// 初始化分配 max task个 wb_node *

void nn_conn_init_(conn_t *c) {
    event_t *rev = nullptr;
    event_t *wev = nullptr;
    nn_conn_t_ *mc = nullptr;
    pool_t *pool = nullptr;
    wb_node_t *node = nullptr;
    wb_node_t *buff = nullptr;
    dfs_thread_t *thread = nullptr;
    int32_t i = 0;

    thread = get_local_thread();

    pool = pool_create(4096, 4096, dfs_cycle->error_log);
    if (!pool) {
        dfs_log_error(dfs_cycle->error_log,
                      DFS_LOG_FATAL, 0, "pool create failed");

        goto error;
    }

    rev = c->read;
    wev = c->write;

    if (!c->conn_data) {
        c->conn_data = pool_calloc(pool, sizeof(nn_conn_t_));
    }
    // mc => nn_conn
    mc = (nn_conn_t_ *) c->conn_data;
    if (!mc) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                      "mc create failed");

        goto error;
    }

    mc->mempool = pool;
    mc->log = dfs_cycle->error_log;
    mc->in = buffer_create(mc->mempool,
                           ((conf_server_t *) dfs_cycle->sconf)->recv_buff_len * 2);
    mc->out = buffer_create(mc->mempool,
                            ((conf_server_t *) dfs_cycle->sconf)->send_buff_len * 2);
    if (!mc->in || !mc->out) {
        dfs_log_error(mc->log, DFS_LOG_ALERT, 0, "buffer_create failed");

        goto error;
    }

    c->ev_base = &thread->event_base;
    c->ev_timer = &thread->event_timer;
    mc->max_task = ((conf_server_t *) dfs_cycle->sconf)->max_tqueue_len;
    mc->count = 0;
    mc->connection = c;
    mc->slow = 0;

    snprintf(mc->ipaddr, sizeof(mc->ipaddr), "%s", c->addr_text.data);

    queue_init(&mc->free_task);

    // 分配 max_task 的内存
    buff = (wb_node_t *) pool_alloc(mc->mempool, mc->max_task * sizeof(wb_node_t));
    if (!buff) {
        goto error;
    }

    // 初始化分配 max task个 wb_node *
    for (i = 0; i < mc->max_task; i++) {
        node = buff + i; // 每个node都是一个单独的 queue
        node->qnode.tk.opq = &node->wbt;
        node->qnode.tk.data = nullptr;
        (node->wbt).mc = mc;

        // process event 时 accept事件 只会由 THREAD_DN OR THREAD_OPEN_PORT 处理
        // todo: change type here
        if (THREAD_OPEN_PORT == thread->type) {
            (node->wbt).thread = openport_thread;
        }
//		if (THREAD_DN == thread->type)
//        {
//            (node->wbt).thread = dn_thread;
//        }


        queue_insert_head(&mc->free_task, &node->qnode.qe);
    }

    memset(&mc->ev_timer, 0, sizeof(event_t));
    mc->ev_timer.data = mc; //
    mc->ev_timer.handler = nn_conn_t_imer_handler;

    rev->handler = nn_event_process_handler;
    wev->handler = nn_event_process_handler;

    queue_init(&mc->out_task);
    // recv buf to mc->in
    // decode task from mc->in
    // dispatch task to task_threads[]
    // called in nn_event_process_handler
    mc->read_event_handler = nn_conn_read_handler;

    nn_conn_update_state(mc, ST_CONNCECTED_);

    // add read event
    if (event_handle_read(c->ev_base, rev, 0) == NGX_ERROR) {
        dfs_log_error(mc->log, DFS_LOG_ALERT, 0, "add read event failed");

        goto error;
    }

    return;

    error:
    if (mc && mc->mempool) {
        pool_destroy(mc->mempool);
    }

    conn_release(c);
    conn_pool_free_connection(&thread->conn_pool, c);
}

// read \ write event handler
// 执行对应mc 的 write event handler 或者 read event handler
static void nn_event_process_handler(event_t *ev) {
    conn_t *c = nullptr;
    nn_conn_t_ *mc = nullptr;

    c = (conn_t *) ev->data;
    mc = (nn_conn_t_ *) c->conn_data;

    if (ev->write) {
        if (!mc->write_event_handler) {
            dfs_log_error(mc->log, DFS_LOG_ERROR,
                          0, "write handler nullptr, fd:%d", c->fd);

            return;
        }

        mc->write_event_handler(mc);
    } else {
        if (!mc->read_event_handler) {
            dfs_log_debug(mc->log, DFS_LOG_DEBUG,
                          0, "read handler nullptr, fd:%d", c->fd);

            return;
        }

        mc->read_event_handler(mc); //nn_conn_read_handler
    }
}

//static void nn_empty_handler(event_t *ev)
//{
//}


// recv buf to mc->in
static int nn_conn_recv(nn_conn_t_ *mc) {
    int n = 0;
    conn_t *c = nullptr;
    size_t blen = 0;

    c = mc->connection;

    while (true) {
        buffer_shrink(mc->in);

        blen = buffer_free_size(mc->in);
        if (!blen) {
            return NGX_BUFFER_FULL;
        }

        n = c->recv(c, mc->in->last, blen); //sysio_unix_recv
        if (n > 0) // move point to buffer end
        {
            mc->in->last += n;

            continue;
        }

        if (n == 0) {
            return NGX_CONN_CLOSED;
        }

        if (n == NGX_ERROR) {
            return NGX_ERROR;
        }

        if (n == NGX_AGAIN) {
            return NGX_AGAIN;
        }
    }

    return NGX_OK;
}

// from nn_conn_read_handler
// decode task from mc->in
// dispatch task to task_threads[]
static int nn_conn_decode(nn_conn_t_ *mc) {
    int rc = 0;
    task_queue_node_t *node = nullptr;

    while (true) {
        // pop free task from free_task queue
        node = (task_queue_node_t *) nn_conn_get_task(mc);
        if (!node) {
            dfs_log_error(mc->log, DFS_LOG_ERROR, 0,
                          "mc to repiad remove from epoll");

            mc->slow = 1;
            event_del_read(mc->connection->ev_base, mc->connection->read);
            event_timer_add(mc->connection->ev_timer, &mc->ev_timer, 1000);

            return NGX_BUSY;
        }

        // decode task to _in buffer
//        printf("nn_conn_decode recv: %s %ld\n", (char *) mc->in->pos, buffer_size(mc->in));

        rc = task_decode(mc->in, &node->tk);
        if (rc == NGX_OK) {
            // dispatch task when recv it
            // push task to dispatch_last_task->tq
            // notice_wake_up (&dispatch_last_task->tq_notice
            dispatch_task_(node);

            continue;
        }

        if (rc == NGX_ERROR) {
            nn_conn_free_task(mc, &node->qe);
            buffer_reset(mc->in);

            return NGX_ERROR;
        }

        if (rc == NGX_AGAIN)  // buffer 不够了也
        {
            nn_conn_free_task(mc, &node->qe);
            buffer_shrink(mc->in);

            return NGX_AGAIN;
        }
    }

    return NGX_OK;
}

// 入口。。。
// listen_rev_handler
// nn_conn_init_
// recv buf to mc->in
// decode task from mc->in
// dispatch task to task_threads[]
static void nn_conn_read_handler(nn_conn_t_ *mc) {
    int rc = 0;
    char *err = nullptr;

    if (mc->state == ST_DISCONNCECTED_) {
        return;
    }

    // recv buf to mc->in
    rc = nn_conn_recv(mc);
    uchar_t *addr = mc->connection->addr_text.data;
//    printf("%s\n",addr);
    char tmpchar[50];
//    dbg(addr);
//    dbg(rc);
    switch (rc) {
        case NGX_CONN_CLOSED:
//        err = (char *)"conn closed";
            sprintf(tmpchar, "%s conn closed", addr);
            err = tmpchar;
            goto error;

        case NGX_ERROR:
            err = (char *) "recv error, conn to be close";

            goto error;

        case NGX_AGAIN:
        case NGX_BUFFER_FULL:
            break;
        default:
            goto error;
    }
    // decode task from mc->in
    // dispatch task to task_threads[]


    rc = nn_conn_decode(mc);
    if (rc == NGX_ERROR) {
        err = (char *) "nn_conn_decode error";

        goto error;
    }

    return;

    error:

    dfs_log_error(mc->log, DFS_LOG_ALERT, 0, err);
    nn_conn_finalize(mc);
}

// mc connect write event
static void nn_conn_write_handler(nn_conn_t_ *mc) {
    conn_t *c = nullptr;

    if (mc->state == ST_DISCONNCECTED_) {
        return;
    }

    c = mc->connection; //获取到被动连接的connection

    if (c->write->timedout) {
        dfs_log_error(mc->log, DFS_LOG_FATAL, 0,
                      "conn_write timer out");

        nn_conn_finalize(mc);

        return;
    }

    if (c->write->timer_set) {
        event_timer_del(c->ev_timer, c->write);
    }
    // mc->write_event_handler = nn_conn_write_handler;
    // send buffer
    // add event
    nn_conn_output(mc);
}

void nn_conn_close(nn_conn_t_ *mc) {
    conn_pool_t *pool = nullptr;

    pool = thread_get_conn_pool();
    if (mc->connection) {
        mc->connection->conn_data = nullptr;
        conn_release(mc->connection);
        conn_pool_free_connection(pool, mc->connection);
        mc->connection = nullptr;
    }
}

int nn_conn_is_connected(nn_conn_t_ *mc) {
    return mc->state == ST_CONNCECTED_;
}

// send buffer
static int nn_conn_out_buffer(conn_t *c, buffer_t *b) {
    size_t size = 0;
    int rc = 0;

    if (!c->write->ready) {
        return NGX_AGAIN;
    }

    size = buffer_size(b);

    while (size) {
        rc = c->send(c, b->pos, size);
        if (rc < 0) {
            return rc;
        }

        b->pos += rc,
                size -= rc;
    }

    buffer_reset(b);

    return NGX_OK;
}

// 插入 task -> mc conn -> out task
// mc->write_event_handler = nn_conn_write_handler;
// send out buffer
// add event
int nn_conn_outtask(nn_conn_t_ *mc, task_t *t) {
    task_queue_node_t *node = nullptr;
    node = queue_data(t, task_queue_node_t, tk);

    if (mc->state != ST_CONNCECTED_) {
        nn_conn_free_task(mc, &node->qe);

        return NGX_OK;
    }

    queue_insert_tail(&mc->out_task, &node->qe);

    return nn_conn_output(mc);
}

// mc->write_event_handler = nn_conn_write_handler;
// send out buffer
// add event
int nn_conn_output(nn_conn_t_ *mc) {
    conn_t *c = nullptr;
    int rc = 0;
    task_t *t = nullptr;
    task_queue_node_t *node = nullptr;
    queue_t *qe = nullptr;
    char *err_msg = nullptr;

    c = mc->connection;

    if (!c->write->ready && buffer_size(mc->out) > 0) {
        return NGX_AGAIN;
    }

    mc->write_event_handler = nn_conn_write_handler;

    repack:
    buffer_shrink(mc->out);

    while (!queue_empty(&mc->out_task)) {
        qe = queue_head(&mc->out_task);
        node = queue_data(qe, task_queue_node_t, qe);
        t = &node->tk;

        // encode task to out buffer
        rc = task_encode(t, mc->out);
        if (rc == NGX_OK)  // 这个task push完成就释放空间
        {
            queue_remove(qe);
            nn_conn_free_task(mc, qe);

            continue;
        }

        if (rc == NGX_AGAIN)  // 塞满一个buffer 就发
        {
            goto send;
        }

        if (rc == NGX_ERROR) {
            queue_remove(qe);
            nn_conn_free_task(mc, qe);
        }
    }

    send:
    if (!buffer_size(mc->out))  // buffer size 0
    {
        return NGX_OK;
    }

    // send buffer
    rc = nn_conn_out_buffer(c, mc->out);
    if (rc == NGX_ERROR) {
        err_msg = (char *) "send data error  close conn";

        goto close;
    }

    if (rc == NGX_AGAIN) {
        if (event_handle_write(c->ev_base, c->write, 0) == NGX_ERROR) {
            dfs_log_error(mc->log, DFS_LOG_FATAL, 0, "event_handle_write");

            return NGX_ERROR;
        }

        event_timer_add(c->ev_timer, c->write, CONN_TIME_OUT);

        return NGX_AGAIN;
    }

    goto repack;

    close:
    dfs_log_error(c->log, DFS_LOG_FATAL, 0, err_msg);
    nn_conn_finalize(mc);

    return NGX_ERROR;
}

// pop free task from free_task queue
void *nn_conn_get_task(nn_conn_t_ *mc) {
    queue_t *queue = nullptr;
    task_queue_node_t *node = nullptr;

    if (mc->count >= mc->max_task) {
        dfs_log_error(mc->log, DFS_LOG_DEBUG, 0,
                      "get mc taskcount:%d", mc->count);

        return nullptr;
    }

    queue = queue_head(&mc->free_task); //
    node = queue_data(queue, task_queue_node_t, qe); //
    queue_remove(queue);
    mc->count++; //

    return node;
}


void nn_conn_free_task(nn_conn_t_ *mc, queue_t *q) {
    mc->count--;

    task_queue_node_t *node = queue_data(q, task_queue_node_t, qe);
    task_t *task = &node->tk;
    if (nullptr != task->data && task->data_len > 0) {
        free(task->data);
        task->data = nullptr;
    }

    queue_init(q);
    queue_insert_tail(&mc->free_task, q);

    if (mc->state == ST_DISCONNCECTED_ && mc->count == 0) {
        nn_conn_finalize(mc);
    }
}

static void nn_conn_free_queue(nn_conn_t_ *mc) {
    queue_t *qn = nullptr;

    while (!queue_empty(&mc->out_task)) {
        qn = queue_head(&mc->out_task);
        queue_remove(qn);
        nn_conn_free_task(mc, qn);
    }
}

int nn_conn_update_state(nn_conn_t_ *mc, int state) {
    mc->state = state;

    return NGX_OK;
}

int nn_conn_get_state(nn_conn_t_ *mc) {
    return mc->state;
}

void nn_conn_finalize(nn_conn_t_ *mc) {
    if (mc->state == ST_CONNCECTED_) {
        if (mc->ev_timer.timer_set) {
            event_timer_del(mc->connection->ev_timer, &mc->ev_timer);
        }

        nn_conn_close(mc);
        nn_conn_free_queue(mc);
        nn_conn_update_state(mc, ST_DISCONNCECTED_);
    }

    if (mc->count > 0) {
        return;
    }

    pool_destroy(mc->mempool);
}

