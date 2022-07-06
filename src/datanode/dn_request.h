#ifndef DN_REQUEST_H
#define DN_REQUEST_H

#include <dfs_task.h>
#include "dfs_types.h"
#include "dfs_conn.h"
#include "dfs_chain.h"
#include "cfs_fio.h"
#include "dfs_task_cmd.h"

#define CONN_POOL_SZ  4096
#define CONN_TIME_OUT 60000

#define WAIT_FIO_TASK_TIMEOUT 500

#define DN_STATUS_CLIENT_CLOSED_REQUEST         499
#define DN_STATUS_INTERNAL_SERVER_ERROR         500
#define DN_STATUS_NOT_IMPLEMENTED               501
#define DN_STATUS_BAD_GATEWAY                   502
#define DN_STATUS_SERVICE_UNAVAILABLE           503
#define DN_STATUS_GATEWAY_TIME_OUT              504
#define DN_STATUS_VERSION_NOT_SUPPORTED         505
#define DN_STATUS_INSUFFICIENT_STORAGE          507
#define DN_STATUS_CC_DENEY                      555

// add


typedef struct nn_conn_s_ nn_conn_t_;
typedef void (*nn_event_handler_t)(nn_conn_t_ *);

enum
{
    ST_CONNCECTED_ = 0,
    ST_DISCONNCECTED_ = 1
};

// 客户端主动？
struct nn_conn_s_
{
    conn_t              *connection; // 这种连接是指 客户端发起的，服务器被动接受的连接
    buffer_t            *in; // in buffer
    buffer_t            *out;
    queue_t              out_task;
    nn_event_handler_t  read_event_handler; //nn_conn_read_handler
    nn_event_handler_t  write_event_handler;
    int32_t              count; // used freetask que count
    int32_t              slow; //
    queue_t              free_task;
    pool_t              *mempool; // poolnn_request
    event_t              ev_timer;
    int32_t              max_task;
    int32_t              state; // CONNECTED
    char                 ipaddr[32];
    log_t               *log;
};
//
typedef enum dn_request_error_s 
{
    DN_REQUEST_ERROR_NONE = 0, 
    DN_REQUEST_ERROR_ZERO_OUT,
    DN_REQUEST_ERROR_SPECIAL_RESPONSE,
    DN_REQUEST_ERROR_TIMEOUT,
    DN_REQUEST_ERROR_READ_REQUEST,
    DN_REQUEST_ERROR_CONN,
    DN_REQUEST_ERROR_BLK_NO_EXIST,
    DN_REQUEST_ERROR_DISK_FAILED,
    DN_REQUEST_ERROR_IO_FAILED,
    DN_REQUEST_ERROR_PSEUDO,
    DN_REQUEST_ERROR_COMBIN,
    DN_REQUEST_ERROR_STREAMING,
    DN_REQUEST_ERROR_STOP_PLAY,
} dn_request_error_t;

typedef struct dn_request_s dn_request_t;
typedef void (*dn_event_handler_pt)(dn_request_t *);

typedef struct dn_request_s 
{
    conn_t                 *conn;//请求对应的客户端连接
    dn_event_handler_pt     read_event_handler;    /*
     * 在接收完http头部，第一次在业务上处理http请求时，http框架提供的处理方法是ngx_http_process_request。
     但如果该方法无法一次处理完该请求的全部业务，在归还控制权到epoll时间模块后，该请求再次被回调时，
     将通过Ngx_http_request_handler方法来处理，而这个方法中对于可读事件的处理就是调用read_event_handler处理请求。
     也就是说，http模块希望在底层处理请求的读事件时，重新实现read_event_handler方法
    */
	dn_event_handler_pt     write_event_handler;
    pool_t                 *pool; //这个请求的内存池
	buffer_t               *input; // input buffer
	chain_output_ctx_t     *output;/*这里要注意ngx_http_request_t中有一个out的chain，这个chain保存的是上一次还没有被发完的buf，这样每次我们接收到新的chain的话，就需要将新的chain连接到老的out chain上，然后再发出去*/
	event_t			        ev_timer;
    char                    ipaddr[32];
	data_transfer_header_t  header; // 头信息
	int                     store_fd; // 接收时用于存储文件的fd
	uchar_t                *path;
	long                    done;// 数据完成的长度
	file_io_t              *fio;
} dn_request_t;

void dn_conn_init(conn_t *c);
void dn_request_init(event_t *rev);

// add
void nn_conn_init_(conn_t *c);
void *nn_conn_get_task(nn_conn_t_ *mc);
void nn_conn_free_task(nn_conn_t_ *mc, queue_t *q);
int  nn_conn_is_connected(nn_conn_t_ *mc);
int  nn_conn_output(nn_conn_t_ *mc);
int  nn_conn_outtask(nn_conn_t_ *mc, task_t *t);
int  nn_conn_update_state(nn_conn_t_ *mc, int state);
int  nn_conn_get_state(nn_conn_t_ *mc);
void nn_conn_finalize(nn_conn_t_ *mc);
#endif

