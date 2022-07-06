#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <dfs_dbg.h>
#include "dn_ns_service.h"
#include "dfs_types.h"
#include "dfs_task.h"
#include "dn_cycle.h"
#include "dn_time.h"
#include "dn_conf.h"
#include "dn_main.h"
#include "dn_net_response_handler.h"
#include "dn_group.h"
#include "dn_process.h"
#include "dn_signal.h"

#define BUF_SZ 4096


extern sys_info_t dfs_sys_info;
typedef struct recv_blk_report_s {
    queue_t que;
    int num;
    pthread_mutex_t lock;
    pthread_cond_t cond;
//    pthread_condattr_t cattr;
} recv_blk_report_t;

typedef struct blk_report_s {
    queue_t que;
    int num;
    pthread_mutex_t lock;
    pthread_cond_t cond;
} blk_report_t;

recv_blk_report_t g_recv_blk_report;
blk_report_t g_blk_report;

static int ns_srv_init(char *ip, int port);

static int send_heartbeat(dfs_thread_t *thread);

static int receivedblock_report(dfs_thread_t *thread);

static int wait_to_work(int second);

static int block_report(int sockfd);

static int delete_blks(char *p, int len);

// 连接上 namenode 注册datanode
// 获取 namespaceid
int dn_register(dfs_thread_t *thread) {
    int sockfd = ns_srv_init(thread->ns_info.ip, thread->ns_info.port); // name server info
    if (sockfd < 0) {
        return NGX_ERROR;
    }

    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = DN_REGISTER;
    strcpy(out_t.key, dfs_cycle->listening_ip); // bind for cli ip
//    dbg(dfs_cycle->listening_ip);

    //send grouplist change mess to local nn
    std::string glstr;
    glstr = dn_group->encodeToString();

    out_t.data_len = glstr.size();
    out_t.data = (void *) glstr.c_str();

    //end
    char sBuf[BUF_SZ] = "";
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    int ws = write(sockfd, sBuf, sLen);
    if (ws != sLen) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "write err, ws: %d, sLen: %d", ws, sLen);

        close(sockfd);

        return NGX_ERROR;
    }

    int pLen = 0;
    int rLen = recv(sockfd, &pLen, sizeof(int), MSG_PEEK);
    if (rLen < 0) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "recv err, rLen: %d", rLen);

        close(sockfd);

        return NGX_ERROR;
    }

    char *pNext = (char *) malloc(pLen);
    if (nullptr == pNext) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "malloc err, pLen: %d", pLen);

        close(sockfd);

        return NGX_ERROR;
    }

    rLen = read(sockfd, pNext, pLen);
    if (rLen < 0) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "read err, rLen: %d", rLen);

        close(sockfd);

        free(pNext);
        pNext = nullptr;

        return NGX_ERROR;
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(pNext, rLen, &in_t);

    if (in_t.ret != NGX_OK) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, 0,
                      "dn_register err, ret: %d", in_t.ret);

        close(sockfd);

        free(pNext);
        pNext = nullptr;

        return NGX_ERROR;
    } else if (nullptr != in_t.data && in_t.data_len > 0) {
        thread->ns_info.namespaceID = *(int64_t *) in_t.data;
    }

    thread->ns_info.sockfd = sockfd;

    free(pNext);
    pNext = nullptr;

    return NGX_OK;
}

// socket 链接 namenode 返回 sockfd
static int ns_srv_init(char *ip, int port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (-1 == sockfd) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "socket() err");

        return NGX_ERROR;
    }

    int reuse = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in servaddr{};
    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    servaddr.sin_addr.s_addr = inet_addr(ip);

    int iRet = connect(sockfd, (struct sockaddr *) &servaddr, sizeof(servaddr));
    if (iRet < 0) {
	    dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
			"connect(%s: %d) err", ip, port);

        return NGX_ERROR;
    }

    return sockfd;
}

// namenode 上报 receivedblock_report、 block_report
int offer_service(dfs_thread_t *thread, dfs_thread_t *local_thread) {
    auto *sconf = (conf_server_t *) dfs_cycle->sconf;
    int heartbeat_interval = sconf->heartbeat_interval; // 心跳间隔
    unsigned long g_last_heartbeat = 0;
//	int block_report_interval = sconf->block_report_interval; // default is 3

//    struct timeval now{};
//	gettimeofday(&now, nullptr);
    unsigned long now_time = CurrentTimeSec();
    unsigned long diff = 0; // 当前时间 - 上一次heartbeat的时间
    while (thread->running) {
//        printf("before send, time: %ld,ip:%s\n", now_time, thread->ns_info.ip);
        if (diff >= heartbeat_interval) {
            g_last_heartbeat = now_time;
            if (send_heartbeat(thread) != NGX_OK) {
                goto out;
            }
        }

        // 提示name node 收到 blk
        // 从 cli 接收完成之后上报
        if (g_recv_blk_report.num > 0) // 接收的？send receivedblock_report
        {
            if (receivedblock_report(thread) != NGX_OK) {
                goto out;
            }
        }

        // 从scanner 那里扫描到，不在hashtable里的上报
        if (g_blk_report.num > 0)  // 上报的？
        {
            if (block_report(thread->ns_info.sockfd) != NGX_OK) {
                goto out;
            }
        }

        int ptime = heartbeat_interval - (int) diff;
        int wtime = ptime > 0 ? ptime : heartbeat_interval; // wait time
        // wait wtime
        if (wtime > 0 && g_recv_blk_report.num == 0 && g_blk_report.num == 0) {
//            g_last_heartbeat = now_time;
            // 阻塞并等待

            wait_to_work(wtime);
        }

        // send task to local namenode
        if (local_thread != nullptr) {
            if (thread->thread_id == local_thread->thread_id) {
                send_local_task(thread);

            }
        }
        //end

        now_time = CurrentTimeSec(); // seconds
        diff = now_time - g_last_heartbeat;
//        printf("after send, time: %ld,ip:%s\n", now_time, thread->ns_info.ip);

    }

    out:
    close(thread->ns_info.sockfd);
    thread->ns_info.sockfd = -1;
    thread->ns_info.namespaceID = -1;
    dfs_log_error(dfs_cycle->error_log, DFS_LOG_INFO, 0, "heart beat thread exit, to namenode %s\n",
                  thread->ns_info.ip);
    return NGX_ERROR;
}
int printcount=0;
int printfrequency=100;
static int send_heartbeat(dfs_thread_t *thread) {
    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = DN_HEARTBEAT;
    strcpy(out_t.key, dfs_cycle->listening_ip); // 上报自己的ip

    // when send heatbeat , upload dn sys info
    dn_get_info(&dfs_sys_info);
    dn_group->Own.setCapacity(dfs_sys_info.capacity);
    dn_group->Own.setDfsUsed(dfs_sys_info.dfs_used);
    dn_group->Own.setRemaining(dfs_sys_info.remaining);

    out_t.data_len = sizeof(sys_info_t);
    out_t.data = &dfs_sys_info;
    //
    char sBuf[BUF_SZ] = "";
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    int ws = write(thread->ns_info.sockfd, sBuf, sLen);
    if (ws != sLen) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "write err, ws: %d, sLen: %d", ws, sLen);

        return NGX_ERROR;
    }

    int pLen = 0;
    int rLen = recv(thread->ns_info.sockfd, &pLen, sizeof(int), MSG_PEEK);
    if (rLen < 0) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "read err, rLen: %d", rLen);

        return NGX_ERROR;
    }

    char *pNext = (char *) malloc(pLen);
    if (nullptr == pNext) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "malloc err, pLen: %d", pLen);

        return NGX_ERROR;
    }

    rLen = read(thread->ns_info.sockfd, pNext, pLen);
    if (rLen < 0) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "read err, rLen: %d", rLen);

        free(pNext);
        pNext = nullptr;

        return NGX_ERROR;
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(pNext, rLen, &in_t);

    if (in_t.ret != NGX_OK) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, 0,
                      "send_heartbeat err, ret: %d", in_t.ret);

        free(pNext);
        pNext = nullptr;

        return NGX_ERROR;
    } else if (in_t.cmd == DN_DEL_BLK && nullptr != in_t.data && in_t.data_len > 0) {
        delete_blks((char *) in_t.data, in_t.data_len);
    }
    if(printcount==printfrequency){
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_INFO, 0,
                      "send_heartbeat to %s ok, ret: %d", thread->ns_info.ip, in_t.ret);
        printcount=0;
    }else{
        printcount++;
    }
//    dfs_log_error(dfs_cycle->error_log, DFS_LOG_INFO, 0,
//                      "send_heartbeat to %s ok, ret: %d", thread->ns_info.ip, in_t.ret);

    free(pNext);
    pNext = nullptr;

    return NGX_OK;
}

// 从 cli 接收完成之后上报
static int receivedblock_report(dfs_thread_t *thread) {
    queue_t *cur = nullptr;
    block_info_t *blk = nullptr;
    report_blk_info_t rbi;

    pthread_mutex_lock(&g_recv_blk_report.lock);

    cur = queue_head(&g_recv_blk_report.que);
    if (!cur) {
        pthread_mutex_unlock(&g_recv_blk_report.lock);
        return NGX_OK;
    }
    queue_remove(cur);
    blk = queue_data(cur, block_info_t, me);
    g_recv_blk_report.num--;

    pthread_mutex_unlock(&g_recv_blk_report.lock);

    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = DN_RECV_BLK_REPORT; // 上报
    strcpy(out_t.key, dfs_cycle->listening_ip); //

    memset(&rbi, 0x00, sizeof(report_blk_info_t));
    rbi.blk_id = blk->id;
    rbi.blk_sz = blk->size;
    strcpy(rbi.dn_ip, dfs_cycle->listening_ip);

    out_t.data_len = sizeof(report_blk_info_t);
    out_t.data = &rbi;

    char sBuf[BUF_SZ] = "";
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    int ws = write(thread->ns_info.sockfd, sBuf, sLen);
    if (ws != sLen) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "receivedblock_report write err, ws: %d, sLen: %d", ws, sLen);

        return NGX_ERROR;
    }

    char rBuf[BUF_SZ] = "";
    int rLen = read(thread->ns_info.sockfd, rBuf, sizeof(rBuf));
    if (rLen < 0) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "read err, rLen: %d", rLen);

        return NGX_ERROR;
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(rBuf, rLen, &in_t);

    if (in_t.ret != NGX_OK) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, 0,
                      "receivedblock_report err, ret: %d", in_t.ret);

        return NGX_ERROR;
    }

    dfs_log_error(dfs_cycle->error_log, DFS_LOG_INFO, 0,
                  "dn received block_report to nn:%s ok, ret: %d", thread->ns_info.ip, in_t.ret);

    return NGX_OK;
}

static int wait_to_work(int second) {
    struct timespec timer{};
    struct timeval now{};
    gettimeofday(&now, nullptr);
    timer.tv_sec = now.tv_sec + second;
    timer.tv_nsec = now.tv_usec * 1000;
//    clock_gettime(CLOCK_MONOTONIC, &timer);
//    timer.tv_sec += second;
    pthread_mutex_lock(&g_recv_blk_report.lock);

    while (g_recv_blk_report.num == 0) {

        int rs = pthread_cond_timedwait(&g_recv_blk_report.cond,
                                        &g_recv_blk_report.lock, &timer);
//        int rs = pthread_cond_timedwait(&(g_recv_blk_report.cond), &(g_recv_blk_report.lock), &timer);
        if (rs == ETIMEDOUT)  //110
        {
            break;
        }
    }

    pthread_mutex_unlock(&g_recv_blk_report.lock);

    return NGX_OK;
}

// 初始化 blk report queue
int blk_report_queue_init() {
    queue_init(&g_recv_blk_report.que);
    g_recv_blk_report.num = 0;

    pthread_mutex_init(&g_recv_blk_report.lock, nullptr);
    pthread_cond_init(&g_recv_blk_report.cond, nullptr);

    queue_init(&g_blk_report.que);
    g_blk_report.num = 0;

    pthread_mutex_init(&g_blk_report.lock, nullptr);
    pthread_cond_init(&g_blk_report.cond, nullptr);

//    int ret = pthread_condattr_init(&(g_recv_blk_report.cattr));
//    if (ret != 0)
//    {
//        dfs_log_error(dfs_cycle->error_log,DFS_LOG_ERROR,errno,"pthread_condattr_init error");
//        return NGX_ERROR;
//    }
//    pthread_condattr_setclock(&(g_recv_blk_report.cattr), CLOCK_MONOTONIC);
//    pthread_cond_init(&(g_recv_blk_report.cond), &(g_recv_blk_report.cattr));
    return NGX_OK;
}

int blk_report_queue_release() {
    pthread_mutex_destroy(&g_recv_blk_report.lock);
    pthread_cond_destroy(&g_recv_blk_report.cond);
    g_recv_blk_report.num = 0;

    pthread_mutex_destroy(&g_blk_report.lock);
    pthread_cond_destroy(&g_blk_report.cond);
    g_blk_report.num = 0;

    return NGX_OK;
}

// 提示name node 收到 blk
int notify_nn_receivedblock(block_info_t *blk) {
    pthread_mutex_lock(&g_recv_blk_report.lock);

    queue_insert_tail(&g_recv_blk_report.que, &blk->me);
    g_recv_blk_report.num++;

    pthread_cond_signal(&g_recv_blk_report.cond);

    pthread_mutex_unlock(&g_recv_blk_report.lock);

    return NGX_OK;
}

static int block_report(int sockfd) {
    queue_t *cur = nullptr;
    block_info_t *blk = nullptr;
    report_blk_info_t rbi;

    pthread_mutex_lock(&g_blk_report.lock);

    cur = queue_head(&g_blk_report.que);
    if (!cur) {
        pthread_mutex_unlock(&g_recv_blk_report.lock);
        return NGX_OK;
    }
    queue_remove(cur);
    blk = queue_data(cur, block_info_t, me);
    g_blk_report.num--;

    pthread_mutex_unlock(&g_blk_report.lock);

    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = DN_BLK_REPORT;
    strcpy(out_t.key, dfs_cycle->listening_ip);

    memset(&rbi, 0x00, sizeof(report_blk_info_t));
    rbi.blk_id = blk->id;
    rbi.blk_sz = blk->size;
    strcpy(rbi.dn_ip, dfs_cycle->listening_ip);

    out_t.data_len = sizeof(report_blk_info_t);
    out_t.data = &rbi;

    char sBuf[BUF_SZ] = "";
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    int ws = write(sockfd, sBuf, sLen);
    if (ws != sLen) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "write err, ws: %d, sLen: %d", ws, sLen);

        return NGX_ERROR;
    }

    char rBuf[BUF_SZ] = "";
    int rLen = read(sockfd, rBuf, sizeof(rBuf));
    if (rLen < 0) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "read err, rLen: %d", rLen);

        return NGX_ERROR;
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(rBuf, rLen, &in_t);

    if (in_t.ret != NGX_OK) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, 0,
                      "block_report err, ret: %d", in_t.ret);

        return NGX_ERROR;
    }

    dfs_log_error(dfs_cycle->error_log, DFS_LOG_INFO, 0,
                  "block_report ok, ret: %d", in_t.ret);

    return NGX_OK;
}

// blk info插入 g_blk_report
int notify_blk_report(block_info_t *blk) {
    pthread_mutex_lock(&g_blk_report.lock);

    queue_insert_tail(&g_blk_report.que, &blk->me);
    g_blk_report.num++;

    pthread_mutex_unlock(&g_blk_report.lock);

    return NGX_OK;
}

int send_local_task(dfs_thread_t *thread) {
    if (thread != nullptr && thread->running) {
        queue_t *cur = nullptr;
        task_t *t = nullptr;
        task_queue_node_t *tnode = nullptr;
        queue_t qhead;
        queue_init(&qhead);
        pop_all(&thread->tq, &qhead);
        cur = queue_head(&qhead);

        while (!queue_empty(&qhead)) {
            tnode = queue_data(cur, task_queue_node_t, qe);
            t = &tnode->tk;

            queue_remove(cur);

            do_send_local_task(thread, t);
            // free tnode
            dn_free_task_node(tnode);

            cur = queue_head(&qhead);
        }
    }

    return 0;
}

int do_send_local_task(dfs_thread_t *thread, task_t *task) {
    int sockfd = thread->ns_info.sockfd;

    char sBuf[BUF_SZ] = "";
    int sLen = task_encode2str(task, sBuf, sizeof(sBuf));
    int ws = write(sockfd, sBuf, sLen);
    if (ws != sLen) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "write err, ws: %d, sLen: %d", ws, sLen);

        return NGX_ERROR;
    }

    char rBuf[BUF_SZ] = "";
    int rLen = read(sockfd, rBuf, sizeof(rBuf));
    if (rLen < 0) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "read err, rLen: %d", rLen);

        return NGX_ERROR;
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(rBuf, rLen, &in_t);

    if (in_t.ret != NGX_OK) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, 0,
                      "block_report err, ret: %d", in_t.ret);

        return NGX_ERROR;
    }
    // do task

    return 0;
}

int dn_kill_namenode() {
    int nn_id = process_get_nnpid();
    int pr = 0;
    if(nn_id > 0){ // kill namenode first
        kill(nn_id, SIGNAL_QUIT);
        do {
            pr = waitpid(nn_id, nullptr, WNOHANG);

        } while (pr == 0);
        if(pr == -1){
            if(errno == 10){ //10: No child processes
                int res = kill(process_get_pid(dfs_cycle), SIGNAL_RECONF);
                dfs_log_error(dfs_cycle->error_log,DFS_LOG_INFO,errno,"%s send SIGNAL_RECONF %d\n",__func__,res);
            }
        }

        if (pr == nn_id){
            int res = kill(process_get_pid(dfs_cycle), SIGNAL_RECONF);
            dfs_log_error(dfs_cycle->error_log,DFS_LOG_INFO,errno,"%s send SIGNAL_RECONF %d\n",__func__,res);
        }
    }
    return 0;
}

static int delete_blks(char *p, int len) {
    uint64_t blk_id = 0;
    int pLen = sizeof(uint64_t);

    while (len > 0) {
        memcpy(&blk_id, p, pLen);

        block_object_del(blk_id);

        p += pLen;
        len -= pLen;
    }

    return NGX_OK;
}

