#include <dfs_utils.h>
#include <queue>
#include "dn_worker_process.h"
#include "dfs_conf.h"
#include "dfs_channel.h"
#include "dfs_notice.h"
#include "dfs_conn.h"
#include "dfs_conn_listen.h"
#include "dn_module.h"
#include "dn_thread.h"
#include "dn_time.h"
#include "dn_conf.h"
#include "dn_process.h"
#include "dn_ns_service.h"
#include "dn_data_storage.h"
#include "dn_task_queue.h"
#include "dn_net_response_handler.h"
#include "dn_task_handler.h"
#include "dn_paxos.h"
#include "dn_ip_scanner.h"
#include "dfs_dbg.h"
#include "dn_group.h"
#include "../../etc/config.h"
#include "dn_signal.h"

#define PATH_LEN  256

#define WORKER_TITLE "datanode: worker process"

extern uint32_t process_type;
extern uint32_t blk_scanner_running;
extern std::string IPRANGE;
extern int IPNUM;
extern char localIp[INET_ADDRSTRLEN];


static int total_threads = 0;
static pthread_mutex_t init_lock;
static pthread_cond_t init_cond;
static int cur_runing_threads = 0;
static int cur_exited_threads = 0;

dfs_thread_t *woker_threads;
dfs_thread_t *last_task;
int woker_num = 0;
pid_t namenode_pid = 0;

dfs_thread_t *dispatch_last_task; // 用于 dispatch task
int task_num = 0;
int dispatch_task_index = 0;
dfs_thread_t *task_threads;
dfs_thread_t *openport_thread;
dfs_thread_t *paxos_thread;
bool start_ns_thread_flag = false;
//end

dfs_thread_t *ns_service_threads; // all the ns threads
dfs_thread_t *local_ns_service_thread = nullptr;
bool start_local_ns_server;
std::queue<DfsNode> newleaders;
std::vector<dfs_thread_t *> new_ns_service_threads;


int ns_service_num = 0;

extern dfs_thread_t *main_thread;

extern faio_manager_t *faio_mgr;

static inline int hash_task_key(char *str, int len);

static void thread_registration_init();

static void threads_total_add(int n);

static int thread_setup(dfs_thread_t *thread, int type);

static void *thread_worker_cycle(void *arg);

static void thread_worker_exit(dfs_thread_t *thread);

static void wait_for_thread_exit();

static void wait_for_thread_registration();

static int process_worker_exit(cycle_t *cycle);

static int create_worker_thread(cycle_t *cycle);

static void stop_worker_thread();

static void stop_task_thread();

static int channel_add_event(int fd, int event,
                             event_handler_pt handler, void *data);

static void channel_handler(event_t *ev);

static int create_ns_service_thread(cycle_t *cycle);

static int get_ns_srv_names(uchar_t *path, uchar_t names[][64]);

static void *thread_ns_service_cycle(void *args);


static int ns_thread_setup(dfs_thread_t *localthread);

static void stop_ns_service_thread();

static void dio_event_handler(event_t *ev);

static int create_data_blk_scanner(cycle_t *cycle);

//todo
static int create_task_thread(cycle_t *cycle);

static void *thread_task_cycle(void *arg);

static void thread_task_exit(dfs_thread_t *thread);

static int create_openport_thread(cycle_t *cycle);

static void *thread_openport_cycle(void *args);

static int dn_ipscan();

static int create_paxos_thread(cycle_t *cycle);

static void *thread_paxos_cycle(void *arg);

static void stop_paxos_thread();

static void stop_openport_thread();

static void check_exited_ns_thread_recycle();

static void main_thread_wait_group_init();

//end
static int thread_setup(dfs_thread_t *thread, int type) {
    conf_server_t *sconf = nullptr;

    sconf = (conf_server_t *) dfs_cycle->sconf;
    thread->event_base.nevents = sconf->connection_n;
    thread->type = type;

    if (thread_event_init(thread) != NGX_OK) {
        return NGX_ERROR;
    }

    if (THREAD_TASK == thread->type)
    {
        return NGX_OK;
    }

    thread->event_base.time_update = time_update;
    // 初始化线程连接池
    if (conn_pool_init(&thread->conn_pool, sconf->connection_n) != NGX_OK) {
        return NGX_ERROR;
    }

    return NGX_OK;
}

static void thread_worker_exit(dfs_thread_t *thread) {
    thread->state = THREAD_ST_EXIT;
    dfs_module_wokerthread_release(thread);
}

static int process_worker_exit(cycle_t *cycle) {
    dfs_module_woker_release(cycle);

    exit(PROCESS_KILL_EXIT);
}

static void thread_registration_init() {
    pthread_mutex_init(&init_lock, nullptr);
    pthread_cond_init(&init_cond, nullptr);
}

static void wait_for_thread_registration() {
    pthread_mutex_lock(&init_lock);

    while (cur_runing_threads < total_threads) {
        pthread_cond_wait(&init_cond, &init_lock);
    }

    pthread_mutex_unlock(&init_lock);
}

static void wait_for_thread_exit() {
    pthread_mutex_lock(&init_lock);

    while (cur_exited_threads < total_threads) {
        pthread_cond_wait(&init_cond, &init_lock);
    }

    pthread_mutex_unlock(&init_lock);
}

void register_thread_initialized(void) {
    sched_yield();

    pthread_mutex_lock(&init_lock);
    cur_runing_threads++;
    pthread_cond_signal(&init_cond);
    pthread_mutex_unlock(&init_lock);
}

void register_thread_exit(void) {
    pthread_mutex_lock(&init_lock);
    cur_exited_threads++;
    pthread_cond_signal(&init_cond);
    pthread_mutex_unlock(&init_lock);
}


// process_start_workers 中进来的入口函数
// 初始化 worker
void worker_processer(cycle_t *cycle, void *data) {
    int ret = 0;
    string_t title;
    sigset_t set;
    struct rlimit rl{};
    process_t *process = nullptr;

    ret = getrlimit(RLIMIT_NOFILE, &rl);
    if (ret == NGX_ERROR) {
        exit(PROCESS_FATAL_EXIT);
    }

    start_local_ns_server = false;

    process_type = PROCESS_WORKER;
    main_thread->event_base.nevents = 512;
    // 初始化 epoll 和 timer
    if (thread_event_init(main_thread) != NGX_OK) {
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno,
                      "thread_event_init() failed");

        exit(PROCESS_FATAL_EXIT);
    }

    // start worker in dn_data_storage.c //dn_data_storage_worker_init
    // faio thread process queue task
    // init cache management
    // init report queue
    if (dfs_module_woker_init(cycle) != NGX_OK) {
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno,
                      "dfs_module_woker_init() failed");

        exit(PROCESS_FATAL_EXIT);
    }

    sigemptyset(&set);//信号集置空
    if (sigprocmask(SIG_SETMASK, &set, nullptr) == -1) // 就是不阻塞信号
    {
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno,
                      "sigprocmask() failed");
    }

    // close this process's write fd
    process_close_other_channel();

    title.data = (uchar_t *) WORKER_TITLE;
    title.len = sizeof(WORKER_TITLE) - 1;
    process_set_title(&title);

    // 初始化线程锁
    thread_registration_init();

    // THREAD_TASK
    // 创建 task_num/worker_n 个 task_thread
    // 处理 task 并把task 分发到不同的tq
    if (create_task_thread(cycle) != NGX_OK) {
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno,
                      "create task thread failed");

        exit(PROCESS_FATAL_EXIT);
    }


    // THREAD_OPEN_PORT
    if (create_openport_thread(cycle) != NGX_OK) {
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno,
                      "create cli thread failed");

        exit(PROCESS_FATAL_EXIT);
    }

    // now start ip scan
    if (!IPRANGE.empty()) {
        dn_ipscan();
        dn_group->setGlobalStatus(PAXOS_BUILD);
    }
    // THREAD_PAXOS
    dn_paxos_worker_init(cycle);
    // do_paxos_task
    if (create_paxos_thread(cycle) != NGX_OK) {
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno,
                      "create paxos thread failed");

        exit(PROCESS_FATAL_EXIT);
    }

    main_thread_wait_group_init();

    dn_group->setGlobalStatus(THREAD_BUILD);

    // start namenode here
    dbg(dn_group->isLeader());
    if (dn_group->isLeader()) {
        namenode_pid = start_ns_server(dfs_cycle->leaders_paxos_ipport_string);
    }
    //end

    // name node server register
    // 获取 namespaceid 和 监控 report 上报
    // 发送心跳
    //
    if (create_ns_service_thread(cycle) != NGX_OK) {
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno,
                      "create ns service thread failed");

        exit(PROCESS_FATAL_EXIT);
    }
    // 扫描 blk ，更新 hashtable 和 全局 reporter
    if (create_data_blk_scanner(cycle) != NGX_OK) {
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno,
                      "create_data_blk_scanner failed");

        exit(PROCESS_FATAL_EXIT);
    }



    //创建worker线程
    //处理posted events？
    if (create_worker_thread(cycle) != NGX_OK) {
        dfs_log_error(cycle->error_log, DFS_LOG_ALERT, errno,
                      "create worker thread failed");

        exit(PROCESS_FATAL_EXIT);
    }
//    sleep(1000);
    process = get_process(process_get_curslot());

    // channel 用于父子进程通信
    /* 根据全局变量ngx_channel开启一个通道,只用于处理读事件(ngx_channel_handler) */
    /*channel[0] 是用来发送信息的，channel[1]是用来接收信息的。那么对自己而言，它需要向其他进程发送信息，
     * 需要保留其它进程的channel[0], 关闭channel[1]; 对自己而言，则需要关闭channel[0]。
     * 最后把ngx_channel放到epoll中，从第一部分中的介绍我们可以知道，这个ngx_channel实际就是自己的 channel[1]。
     * 这样有信息进来的时候就可以通知到了。*/
    // 子进程读取通道消息
    if (channel_add_event(process->channel[1],
                          EVENT_READ_EVENT, channel_handler, nullptr) != NGX_OK) {
        exit(PROCESS_FATAL_EXIT);
    }

    //
    dn_group->setGlobalStatus(GROUP_FINISH);
    //
    int cnt_ = 0;
    for (;;) {

//        PhxElection *election = dn_get_paxos_obj();
//        dbg(election->GetMaster().GetIP());
//        election->showmembership();
//        dn_group->printGroup();

        if (process_quit_check()) {
            stop_worker_thread();
            stop_task_thread();
            stop_openport_thread();
            stop_paxos_thread();
            stop_ns_service_thread();
            blk_scanner_running = NGX_FALSE;

            break;
        } else {
            // if need start namenode
//            sleep(1);
//            dbg(cnt_/1000);
//            dbg(start_local_ns_server);
//            dbg(namenode_pid);


            if (start_local_ns_server && namenode_pid == 0) {
                dbg(namenode_pid);
                dbg(dfs_cycle->leaders_paxos_ipport_string);
                namenode_pid = start_ns_server(dfs_cycle->leaders_paxos_ipport_string);
            }

            if (namenode_pid > 0) {
                // check namenode running
                if (kill(namenode_pid, 0) < 0 && process_quit_check()) {
                    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno, "namenode exist!");
                    process_quit();
                    break;
                }
            }

            // start new ns thread
            while (!newleaders.empty()) {
//                dfs_log_error(dfs_cycle->error_log, DFS_LOG_INFO, errno, "newleaders.size() is %d",newleaders.size());
                DfsNode newleader = newleaders.front();
//                dbg(newleader.getNodeIp());
                auto *newthread = (dfs_thread_t *) malloc(sizeof(dfs_thread_t));
                if (!newthread) {
                    dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "create_local_ns_service_thread pool_calloc err");
                    break;
                }
                new_ns_service_threads.push_back(newthread);
                create_new_ns_service_thread(dfs_cycle, newthread,
                                             newleader.getNodeIp().c_str(), dfs_cycle->listening_nssrv_port);
//                dbg("create sucess");
                newleaders.pop();
            }
        }

//            dbg("check_exited_ns_thread_recycle before");

        // recycle exited ns thread
        check_exited_ns_thread_recycle();

//            dbg("check_exited_ns_thread_recycle");

        thread_event_process(main_thread);

//        dbg("thread_event_process");


    }

    wait_for_thread_exit();

    process_worker_exit(cycle);
    dbg("main thread exit");
}

// start namenode here
int start_ns_server(const std::string &ns_ipport_string) {
    pid_t pid;
    pid = fork();
    int ret;
    std::string execDir = getExecDir();

    dbg(pid);
    switch (pid) {
        case -1: // error occur
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, 0, "start_ns_server fork failed");
            exit(1);
        case 0: // child
            char file_path_getcwd[256];
            getcwd(file_path_getcwd, 256);
//            dbg(file_path_getcwd);
            namenode_pid = getpid();
            dbg((execDir + NAMESERVER).c_str());
            ret = execl((execDir + NAMESERVER).c_str(), NAMESERVER, "-l", ns_ipport_string.c_str(), nullptr);
            if (ret == -1) {
                fprintf(stderr, "start_ns_server error : %s\n", strerror(errno));
                exit(errno);
            }
            break;
        default: // parent
            break;
    }
    return pid;
}

// 开启worker 线程

int create_worker_thread(cycle_t *cycle) {
    conf_server_t *sconf = nullptr;
    int i = 0;

    sconf = (conf_server_t *) cycle->sconf;
    woker_num = sconf->worker_n;

    woker_threads = (dfs_thread_t *) pool_calloc(cycle->pool,
                                                 woker_num * sizeof(dfs_thread_t));
    if (!woker_threads) {
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "pool_calloc err");

        return NGX_ERROR;
    }

    for (i = 0; i < woker_num; i++) {
        // 初始化epoll connection pool
        if (thread_setup(&woker_threads[i], THREAD_WORKER) == NGX_ERROR) {
            dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0,
                          "thread_setup err");

            return NGX_ERROR;
        }
    }

    for (i = 0; i < woker_num; i++) {
        woker_threads[i].run_func = thread_worker_cycle; //
        woker_threads[i].running = NGX_TRUE;
        woker_threads[i].state = THREAD_ST_UNSTART;

        if (thread_create(&woker_threads[i]) != NGX_OK) {
            dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0,
                          "thread_create err");

            return NGX_ERROR;
        }

        threads_total_add(1);
    }

    wait_for_thread_registration();

    for (i = 0; i < woker_num; i++) {
        if (woker_threads[i].state != THREAD_ST_OK) {
            dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0,
                          "create_worker thread[%d] err", i);

            return NGX_ERROR;
        }
    }

    return NGX_OK;
}

// worker 线程的 处理函数
// dn_data_storage_thread_init
static void *thread_worker_cycle(void *arg) {
    auto *me = (dfs_thread_t *) arg;

    thread_bind_key(me);

    time_init();

    // dn_data_storage_thread_init
    // worker thread
    // init faio \ fio
    // init notifier eventfd
    // 初始化 io events 队列 posted events, posted bad events
    if (dfs_module_workethread_init(me) != NGX_OK) {
        goto exit;
    }

    me->state = THREAD_ST_OK;

    register_thread_initialized();

    if (faio_mgr) {
        // 添加 读事件
        // 监听的 fd me->faio_notify.nfd
        // eventfd 进程间通信
        // handle 处理 fio回调
        if (channel_add_event(me->faio_notify.nfd, EVENT_READ_EVENT,
                              dio_event_handler, (void *) me) == NGX_ERROR) {
            goto exit;
        }
    }

    while (me->running) {
//
//	    if (process_doing & PROCESS_DOING_QUIT
//            || process_doing & PROCESS_DOING_TERMINATE)
//        {
//            if (thread_exit_check(me))
//			{
//                break;
//            }
//        }


        thread_event_process(me);
    }

    exit:
    dbg("thread_worker_cycle exit");
    thread_clean(me);
    register_thread_exit();
    thread_worker_exit(me);

    return nullptr;
}

// thread->faio_notify.nfd 被唤醒
// epoll event handler in worker cycle
// notifier handler
// eventfd
static void dio_event_handler(event_t *ev) {
    auto *thread = (dfs_thread_t *) ((conn_t *) (ev->data))->conn_data;

    // 读取 eventfd
    // 设置 noticed FAIO_FALSE ??
    cfs_recv_event(&thread->faio_notify);
    cfs_ioevents_process_posted(&thread->io_events, &thread->fio_mgr);
}

// 根据 eventfd 初始化 connection
// epoll event 添加 读写事件
static int channel_add_event(int fd, int event,
                             event_handler_pt handler, void *data) {
    event_t *ev = nullptr;
    event_t *rev = nullptr;
    event_t *wev = nullptr;
    conn_t *c = nullptr;
    event_base_t *base = nullptr;

    // 根据 eventfd 初始化 connection
    c = conn_get_from_mem(fd);

    if (!c) {
        return NGX_ERROR;
    }
    // worker thread  event base
    base = thread_get_event_base();

    c->pool = nullptr;
    c->conn_data = data; // data 是 thread 本身

    rev = c->read;
    wev = c->write;

    ev = (event == EVENT_READ_EVENT) ? rev : wev;
    ev->handler = handler;

    // epoll add event
    // ev->data(conn)->fd
    if (epoll_add_event(base, ev, event, 0) == NGX_ERROR) {
        return NGX_ERROR;
    }

    return NGX_OK;
}

//ngx_channel_handler
static void channel_handler(event_t *ev) {
    int n = 0;
    conn_t *c = nullptr;
    channel_t ch;
    event_base_t *ev_base = nullptr;
    process_t *process = nullptr;

    if (ev->timedout) {
        ev->timedout = 0;

        return;
    }

    c = (conn_t *) ev->data;

    ev_base = thread_get_event_base();

    dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0, "channel handler");

    for (;;) {
        n = channel_read(c->fd, &ch, sizeof(channel_t), dfs_cycle->error_log);

        dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0, "channel: %i", n);

        if (n == NGX_ERROR) {
            if (ev_base->event_flags & EVENT_USE_EPOLL_EVENT) {
                event_del_conn(ev_base, c, 0);
            }

            conn_close(c);
            conn_free_mem(c);

            return;
        }

        if (ev_base->event_flags & EVENT_USE_EVENTPORT_EVENT) {
            if (epoll_add_event(ev_base, ev, EVENT_READ_EVENT, 0) == NGX_ERROR) {
                return;
            }
        }

        if (n == NGX_AGAIN) {
            return;
        }

        dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                      "channel command: %d", ch.command);

        switch (ch.command) {
            case CHANNEL_CMD_QUIT:
                //process_doing |= PROCESS_DOING_QUIT;
                process_set_doing(PROCESS_DOING_QUIT);
                break;

            case CHANNEL_CMD_TERMINATE:
                process_set_doing(PROCESS_DOING_TERMINATE);
                break;

            case CHANNEL_CMD_OPEN:
                dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                              "get channel s:%i pid:%P fd:%d",
                              ch.slot, ch.pid, ch.fd);
                /* 收到其他进程的pid 和fd 信息 ，进程通信
                 * 就是在对应的位置上复制pid和fd,下次向往哪个进程发信息的时候，直接发到 ngx_process[目标进程].channel[0]*/
                process = get_process(ch.slot);
                process->pid = ch.pid;
                process->channel[0] = ch.fd;
                break;

            case CHANNEL_CMD_CLOSE:
                process = get_process(ch.slot);

                dfs_log_debug(dfs_cycle->error_log, DFS_LOG_DEBUG, 0,
                              "close channel s:%i pid:%P our:%P fd:%d",
                              ch.slot, ch.pid, process->pid,
                              process->channel[0]);

                if (close(process->channel[0]) == NGX_ERROR) {
                    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno,
                                  "close() channel failed");
                }

                process->channel[0] = NGX_INVALID_FILE;
                process->pid = NGX_INVALID_PID;
                break;

            case CHANNEL_CMD_BACKUP:
                //findex_db_backup();
                break;
        }
    }
}

static void stop_worker_thread() {
    for (int i = 0; i < woker_num; i++) {
        woker_threads[i].running = NGX_FALSE;
    }
}

static void stop_task_thread() {
    for (int i = 0; i < task_num; i++) {
        task_threads[i].running = NGX_FALSE;
    }
}

static void stop_openport_thread() {
    openport_thread->running = NGX_FALSE;
}

static void stop_paxos_thread() {
    paxos_thread->running = NGX_FALSE;
}

static void threads_total_add(int n) {
    total_threads += n;
}

static inline int hash_task_key(char *str, int len) {
    (void) len;
    if(dispatch_task_index  == task_num){
        dispatch_task_index = 0;
    }
    return dispatch_task_index++;
}

// namenode 线程
static int create_ns_service_thread(cycle_t *cycle) {
    auto *sconf = (conf_server_t *) cycle->sconf;

    int i = 0;
    uchar_t names[16][64]; // 127.0.0.1:8001

    ns_service_num = get_ns_srv_names(sconf->ns_srv.data, names);

    ns_service_threads = (dfs_thread_t *) pool_calloc(cycle->pool,
                                                      ns_service_num * sizeof(dfs_thread_t));
    if (!ns_service_threads) {
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "pool_calloc err");

        return NGX_ERROR;
    }

    for (i = 0; i < ns_service_num; i++) {
        int count = sscanf((const char *) names[i], "%[^':']:%d",
                           ns_service_threads[i].ns_info.ip,
                           &ns_service_threads[i].ns_info.port);
        if (count != 2) {
            return NGX_ERROR;
        }

        // all the port are same
//        dfs_cycle->listening_nssrv_port = ns_service_threads[i].ns_info.port;
        if (strcmp(localIp, ns_service_threads[i].ns_info.ip) == 0) {
            local_ns_service_thread = &ns_service_threads[i];
            ns_thread_setup(local_ns_service_thread);

        }
        // namenode 的run func
        ns_service_threads[i].run_func = thread_ns_service_cycle;
        task_queue_init(&ns_service_threads[i].tq);

        ns_service_threads[i].running = NGX_TRUE;
        ns_service_threads[i].state = THREAD_ST_UNSTART;
        // 线程创建之后运行 thread_ns_service_cycle，参数是thread 本身
        if (thread_create(&ns_service_threads[i]) != NGX_OK) {
            dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0,
                          "thread_create err");

            return NGX_ERROR;
        }

        threads_total_add(1);
    }

    wait_for_thread_registration();

    for (i = 0; i < ns_service_num; i++) {
        if (ns_service_threads[i].state != THREAD_ST_OK) {
            dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0,
                          "create ns service thread[%d] err", i);

            return NGX_ERROR;
        }
    }

    return NGX_OK;
}

static int get_ns_srv_names(uchar_t *path, uchar_t names[][64]) {
    uchar_t *str = nullptr;
    char *saveptr = nullptr;
    uchar_t *token = nullptr;
    int i = 0;

    for (str = path;; str = nullptr, token = nullptr, i++) {
        token = (uchar_t *) strtok_r((char *) str, ",", &saveptr);
        if (token == nullptr) {
            break;
        }

        memset(names[i], 0x00, PATH_LEN);
        strcpy((char *) names[i], (const char *) token);
    }

    return i;
}

// name node server
// args is thread self
static void *thread_ns_service_cycle(void *args) {
    auto *me = (dfs_thread_t *) args;
    auto *sconf = (conf_server_t *) dfs_cycle->sconf;
    int heartbeat_timeout = sconf->heartbeat_timeout;

    thread_bind_key(me);

    me->state = THREAD_ST_OK;

    //
    register_thread_initialized();

    int tries = 0;
    while (me->running) {
        // 连接上 namenode 获取 namespaceid
        if (dn_register(me) != NGX_OK) {
            printf("can not connect to namenode : %s:%d ,sencode:%d \n", me->ns_info.ip, me->ns_info.port, tries);
            sleep(1);
            // can not conn to nn, after 60s exit
            tries++;
            if (tries == heartbeat_timeout || me->running == NGX_FALSE) {
                me->running = NGX_FALSE;
                break;
            }

            //
            continue;
        }
        tries = 0;
        // 检查 version namespace id ，创建子文件夹
        setup_ns_storage(me);
//        dbg("dn_register");
        // namenode 上报 receivedblock_report、 block_report
        offer_service(me, local_ns_service_thread);


    }

    register_thread_exit();
    me->state = THREAD_ST_EXIT;
    dbg("thread_ns_service_cycle exit");
    dbg(me->ns_info.ip);
    return nullptr;
}


int create_new_ns_service_thread(cycle_t *cycle, dfs_thread_t *lthread, const char *newip, int port) {
    if (lthread == nullptr) {
        return NGX_ERROR;
    }

    //
    if (strcmp(newip, localIp) == 0) {
        local_ns_service_thread = lthread;
        ns_thread_setup(local_ns_service_thread);

    }

    strcpy(lthread->ns_info.ip, newip);
    lthread->ns_info.port = port;
    // namenode 的run func
    lthread->run_func = thread_ns_service_cycle;
    task_queue_init(&lthread->tq);

    lthread->running = NGX_TRUE;
    lthread->state = THREAD_ST_UNSTART;
    // 线程创建之后运行 thread_ns_service_cycle，参数是thread 本身
    if (thread_create(lthread) != NGX_OK) {
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0,
                      "local_ns_service thread_create err");

        return NGX_ERROR;
    }

    threads_total_add(1);
    wait_for_thread_registration();

    if (lthread->state != THREAD_ST_OK) {
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0,
                      "create_local_ns_service_thread err");

        return NGX_ERROR;
    }

    return NGX_OK;
}

static int ns_thread_setup(dfs_thread_t *localthread) {
    if (thread_setup(localthread, THREAD_LOCAL_NS) == NGX_ERROR) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, 0,
                      "thread_setup err");

        return NGX_ERROR;
    }
    task_queue_init(&localthread->tq);
    return NGX_OK;
}

static void stop_ns_service_thread() {
    for (int i = 0; i < ns_service_num; i++) {
        ns_service_threads[i].running = NGX_FALSE;
    }
    if (local_ns_service_thread != nullptr) {
        local_ns_service_thread->running = NGX_FALSE;
    }
    for (auto &new_ns_server:new_ns_service_threads) {
        new_ns_server->running = NGX_FALSE;
    }
}

// 创建 scanner 线程
static int create_data_blk_scanner(cycle_t *cycle) {
    pthread_t pid;

    if (pthread_create(&pid, nullptr, &blk_scanner_start, nullptr) != NGX_OK) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno,
                      "create blk_scanner thread failed");

        return NGX_ERROR;
    }

    return NGX_OK;
}



// thread task cycle
// 初始化 task num/worker_n 个 task_thread
// 分发 task 到不同的处理线程
int create_task_thread(cycle_t *cycle) {
    conf_server_t *sconf = nullptr;
    int i = 0;

    sconf = (conf_server_t *) cycle->sconf;
    task_num = 1;//sconf->worker_n

    task_threads = (dfs_thread_t *) pool_calloc(cycle->pool,
                                                task_num * sizeof(dfs_thread_t));
    if (!task_threads) {
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "pool_calloc err");

        return NGX_ERROR;
    }

    for (i = 0; i < task_num; i++) {
        if (thread_setup(&task_threads[i], THREAD_TASK) == NGX_ERROR) {
            dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0,
                          "thread_setup err");

            return NGX_ERROR;
        }
    }

    dispatch_last_task = &task_threads[0];

    for (i = 0; i < task_num; i++) {
        task_threads[i].run_func = thread_task_cycle;
        task_threads[i].running = NGX_TRUE;
        task_queue_init(&task_threads[i].tq);
        task_threads[i].state = THREAD_ST_UNSTART;

        if (thread_create(&task_threads[i]) != NGX_OK) {
            dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0,
                          "thread_create err");

            return NGX_ERROR;
        }

        threads_total_add(1);
    }

    wait_for_thread_registration();

    for (i = 0; i < task_num; i++) {
        if (task_threads[i].state != THREAD_ST_OK) {
            dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0,
                          "create_worker thread[%d] err", i);

            return NGX_ERROR;
        }
    }

    return NGX_OK;
}

// do_task_handler
static void *thread_task_cycle(void *arg) {
    dfs_thread_t *me = (dfs_thread_t *) arg;

    thread_bind_key(me);


    me->state = THREAD_ST_OK;

    register_thread_initialized();

    // 把一些task 直接push到paxoas 的tq
    // call from ... listening_rev_handler ... dispatch_task_
    notice_init(&me->event_base, &me->tq_notice, dn_task_handler, &me->tq);

    while (me->running) {
         thread_event_process(me);
    }
    dbg("thread_event_process exit");
    exit:
    thread_clean(me);
    register_thread_exit();
    thread_task_exit(me);

    return nullptr;
}

static void thread_task_exit(dfs_thread_t *thread) {
    thread->state = THREAD_ST_EXIT;
    dfs_module_wokerthread_release(thread);
}


static void *thread_openport_cycle(void *args) {
    dfs_thread_t *me = (dfs_thread_t *) args;
    array_t *listens = nullptr;

    listens = cycle_get_listen_for_openport();
    thread_bind_key(me);

    notice_init(&me->event_base, &me->tq_notice, net_response_handler, me);

    if (conn_listening_add_event(&me->event_base, listens) != NGX_OK) {
        goto exit;
    }

    me->state = THREAD_ST_OK;
    register_thread_initialized();

    while (me->running) {
        thread_event_process(me);
    }

    exit:
    thread_clean(me);
    me->state = THREAD_ST_EXIT;
    register_thread_exit();

    return nullptr;
}

static int create_openport_thread(cycle_t *cycle) {
    int i = 0;

    openport_thread = (dfs_thread_t *) pool_calloc(cycle->pool, sizeof(dfs_thread_t));
    if (!openport_thread) {
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "pool_calloc err");

        return NGX_ERROR;
    }

    if (thread_setup(openport_thread, THREAD_OPEN_PORT) != NGX_OK) {
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "thread_setup err");

        return NGX_ERROR;
    }

    task_queue_init(&openport_thread->tq);

    openport_thread->queue_size = ((conf_server_t *) cycle->sconf)->worker_n;

    openport_thread->bque = (task_queue_t *) malloc(sizeof(task_queue_t) * openport_thread->queue_size);
    if (!openport_thread->bque) {
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "queue malloc fail");
    }

    for (i = 0; i < openport_thread->queue_size; i++) {
        task_queue_init(&openport_thread->bque[i]);
    }

    openport_thread->run_func = thread_openport_cycle;
    openport_thread->running = NGX_TRUE;
    openport_thread->state = THREAD_ST_UNSTART;

    if (thread_create(openport_thread) != NGX_OK) {
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0,
                      "thread_create error");

        return NGX_ERROR;
    }

    threads_total_add(1);
    wait_for_thread_registration();
    if (openport_thread->state != THREAD_ST_OK) {
        return NGX_ERROR;
    }

    return NGX_OK;
}


// dispatch task when recv it
// data is task_queue_node_t
// push task to dispatch_last_task->tq
// notice_wake_up (&dispatch_last_task->tq_notice
void dispatch_task_(void *data) {
    task_queue_node_t *node = nullptr;
    task_t *t = nullptr;
    uint32_t index = 0;

    node = (task_queue_node_t *) data;
    t = &node->tk;

    index = hash_task_key(t->key, 16); // index is key address
    if(t->cmd!=TASK_IPSCAN){
        dbg(t->cmd);
        dbg((char *)t->data);
        dbg(index);
    }
//    printf("task num:%d \n", task_num);
    // check task need pre deal


    dispatch_last_task = &task_threads[index % task_num]; // 初始化 dispatch_last_task = &task_threads[0];
    if(task_pre_check(t)){
        task_queue_node_t *replace_node= static_cast<task_queue_node_t *>(malloc(sizeof(task_queue_node_t)));
        if(task_pre_handle(t,replace_node)){
            push_task(&dispatch_last_task->tq, replace_node);
        }else{
            return;
        }
    }else{
        push_task(&dispatch_last_task->tq, (task_queue_node_t *) data); //
    }
    notice_wake_up(&dispatch_last_task->tq_notice);

}

bool task_pre_handle(task_t *task,task_queue_node_t *replace_node) {
    switch (task->cmd) {
        case NODE_REMOVE:{
            auto *node = queue_data(task, task_queue_node_t, tk);
            string rmip=(char *)task->data;
            string messfromleaderip=task->key;
            if(!master_newest_group->isContain(DfsNode(rmip,0))){
                task->data = nullptr;
                task->data_len = 0;
                task->ret = SUCC;
                int wb = write_back(node);
                if(wb==NGX_ERROR){
                    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, 0, "task_pre_handle-write_back error");
                }
                return false;
            }
            task_t rmtask;
            bzero(&rmtask, sizeof(task_t));
            rmtask=*task;
            rmtask.data=malloc(task->data_len);
            memcpy(rmtask.data,task->data,task->data_len);
            replace_node->tk=rmtask;
            task->data = nullptr;
            task->data_len = 0;
            task->ret = SUCC;
            int wb= write_back(node);
            if(wb==NGX_ERROR){
                dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, 0, "task_pre_handle-write_back error");
            }
            break;
        }
        default:
            break;
    }
    return true;
}

bool task_pre_check(task_t *t) {
    switch (t->cmd) {
        case NODE_REMOVE:// 在 dispatch 前特别处理
            return true;
        default:
            return false;
    }
    return false;
}

static int dn_ipscan() {
    start_scan ss;
    server_bind_t *listening_for_open;
    conf_server_t *sconf = nullptr;
    int listen_for_open_port = 0;
    sconf = static_cast<conf_server_t *>(dfs_cycle->sconf);
    listening_for_open = (server_bind_t *) sconf->listen_open_port.elts;
    listen_for_open_port = listening_for_open[0].port;
    if (old_dn_group->groups.size() != 0) {
        vector<string> oldips;
        set<string> oldleaders;
        for (auto group :old_dn_group->getGroups()) {
            oldleaders.insert(group.getGroupLeader().getNodeIp());
            for (const auto& node:group.getNodeList()) {
                oldips.push_back(node.getNodeIp());
            }
        }
        int oldleader_num = oldleaders.size();
        int oldgroup_scan_ret = ss.start_oldgroup_scan(oldips, oldleaders);
        int thread_hold = 0;
        if (oldleader_num % 2 == 0) {
            thread_hold = oldleader_num / 2;
        } else {
            thread_hold = oldleader_num / 2 + 1;
        }
        while (oldgroup_scan_ret < thread_hold) {
            if (oldgroup_scan_ret == NODE_HAS_JOINED) {
                // 节点已经加入了就用空的NodeList启动
                return NGX_OK;
            }
            oldgroup_scan_ret = ss.start_oldgroup_scan(oldips, oldleaders);
            sleep(2);
            // if only self , then break
            if (thread_hold == 1 && oldgroup_scan_ret == thread_hold) {
                break;
            }
        }
        return NGX_OK;
    } else {
        int scan_ret = ss.start(IPRANGE, to_string(listen_for_open_port), false);
        while (scan_ret != IPNUM) {
            if (scan_ret == NGX_ERROR) {
                dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, 0, "scan ip port error!");
                return NGX_ERROR;
            } else if (scan_ret == NODE_HAS_JOINED) {
                // 节点已经加入了就用空的NodeList启动
                return NGX_OK;
            }
            scan_ret = ss.start(IPRANGE, to_string(listen_for_open_port), false);
        }
        return NGX_OK;
    }

}


// paxos thread
int create_paxos_thread(cycle_t *cycle) {
    paxos_thread = (dfs_thread_t *) pool_calloc(cycle->pool,
                                                sizeof(dfs_thread_t));
    if (!paxos_thread) {
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "pool_calloc err");

        return NGX_ERROR;
    }

    // init task queue
    // set event_base and event_timer
    // init conn_pool
    if (thread_setup(paxos_thread, THREAD_PAXOS) == NGX_ERROR) {
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "thread_setup err");

        return NGX_ERROR;
    }

    paxos_thread->run_func = thread_paxos_cycle;
    paxos_thread->running = NGX_TRUE;
    task_queue_init(&paxos_thread->tq);
    paxos_thread->state = THREAD_ST_UNSTART;

    if (thread_create(paxos_thread) != NGX_OK) {
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "thread_create err");

        return NGX_ERROR;
    }

    threads_total_add(1);
    wait_for_thread_registration();

    if (paxos_thread->state != THREAD_ST_OK) {
        return NGX_ERROR;
    }

    if (dn_paxos_run() != NGX_OK) {
        dfs_log_error(cycle->error_log, DFS_LOG_FATAL, 0, "run paxos err");

        return NGX_ERROR;
    }

    return NGX_OK;
}

// paxos 线程
// ngx_module_workethread_init
static void *thread_paxos_cycle(void *arg) //paxos_thread
{
    dfs_thread_t *me = (dfs_thread_t *) arg;

    // set paxos_thread to thread private data
    thread_bind_key(me);

    // 这个初始化应该放在外面去, 线程函数的初始化
    // 目前都是nullptr 不用初始化
//    if (ngx_module_workethread_init(me) != NGX_OK)
//	{
//        goto exit;
//    }

    me->state = THREAD_ST_OK;

    register_thread_initialized();

    // 进程间通信
    // n -> tq_notice
    // data -> task queue
    // open channel and 分配 pfd
    // 根据 pipe fd 添加 event 事件
    // 被唤醒的时候执行 do_paxos_task_handler
    notice_init(&me->event_base, &me->tq_notice, do_paxos_task_handler, &me->tq);

    while (me->running) {
        thread_event_process(me); // nn thread
    }

    exit:
    thread_clean(me);
    register_thread_exit();
    thread_task_exit(me);

    return nullptr;
}

//int checkingroup_cnt=0;
static void check_exited_ns_thread_recycle() {
    // check new ns thread
    //dbg(__func__);
    auto iter = new_ns_service_threads.begin();
    while (iter != new_ns_service_threads.end()) {
        //dbg((*iter)->state);
        if ((*iter)->state == THREAD_ST_EXIT) {
            if((*iter)!= nullptr){
                free((*iter));
                iter=new_ns_service_threads.erase(iter);
            }else{
                iter++;
            }
        }else{
            iter++;
        }
    }
    //dbg("check all the ns thread");
    // check all the ns thread

    if(new_ns_service_threads.empty()){
        int dead_thread_num = 0;
        for(int i = 0;i<ns_service_num;i++){

            if(!ns_service_threads[i].running){
                dead_thread_num++;
            }
        }

        if(dead_thread_num == ns_service_num && dn_group->getGlobalStatus()!=GROUP_SHOTDOWN){
            int nn_id = process_get_nnpid();
            if(nn_id > 0){ // kill namenode first
                dn_kill_namenode();
                int res = kill(process_get_pid(dfs_cycle), SIGNAL_RECONF);
            } else{ // if not namenode ,then do reconf
                int res = kill(process_get_pid(dfs_cycle), SIGNAL_RECONF);
                dfs_log_error(dfs_cycle->error_log,DFS_LOG_INFO,errno,"%s send SIGNAL_RECONF %d\n",__func__,res);
            }
        }
    }
    // chen if in dn group
//    if(checkingroup_cnt==1000){
//        dbg(checkingroup_cnt);
//        if(!dn_group->isContain(DfsNode(dn_group->getOwn().getNodeIp(),0))){
//            int nn_id = process_get_nnpid();
//            int pr = 0;
//            if(nn_id > 0){ // kill namenode first
//                dn_kill_namenode();
//                int res = kill(process_get_pid(dfs_cycle), SIGNAL_RECONF);
//
//            } else{ // if not namenode ,then do reconf
//                int res = kill(process_get_pid(dfs_cycle), SIGNAL_RECONF);
//                dfs_log_error(dfs_cycle->error_log,DFS_LOG_INFO,errno,"%s send SIGNAL_RECONF %d\n",__func__,res);
//            }
//
//        }
//        checkingroup_cnt=0;
//    }
//    checkingroup_cnt++;
}

static void main_thread_wait_group_init() {
    // wait for group init
    printf("[] wait for group init \n");
    while ((dn_group->groups.empty() || !start_ns_thread_flag) && !process_quit_check() ) {
        usleep(500);
    }

    printf("[] group init finish \n");

}