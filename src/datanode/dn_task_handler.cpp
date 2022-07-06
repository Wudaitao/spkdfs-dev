
#include <stdlib.h>
#include <dfs_utils.h>
#include <PhxElection.h>
#include <dfs_dbg.h>

#include "dn_task_handler.h"
#include "dn_task_queue.h"
#include "dn_thread.h"
#include "dn_rpc_server.h"
#include "dn_net_response_handler.h"
#include "dn_group.h"
#include "dn_conf.h"
#include "dn_paxos.h"
#define BUF_SZ  4096
extern dfs_thread_t *local_ns_service_thread;

// dn_rpc_service_run
static void do_task(task_t *task) {
    assert(task);
    dn_rpc_service_run(task);
}

// do task
// 对应Thread_TASK 的task queue
// 分发 task 到不同线程
void dn_task_handler(void *q) // thread->tq
{
    task_queue_node_t *tnode = nullptr;
    task_t *t = nullptr;
    queue_t *cur = nullptr;
    queue_t qhead;
    task_queue_t *tq = nullptr;
    dfs_thread_t *thread = nullptr;

    tq = (task_queue_t *) q;
    thread = get_local_thread(); // task_threads[]

    queue_init(&qhead);
    pop_all(tq, &qhead);

    cur = queue_head(&qhead);

    while (!queue_empty(&qhead) && thread->running) {
        // |task_queue_node_t    |  qe |
        // |                     |     |
        // 所以 qe 减去偏移 得到 对应 task_queue_node_t
        tnode = queue_data(cur, task_queue_node_t, qe);
        t = &tnode->tk;

        queue_remove(cur);

        do_task(t);

        cur = queue_head(&qhead);
    }
}

int task_test(task_t *task) {
    dbg(__func__);
    auto *node = queue_data(task, task_queue_node_t, tk);
//    printf("ls key:%s\n", task->key);

    // just for example

    if (strlen(task->key) == 0) {
        task->ret = KEY_NOTEXIST;

        // if u not use malloc then make task->data  to null
        task->data = nullptr;
        task->data_len = 0;
        return write_back(node); // 这里是 cli thread ，直接交给cli thread 处理（直接发送回去）
    }
    // do some task deal here

    //
    task->ret = NGX_OK;
    task->data = nullptr;
    task->data_len = 0;
    return write_back(node);

}

// add scan node to nodelist
int handle_ipscan(task_t *task) {
    auto *node = queue_data(task, task_queue_node_t, tk);
    //
    char ot_paxos_string[25] = {0};
    if (task->data != nullptr) {
        memcpy(ot_paxos_string, task->data, task->data_len);
        char ot_paxos_ip[INET_ADDRSTRLEN];
        int ot_paxos_port;

        if (parseIpPort(ot_paxos_string, ot_paxos_ip, ot_paxos_port) == NGX_OK) {
            // ignore ot_paxos_port and just add port 0
            dn_group->lockGroup();
            // check group status, if group is finishing, node need to ask master

//            dbg(dn_group->getGlobalStatus() == GROUP_FINISH);
            if (task->cmd == TASK_IPSCAN && THREAD_BUILD < dn_group->getGlobalStatus()
            && dn_group->getGlobalStatus() < GROUP_FINISH) {
                dn_group->unlockGroup();
                task->data = nullptr;
                task->data_len = 0;
                task->ret = NODE_NEED_WAIT;
                return write_back(node);
            }

            if (task->cmd == TASK_IPSCAN && dn_group->getGlobalStatus() == GROUP_FINISH) {
                dn_group->unlockGroup();
                task->cmd = NODE_WANT_JOIN;
                return handle_node_want_join(task);
            }
            // node exist
            if (dn_group->checkAddrInNodeList(ot_paxos_ip, dfs_cycle->listening_paxos_port) == NGX_OK) {
                dn_group->unlockGroup();

                task->ret = SUCC;
                task->data = malloc(INET_ADDRSTRLEN);
                task->data_len = INET_ADDRSTRLEN;
                strcpy((char *) task->data, dn_group->getOwn().getNodeIp().c_str());
                return write_back(node);
            } else {
                // ignore ot_paxos_port and just add port 0
                // add node to list

                if (dn_group->addAddrToNodeList(ot_paxos_ip, dfs_cycle->listening_paxos_port) == NGX_OK) {
                    dn_group->unlockGroup();
                    task->ret = SUCC;
                    // write own addr to remote
                    task->data = malloc(INET_ADDRSTRLEN);
                    task->data_len = INET_ADDRSTRLEN;
                    strcpy((char *) task->data, dn_group->getOwn().getNodeIp().c_str());
                    return write_back(node);
                    // add failed
                } else {
                    printf("handle_ipscan add node fails");
                    dn_group->unlockGroup();
                    task->ret = NGX_ERROR;
                    task->data = nullptr;
                    task->data_len = 0;
                    return write_back(node);
                }
            }
        }
    }
//    dn_group->unlockGroup();
    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, 0, "handle_ipscan not find remote paxos ips");
    task->ret = NGX_ERROR;
    task->data = nullptr;
    task->data_len = 0;
    return write_back(node);
}

// trans task to paxos thread
int handle_node_want_join(task_t *task) {
    auto *node = queue_data(task, task_queue_node_t, tk);
    char node_iport_string[25] = {0};
    PhxElection *phxElection = dn_get_paxos_obj();

    if (task->data != nullptr) {
        strcpy(node_iport_string, (char *) task->data);
        dbg(node_iport_string);
        // check group status
        if (dn_group->getGlobalStatus() != GROUP_FINISH) {
            // node need to wait
            task->ret = NODE_NEED_WAIT;
            task->data = nullptr;
            task->data_len = 0;
            return write_back(node);
        } else {
            // group finish, means that paxos has run
            // check global group master alive
            dbg(phxElection->GetMaster().GetIP());
            dbg(phxElection->GetMaster().GetPort());
            if(phxElection->GetMaster().GetPort()==0){
                // now the group doesnt have a group master
                task->ret = NODE_NEED_WAIT;
                task->data = nullptr;
                task->data_len = 0;
                return write_back(node);
            }
            if (phxElection->IsIMMaster()) {
                // group has build finished and new node joined,
                // new node will start with blank NodeLists
                char newIp[INET_ADDRSTRLEN];
                int  newPort;
                // old group string
                string old_group_string_exclude_newnode; //
                phxgrouplist::GroupList gl = dn_group->encodeToProtobufMess();
                gl.SerializeToString(&old_group_string_exclude_newnode);
                // end

                // check if generate new group
                parseIpPort(node_iport_string,newIp,newPort);
                DfsNode tnode;
                tnode.setNodeIp(newIp);
                tnode.setNodePort(dfs_cycle->listening_paxos_port);
                //? is check contain?
//                master_newest_group->lockGroup();
//                if(master_newest_group->isContain(tnode)){}
                DfsNode leader = group_dispatch_add(tnode);

                dn_group->addAddrToNodeList(newIp,dfs_cycle->listening_paxos_port);
                // add leader to task->key
                memcpy(task->key,leader.getNodeIp().c_str(),leader.getNodeIp().length());
                // add former ip to task -> data
                bzero(task->data,task->data_len);
                memcpy(task->data,newIp,strlen(newIp));
                task->data_len = strlen(newIp);
                dbg(task->data_len);
                dbg((char *)task->data);
                dbg(newIp);


                // trans task to paxos_thread
                dn_trans_task(task,paxos_thread);
//
                task->ret = NODE_JOIN_FINISH;
                strcpy(task->key,leader.getNodeIp().c_str());


                // do remember ,malloc
                // return all the leaders
                task->data = malloc(old_group_string_exclude_newnode.length());
                memcpy(task->data,old_group_string_exclude_newnode.c_str(),old_group_string_exclude_newnode.length());

                //
                task->data_len = old_group_string_exclude_newnode.length();
                return write_back(node);

            } else{
                // master redierct ( return master ip)

                task->ret = REDIRECT_TO_MASTER;
                std::string ips = phxElection->GetMaster().GetIP();

                task->data = malloc(ips.length() + 1);
                memcpy(task->data,ips.c_str(),ips.length() + 1);
                task->data_len = (int)ips.length() + 1;
                return write_back(node);

            }
        }
    }

    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, 0, "handle_ipscan not find remote paxos ips");
    task->ret = NGX_ERROR;
    task->data = nullptr;
    task->data_len = 0;
    return write_back(node);
}

int rpc_handle_node_remove(task_t *task) {
    dbg(__func__);
    //解决方案
    //1.主动向其他leader询问
    //2.计数等待，设置定时器30s未达到要求重置，达到删除
    //3.记断开leader_ip等待，设置定时器30s后未达到要求重置，达到删除
    auto *node = queue_data(task, task_queue_node_t, tk);
    string rmip=(char *)task->data;
    string messfromleaderip=task->key;
    dbg(rmip);
    dbg(messfromleaderip);
    if(check_node_is_alive(messfromleaderip,rmip)){
        dn_free_task_node(node);
        return 0;
    }
    dn_trans_task(task, paxos_thread);
    dn_free_task_node(node);
    return 0;
}
bool check_node_is_alive(const string& messfromleaderip,string nodeip){
    // send task
    // send self ip and open port to remote
    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = TASK_CHECK_NODESTATUS;
    out_t.data = malloc(nodeip.size()+1);
    bzero(out_t.data,out_t.data_len);
    memcpy(out_t.data,nodeip.c_str(),nodeip.size());
    out_t.data_len = (int)nodeip.size()+1;

    auto leaders = dn_group->getleaders();
    dbg(nodeip);
    dbg((char*)out_t.data);
    char sBuf[BUF_SZ] = {0};
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    if(out_t.data!= nullptr){
        free(out_t.data);
        out_t.data= nullptr;
    }
//ask node itself for it status
    if(ask_node_status(sBuf,sLen,DfsNode(nodeip,dfs_cycle->listening_open_port))){
        return true;
    }
//ask leaders for the node status
    auto *sconf = static_cast<conf_server_t *>(dfs_cycle->sconf);
    for (auto & leader : leaders) {
        if(leader.getNodeIp()==messfromleaderip){
            continue;
        }
        if(ask_node_status(sBuf,sLen,DfsNode(leader.getNodeIp(),sconf->ns_srv_port))){
            return true;
        }
    }
    return false;
}
bool ask_node_status(char* sBuf,int sLen,DfsNode askfor){
    char *leaderip = new char[askfor.getNodeIp().size() + 1];
    strcpy(leaderip, askfor.getNodeIp().c_str());
    dbg(leaderip);
    int node_sock = net_connect(leaderip, askfor.getNodePort(), dfs_cycle->error_log, 10);
    dbg(node_sock);
    if (node_sock != NGX_ERROR) { // change sock to master sock and redo send request
//            fcntl(leader_sock, F_SETFL, fcntl(leader_sock, F_GETFL, 0) & ~O_NONBLOCK); // set socket back to blocking
        int ret = send(node_sock, sBuf, sLen, 0);
        dbg(ret);
        if (ret < 0) {
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                          "send err");
            close(node_sock);
            return false;
        }

        int pLen = 0;
        dbg(pLen);
        int rLen = recv(node_sock, &pLen, sizeof(int), MSG_PEEK);
        dbg(rLen);
        if (rLen < 0)
        {
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                          "recv err, rLen: %d", rLen);
            close(node_sock);
            return false;
        }

        char *pNext = (char *)malloc(pLen);
        if (nullptr == pNext)
        {
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                          "malloc err, pLen: %d", pLen);

            close(node_sock);

            return false;
        }

        rLen = readn(node_sock, pNext, pLen);
        dbg(rLen);
        if (rLen < 0)
        {
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                          "read err, rLen: %d", rLen);
            close(node_sock);
            free(pNext);
            pNext = nullptr;
            return false;
        }
        task_t in_t;
        bzero(&in_t, sizeof(task_t));
        task_decodefstr(pNext, rLen, &in_t);
        switch (in_t.ret) {
            case ALIVE: {
                dbg("ALIVE");
                free(pNext);
                pNext = nullptr;
                close(node_sock);
                return true;

            }
            case DEAD: {
                dbg("DEAD");
                break;
            }
            default:
                dfs_log_error(dfs_cycle->error_log,DFS_LOG_ERROR,0,"unknown ip scan cmd: %d\n",PENU(in_t.ret));
                break;
        }
        free(pNext);
        pNext = nullptr;
        close(node_sock);
    }
    return false;
}
void hande_check_nodeself(task_t *task) {
    dbg(__func__);
    auto *node = queue_data(task, task_queue_node_t, tk);
    string nodeip((char*)task->data);
    dbg("ALIVE");
    task->ret=ALIVE;
    task->data = nullptr;
    task->data_len = 0;
    write_back(node);
}

int handle_kill_node(task_t *task) {
    auto *node = queue_data(task, task_queue_node_t, tk);
    char node_iport_string[25] = {0};
    PhxElection *phxElection = dn_get_paxos_obj();
    if (task->data != nullptr) {
        strcpy(node_iport_string, (char *) task->data);
        dbg(node_iport_string);
        // check group status
        // group finish, means that paxos has run
        // check global group master alive
        dbg(phxElection->GetMaster().GetIP());
        dbg(phxElection->GetMaster().GetPort());
        if(phxElection->GetMaster().GetPort()==0){
            // now the group doesnt have a group master
            task->ret = FAIL;
        }
        if (phxElection->IsIMMaster()) {
            // trans task to paxos_thread
            if(node_iport_string==dn_group->getOwn().getNodeIp()){
                task->ret = FAIL;
            }else{
                dn_trans_task(task,paxos_thread);
                task->ret = SUCC;
            }
        } else{
            // master redierct ( return master ip)
            task->ret = FAIL;
        }
    }
    task->data = nullptr;
    task->data_len = 0;
    return write_back(node);
}