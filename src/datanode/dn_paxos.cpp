#include <string>
#include <GrouplistSM.h>
#include <dfs_dbg.h>
#include <queue>
#include <dfs_utils.h>
#include "dn_paxos.h"
#include "dn_conf.h"
#include "dn_group.h"
#include "dn_ip_scanner.h"
#include "GroupListMess.pb.h"
#include "dn_net_response_handler.h"
#include "dn_thread.h"
#include "../../etc/config.h"
#include "dn_process.h"


using namespace phxpaxos;
using namespace std;

static PhxElection *g_election = nullptr;
static uint32_t   g_edit_op_num = 0;

extern uint64_t g_fs_object_num;
extern _xvolatile rb_msec_t dfs_current_msec;
extern dfs_thread_t *local_ns_service_thread;
extern int start_local_ns_server;
extern std::queue<DfsNode> newleaders;
extern bool start_ns_thread_flag;
/*
static int inc_edit_op_num();
static void *checkpoint_start(void *arg);
static int log_create(task_t *task);
static int log_get_additional_blk(task_t *task);
static int log_close(task_t *task);
static int log_rm(task_t *task);*/

PhxElection* dn_get_paxos_obj(){
    return g_election;
}

static int parse_ipport(const char * pcStr, NodeInfo & oNodeInfo)
{
    char sIP[32] = {0};
    int iPort = -1;

    int count = sscanf(pcStr, "%[^':']:%d", sIP, &iPort);
    if (count != 2)
    {
        return NGX_ERROR;
    }

    oNodeInfo.SetIPPort(sIP, iPort);

    return NGX_OK;
}

static int parse_ipport_list(const char * pcStr,
	NodeInfoList & vecNodeInfoList)
{
    string sTmpStr;
    int iStrLen = strlen(pcStr);

    for (int i = 0; i < iStrLen; i++)
    {
        if (pcStr[i] == ',' || i == iStrLen - 1)
        {
            if (i == iStrLen - 1 && pcStr[i] != ',')
            {
                sTmpStr += pcStr[i];
            }

            NodeInfo oNodeInfo;
            int ret = parse_ipport(sTmpStr.c_str(), oNodeInfo);
            if (ret != 0)
            {
                return ret;
            }

            vecNodeInfoList.push_back(oNodeInfo);

            sTmpStr = "";
        }
        else
        {
            sTmpStr += pcStr[i];
        }
    }

    return NGX_OK;
}

// paxos worker init
// 配置当前运行节点的IP/PORT参数
// 初始化FSEditlog 对象
int dn_paxos_worker_init(cycle_t *cycle)
{
    auto *sconf = (conf_server_t *)cycle->sconf;
    //for test
    //end
    NodeInfo oMyNode; //当前运行节点的IP/PORT参数
    dbg((const char *)sconf->my_paxos.data);
    if (parse_ipport((const char *)sconf->my_paxos.data, oMyNode) != NGX_OK)
    {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, 0,
            "parse myip:myport err");

        return NGX_ERROR;
    }


    //for test
    char myip[INET_ADDRSTRLEN]  = {0};
    get_local_ip(myip);
    oMyNode.SetIPPort(myip,dfs_cycle->listening_paxos_port);
    //end
    NodeInfoList vecNodeList; // Paxos由多个节点构成，这个列表设置这些节点的IP/PORT信息。
    dn_group->lockGroup();
    auto nodelist=dn_group->getNodeList();

    dbg(nodelist.size());
    for(int i=0; i<nodelist.size();i++){
        NodeInfo newnodeinfo;
        newnodeinfo.SetIPPort(nodelist[i].getNodeIp(),dfs_cycle->listening_paxos_port);
        vecNodeList.push_back(newnodeinfo);
        dn_group->initNodeList.push_back(nodelist[i]);
//        dbg(nodelist[i].getNodeIp());
//        dbg(dfs_cycle->listening_paxos_port);
    }
    dn_group->unlockGroup();
    dbg(vecNodeList.size());
    // 当开启了PhxPaxos的成员管理功能后，这个信息仅仅会被接受一次作为集群的初始化，后面将会无视这个参数的存在。

    /*if (parse_ipport_list((const char *)sconf->ot_paxos.data, vecNodeList)
		!= NGX_OK)
    {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, 0,
            "parse ip:port err");

        return NGX_ERROR;
    }*/

    //string editlogDir = string((const char *)sconf->editlog_dir.data);

   // int iGroupCount = (int)sconf->paxos_group_num;

    //g_editlog = new FSEditlog(sconf->enableMaster, oMyNode, vecNodeList, editlogDir, iGroupCount);

    g_election=new PhxElection(oMyNode, vecNodeList);

    if (nullptr == g_election)
    {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, 0,
            "new FSEditlog(...) err");

        return NGX_ERROR;
    }

    return NGX_OK;
}

int dn_paxos_worker_release(cycle_t *cycle)
{
    if (nullptr != g_election)
    {
        delete g_election;
        g_election = nullptr;
    }

    return NGX_OK;
}

int dn_paxos_run()
{
    dbg(__func__);
    int ret = g_election->RunPaxos();
    dbg(ret);
    if(ret == NGX_INVALID_PID){
        process_quit();
        return ret;
    }

    while(g_election->GetMaster().GetPort()==0 && !process_quit_check()){
        sleep(1);
        dbg("can't find master");
    }
    if(g_election->IsIMMaster()){
        char myip[16];
        get_local_ip(myip);
        printf("I'm Msaster. My ip is %s\n",myip);

        group_list_paxos_init();
        //dn_group->printGroup();
    }
    return ret;
}

void set_checkpoint_instanceID(const uint64_t llInstanceID)
{
    //g_election->setCheckpointInstanceID(llInstanceID);
}

void group_list_paxos_init(){
    task_t init_task;
    bzero(&init_task,sizeof(task_t));
    init_task.cmd=GROUP_INIT;
    dn_trans_task(&init_task,paxos_thread);
}

void do_paxos_task_handler(void *q) // param task queue
{
    task_queue_node_t *tnode = nullptr;
    task_t            *t = nullptr;
    queue_t           *cur = nullptr;
    queue_t            qhead;
    task_queue_t      *tq = nullptr;
    dfs_thread_t      *thread = nullptr;

    tq = (task_queue_t *)q; // task que
    thread = get_local_thread(); // THREAD_TASK

    queue_init(&qhead);
    pop_all(tq, &qhead);

    cur = queue_head(&qhead);

    while (!queue_empty(&qhead) && thread->running)
    {
        tnode = queue_data(cur, task_queue_node_t, qe);
        t = &tnode->tk;

        queue_remove(cur);

        do_paxos_task(t, tnode);

        cur = queue_head(&qhead);
    }
}

//paxos thread
int do_paxos_task(task_t *task ,task_queue_node_t *node)
{
    int optype = task->cmd;

    switch (optype)
    {
        case NODE_WANT_JOIN:
            dbg("grouplist_node_join");
            grouplist_node_join(task);
            dn_free_task_node(node);
            break;

        case NODE_REMOVE: //
            grouplist_node_remove(task);
            dn_free_task_node(node);
            break;

        case GROUP_INIT:
            dn_free_task_node(node);
            grouplist_node_init();
            break;

        case LEADERS_CHANGE:
            handle_leaders_change(task);
            dn_free_task_node(node);
            break;

        case DN_MASTER_CHANGE:
            handl_master_change(task);
            dn_free_task_node(node);
            break;

        case GROUP_SHUTDOWN:
            dn_free_task_node(node);
            grouplist_shutdown();
            break;

        case KILL_NODE:
            grouplist_killnode(task);
            dn_free_task_node(node);
            break;
        default:
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                          "unknown optype: ", optype);

            return NGX_ERROR;
    }

    return NGX_OK;
}

//run by other message call
/**
 * 包装节点加入信息，发起propose
 * @param task 来自其他节点的加入信息
 * @return
 */
int grouplist_node_join(task_t *task)
{
    char leaderip[INET_ADDRSTRLEN]  = {0};
    memcpy(leaderip,task->key,INET_ADDRSTRLEN);
    dbg(leaderip);
    char new_nodeip[task->data_len+1];
    bzero(new_nodeip,task->data_len+1);
    memcpy(new_nodeip,task->data,task->data_len);
    if(dn_group->isContain(DfsNode(new_nodeip,dfs_cycle->listening_paxos_port))){
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, 0,"Node join failed,Node %s exist!",new_nodeip);
        dn_get_paxos_obj()->removemember(phxpaxos::NodeInfo(new_nodeip,dfs_cycle->listening_paxos_port));
        dn_get_paxos_obj()->addmember(phxpaxos::NodeInfo(new_nodeip,dfs_cycle->listening_paxos_port));
        dn_get_paxos_obj()->showmembership();
        return NGX_OK;
    }
    phxpaxos::NodeInfo newmember(new_nodeip,dfs_cycle->listening_paxos_port);
    dn_get_paxos_obj()->addmember(newmember);
    dn_get_paxos_obj()->showmembership();
    dn_group->printGroup();
	string sPaxosValue;
	PhxGrouplistSMCtx oGrouplistSMCtx;
    phxgrouplist::GrouplistMess  gl_mess;
    gl_mess.set_epoch(dn_group->epoch+1);
    gl_mess.set_op_type(phxgrouplist::GrouplistMess_op_types_GL_ADD);
	auto gl_add= gl_mess.mutable_gl_add(); // protobuf
    gl_add->mutable_master()->set_nodeport(dn_group->getGroupMaster().getNodePort());
    gl_add->mutable_master()->set_nodeip(dn_group->getGroupMaster().getNodeIp());
    gl_add->mutable_group_leader_addto()->set_nodeport(dfs_cycle->listening_paxos_port);
    dbg(dfs_cycle->listening_paxos_port);
    gl_add->mutable_group_leader_addto()->set_nodeip(leaderip);
    gl_add->mutable_new_node()->set_nodeport(dfs_cycle->listening_paxos_port);
    gl_add->mutable_new_node()->set_nodeip(new_nodeip);
    gl_mess.SerializeToString(&sPaxosValue);

    // 写入
    int ret=g_election->Propose( sPaxosValue, oGrouplistSMCtx);
    dbg(ret);
    dbg("Propose finish");
	return NGX_OK;//write_back(node);
}
/**
 * 向paxos发出删除节点propose
 * @param task 来自其他节点的删除节点请求信息
 * @return
 */
int grouplist_node_remove(task_t *task)
{
    string rmip=(char *)task->data;
    DfsNode rmnode(rmip,dfs_cycle->listening_paxos_port);
    DfsNode oldleader;
    DfsNode newleader;
    if(!group_handl_rm(rmnode, &oldleader, &newleader)){
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, 0,"Node_remove failed,Node %s doesn't exist!",rmip.c_str());
        task->ret = SUCC;
        return 0;
    }
    phxpaxos::NodeInfo rmmember(rmip,dfs_cycle->listening_paxos_port);
    master_newest_group->dbgGroup();
    dn_get_paxos_obj()->removemember(rmmember);
    dn_get_paxos_obj()->showmembership();
    string sPaxosValue;
    PhxGrouplistSMCtx oGrouplistSMCtx;
    phxgrouplist::GrouplistMess  gl_mess;
    gl_mess.set_epoch(dn_group->epoch+1);
    gl_mess.set_op_type(phxgrouplist::GrouplistMess_op_types_GL_REMOVE);
    auto gl_remove= gl_mess.mutable_gl_remove(); // protobuf
    gl_remove->mutable_master()->set_nodeport(dn_group->getGroupMaster().getNodePort());
    gl_remove->mutable_master()->set_nodeip(dn_group->getGroupMaster().getNodeIp());
    gl_remove->mutable_remove_node()->set_nodeip(rmip);
    gl_remove->mutable_remove_node()->set_nodeport(dfs_cycle->listening_paxos_port);
    gl_remove->mutable_group_oldleader_removefrom()->set_nodeip(oldleader.getNodeIp());
    gl_remove->mutable_group_oldleader_removefrom()->set_nodeport(oldleader.getNodePort());
    gl_remove->mutable_group_newleader_removefrom()->set_nodeip(newleader.getNodeIp());
    gl_remove->mutable_group_newleader_removefrom()->set_nodeport(newleader.getNodePort());
    gl_mess.SerializeToString(&sPaxosValue);
    // 写入
    g_election->Propose( sPaxosValue, oGrouplistSMCtx);
    task->ret = SUCC;
    return 0;
}
/**
 * 向paxos群组发起初始化propose
 * @return
 */
int grouplist_node_init()
{
    string sPaxosValue;
   // DfsGroup* dn_group_init;//dn_group
    if(old_dn_group->getGroups().size()!=0){
        group_dispatch_init_asold();
        //dn_group_init=old_dn_group;
        master_newest_group = new DfsGroup(old_dn_group,"master_newest_group");
        old_dn_group->dbgGroup();
    }else{
        master_newest_group=new DfsGroup("master_newest_group");
        group_dispatch_init(master_newest_group);
    }
    master_newest_group->dbgGroup();
    PhxGrouplistSMCtx oGrouplistSMCtx;
    phxgrouplist::GrouplistMess  gl_mess;
    gl_mess.set_epoch(dn_group->epoch+1);
    gl_mess.set_op_type(phxgrouplist::GrouplistMess_op_types_GL_INIT);
    auto groupList= gl_mess.mutable_gl_init(); // protobuf
    //groupList.set_groupstatus(static_cast<phxgrouplist::GroupList_st_type>(dn_group->getGlobalStatus()));
    groupList->mutable_master()->set_nodeport(master_newest_group->getGroupMaster().getNodePort());
    groupList->mutable_master()->set_nodeip(master_newest_group->getGroupMaster().getNodeIp());
    auto init_group_list=groupList->mutable_init_grouplist();
    for(auto& itr_group:master_newest_group->getGroups()){
        auto new_group=init_group_list->add_groups();
        new_group->mutable_leader()->set_nodeport(itr_group.getGroupLeader().getNodePort());
        new_group->mutable_leader()->set_nodeip(itr_group.getGroupLeader().getNodeIp());
        for(auto& itr_node:itr_group.getNodeList()){
            auto group_node=new_group->add_nodelist();
            group_node->set_nodeip(itr_node.getNodeIp());
            group_node->set_nodeport(itr_node.getNodePort());
        }
    }
    gl_mess.SerializeToString(&sPaxosValue);
    // 写入

    g_election->Propose( sPaxosValue, oGrouplistSMCtx);

    return 0;
}
/**
 * 处理群组leader变化的消息,主要为了告知leader，群组所有leader的变化
 * @param task
 * @return
 */
int handle_leaders_change(task_t *task) {
    dbg(__func__ );
    phxgrouplist::LeadersChangeMess LCM;
    LCM.ParseFromArray(task->data,task->data_len);
    // update name node leaders ip port string
    dfs_cycle->leaders_paxos_ipport_string = dn_group->get_ns_leaders_string();
    dbg(dfs_cycle->leaders_paxos_ipport_string );
//    if(LCM.epoch()!=1 && LCM.epoch()<=dn_group->epoch){
//        return NGX_OK;
//    }
    dbg(LCM.op_type());
    switch (LCM.op_type()) {
        case phxgrouplist::LeadersChangeMess_op_types_LC_INIT:{
            // set namenode start string
            string ns_srv_string;
            conf_server_t  *sconf = nullptr;
            int cnt = 0;
            for(auto &node:LCM.leaders()){
                ns_srv_string += node.nodeip()+':'+ std::to_string(dfs_cycle->listening_nssrv_port);
                if(cnt!=LCM.leaders().size()){
                    ns_srv_string += ',';
                }
            }
            sconf = static_cast<conf_server_t *>(dfs_cycle->sconf);
            sconf->ns_srv.data = reinterpret_cast<uchar_t *>(strdup(ns_srv_string.c_str()));
            sconf->ns_srv.len = ns_srv_string.length();
            // if master propose group init finish , then node can start ns thread
            start_ns_thread_flag = true;
            break;
        }
        //
        case phxgrouplist::LeadersChangeMess_op_types_LC_ADD: {
            DfsNode leader(LCM.leaders()[0]);
            // change  global ip port string
            // check if itself start namenode
            if(leader.getNodeIp() == dn_group->Own.getNodeIp()){
                // start namenode
                start_local_ns_server = true;
            }
            newleaders.push(leader);
            break;
        }
        case phxgrouplist::LeadersChangeMess_op_types_LC_REMOVE:
            //节点leader能自动检测到节点掉线，不需要额外通知
            break;
        case phxgrouplist::LeadersChangeMess_op_types_LC_REPLACE: {
            DfsNode oldleader(LCM.leaders()[0]);
            DfsNode newleader(LCM.leaders()[1]);
            // change  global ip port string
            // check if itself start namenode
            dbg(newleader.getNodeIp());
            dbg(dn_group->Own.getNodeIp());
            if (newleader.getNodeIp() == dn_group->Own.getNodeIp()) {
                // start namenode
                dbg(start_local_ns_server);
                start_local_ns_server = true;
            }
            newleaders.push(newleader);
            break;
        }
        default:
            dbg("KEY default");
            break;
    }
    return 0;
}
/**
 * 处理群组master变化的消息,主要为了告知leader，群组master的变化
 * @param task
 * @return
 */
int handl_master_change(task_t *task) {
    dbg(__func__ );
    auto *sconf = static_cast<conf_server_t *>(dfs_cycle->sconf);
    if(dn_group->getGroups().empty()){
        return NGX_ERROR;
    }
    if(dn_group->isLeader()){
        char *leaderip = new char[dn_group->getOwn().getNodeIp().size() + 1];
        strcpy(leaderip, dn_group->getOwn().getNodeIp().c_str());
        dbg(leaderip);
        int leader_sock = net_connect(leaderip, sconf->ns_srv_port, dfs_cycle->error_log);
        if (leader_sock != NGX_ERROR) { // change sock to master sock and redo send request
            char sBuf[BUF_SZ] = {0};
            int sLen = task_encode2str(task, sBuf, sizeof(sBuf));
//            fcntl(leader_sock, F_SETFL, fcntl(leader_sock, F_GETFL, 0) & ~O_NONBLOCK); // set socket back to blocking
            int ret = send(leader_sock, sBuf, sLen, 0);
            if (ret < 0) {
                dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                              "send err");
                close(leader_sock);
                return NGX_ERROR;
            }
            int pLen = 0;
            int rLen = 0;
            rLen = recv(leader_sock, &pLen, sizeof(int), MSG_PEEK);
//            dbg(rLen);
            if (rLen < 0)
            {
                dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                              "recv err, rLen: %d", rLen);
                close(leader_sock);
                return NGX_ERROR;
            }
            char *pNext = (char *)malloc(pLen);
            if (nullptr == pNext)
            {
                dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                              "malloc err, pLen: %d", pLen);
                close(leader_sock);
                return NGX_ERROR;
            }
            rLen = read(leader_sock, pNext, pLen);
            if (rLen < 0)
            {
                dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                              "read err, rLen: %d", rLen);
                close(leader_sock);
                free(pNext);
                pNext = nullptr;
                return NGX_ERROR;
            }
            task_t in_t;
            bzero(&in_t, sizeof(task_t));
            task_decodefstr(pNext, rLen, &in_t);
            dbg(in_t.ret);
            if(in_t.ret!=SUCC){
                dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, errno,
                              "master change failed: %d", rLen);
                close(leader_sock);
                free(pNext);
                pNext = nullptr;
                return NGX_ERROR;
            }
            free(pNext);
            pNext = nullptr;
            close(leader_sock);
        }
    }
    return 0;
}
int grouplist_killnode(task_t *task){
    string killip=(char *)task->data;
    DfsNode killnode(killip,dfs_cycle->listening_paxos_port);
    DfsNode oldleader;
    DfsNode newleader;
    if(!group_handl_rm(killnode, &oldleader, &newleader)){
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, 0,"Node_kill failed,Node %s doesn't exist!",killip.c_str());
        task->ret = SUCC;
        return 0;
    }
    phxpaxos::NodeInfo killmember(killip,dfs_cycle->listening_paxos_port);
    dn_get_paxos_obj()->showmembership();
    dn_get_paxos_obj()->removemember(killmember);
    dn_get_paxos_obj()->showmembership();
    string sPaxosValue;
    PhxGrouplistSMCtx oGrouplistSMCtx;
    phxgrouplist::GrouplistMess  gl_mess;
    gl_mess.set_epoch(dn_group->epoch+1);
    gl_mess.set_op_type(phxgrouplist::GrouplistMess_op_types_GL_KILLNODE);
    auto gl_remove= gl_mess.mutable_gl_remove(); // protobuf
    gl_remove->mutable_master()->set_nodeport(dn_group->getGroupMaster().getNodePort());
    gl_remove->mutable_master()->set_nodeip(dn_group->getGroupMaster().getNodeIp());
    gl_remove->mutable_remove_node()->set_nodeip(killip);
    gl_remove->mutable_remove_node()->set_nodeport(dfs_cycle->listening_paxos_port);
    gl_remove->mutable_group_oldleader_removefrom()->set_nodeip(oldleader.getNodeIp());
    gl_remove->mutable_group_oldleader_removefrom()->set_nodeport(oldleader.getNodePort());
    gl_remove->mutable_group_newleader_removefrom()->set_nodeip(newleader.getNodeIp());
    gl_remove->mutable_group_newleader_removefrom()->set_nodeport(newleader.getNodePort());
    gl_mess.SerializeToString(&sPaxosValue);
    // 写入
    g_election->Propose( sPaxosValue, oGrouplistSMCtx);
    task->ret = SUCC;
    return 0;
}
/**
 * 向paxos群组发起关机propose
 */
void grouplist_shutdown(){
    dbg(__func__ );
    dn_group->start_shutdown = true;
    string sPaxosValue;
    PhxGrouplistSMCtx oGrouplistSMCtx;
    phxgrouplist::GrouplistMess  gl_mess;
    gl_mess.set_epoch(dn_group->epoch+1);
    gl_mess.set_op_type(phxgrouplist::GrouplistMess_op_types_GL_SHUTDOWN);
    gl_mess.SerializeToString(&sPaxosValue);
    // 写入
    g_election->Propose( sPaxosValue, oGrouplistSMCtx);
}
