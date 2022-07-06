//
// Created by llp on 6/5/20.
//
#include <dfs_types.h>
#include <dfs_dbg.h>
#include <dfs_utils.h>
#include "dn_paxos_handler.h"

#include "dn_cycle.h"
#include "dfs_error_log.h"
#include "dn_group.h"
#include "dn_paxos.h"
#include "dn_net_response_handler.h"
#include "dn_process.h"
#include "dn_signal.h"
#include "dn_ns_service.h"

using namespace std;
extern dfs_thread_t *local_ns_service_thread;
/**
 * 节点接收到propose消息后的处理函数
 * @param llInstanceID
 * @param sPaxosValue 通过propose传递的值
 * @return
 */
int group_paxos_handler(const uint64_t llInstanceID,
                        const std::string &sPaxosValue) {
    string str;
    GrouplistMess gl_mess;
    gl_mess.ParseFromString(sPaxosValue);
    int optype = gl_mess.op_type();
    if(dn_group->epoch >=gl_mess.epoch()){
        return NGX_OK;
    }
    dn_group->epoch=gl_mess.epoch();
    dbg(optype);
    dbg(dn_group->epoch);
    task_t glchange_task;
    glchange_task.cmd = GROUPLIST_CHANGE;

    switch (optype) {
        case GrouplistMess_op_types_GL_INIT:
            // init group
            update_gl_init(gl_mess.gl_init());
            break;
            //add node to grouplist
        case GrouplistMess_op_types_GL_ADD:
            update_gl_add(gl_mess.gl_add());
            strcpy(glchange_task.key, gl_mess.gl_add().new_node().nodeip().c_str());
            break;
            //remove node from grouplist
        case GrouplistMess_op_types_GL_REMOVE:
            update_gl_remove(gl_mess.gl_remove());
            break;
        case GrouplistMess_op_types_GL_KILLNODE:
            update_gl_killnode(gl_mess.gl_remove());
            break;
        case GrouplistMess_op_types_GL_SHUTDOWN:
            gl_shutdown();
            break;
        default:
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, 0,
                          "unknown optype: ", optype);
            return NGX_ERROR;
    }
    //dn_group->epochIncrease();
    dn_group->printGroup();
    //send grouplist change mess to local nn
    string glstr;
    auto gl=dn_group->encodeToProtobufMess();
    gl.set_epoch(dn_group->epoch+1);
    gl.SerializeToString(&glstr);
    task_data_assignment(&glchange_task, glstr.c_str(), glstr.size());

    if (local_ns_service_thread != nullptr && local_ns_service_thread->running) {
        push_task_to_tq(&glchange_task, local_ns_service_thread);
    }
    dn_group->store_persistent_grouplist();
    return NGX_OK;
}
/**
 * grouplist 初始化
 * @param gl_init 初始化消息
 * @return
 */
int update_gl_init(const GroupListInit &gl_init) {
    dbg(__func__);
    dn_get_paxos_obj()->showmembership();
    //检测到member group中不包含自己，重启
    if(!dn_get_paxos_obj()->IsImInGroup()){
        //do restart
        dbg("I'm not in member group, Restart!");
        dfs_log_error(dfs_cycle->error_log,DFS_LOG_INFO,errno,"I'm not in member group, Restart!\n");
        int nn_id = process_get_nnpid();
        if(nn_id > 0){ // kill namenode first
            dn_kill_namenode();
            int res = kill(process_get_pid(dfs_cycle), SIGNAL_RECONF);
            dfs_log_error(dfs_cycle->error_log,DFS_LOG_INFO,errno,"%s send SIGNAL_RECONF %d\n",__func__,res);
        } else{ // if not namenode ,then do reconf
            int res = kill(process_get_pid(dfs_cycle), SIGNAL_RECONF);
            dfs_log_error(dfs_cycle->error_log,DFS_LOG_INFO,errno,"%s send SIGNAL_RECONF %d\n",__func__,res);
        }
    }
    dn_group->clear();
    for (int i = 0; i < gl_init.init_grouplist().groups_size(); i++) {
        auto group_cur = gl_init.init_grouplist().groups(i);
        Group new_group;
        for (int j = 0; j < group_cur.nodelist_size(); j++) {
            DfsNode new_node(group_cur.nodelist(j));
            new_group.addNode(new_node);
        }
        new_group.setGroupLeader(DfsNode(group_cur.leader()));
        dn_group->addGroup(new_group);
    }
    //检测到node group中不包含自己，重启
    if(dn_group->isContain(dn_group->getOwn())==false){
        //do restart
        dbg("I'm not in node group, Restart!");
        dfs_log_error(dfs_cycle->error_log,DFS_LOG_INFO,errno,"I'm not in node group, Restart!\n");
        int nn_id = process_get_nnpid();
        if(nn_id > 0){ // kill namenode first
            dn_kill_namenode();
            int res = kill(process_get_pid(dfs_cycle), SIGNAL_RECONF);
            dfs_log_error(dfs_cycle->error_log,DFS_LOG_INFO,errno,"%s send SIGNAL_RECONF %d\n",__func__,res);
        } else{ // if not namenode ,then do reconf
            int res = kill(process_get_pid(dfs_cycle), SIGNAL_RECONF);
            dfs_log_error(dfs_cycle->error_log,DFS_LOG_INFO,errno,"%s send SIGNAL_RECONF %d\n",__func__,res);
        }
    }
    group_leaders_change(LeadersChangeMess::LC_INIT, dn_group->getleaders());
    return NGX_OK;
}
/**
 * 处理节点加入群组propose操作
 * @param gl_add master传递来的加入信息
 * @return
 */
static int update_gl_add(const GrouplistAdd &gl_add) {
    dbg(__func__);
    DfsNode new_node(gl_add.new_node());

    //dn_get_paxos_obj()->addmember(newmember);
    DfsNode group_leader_addto(gl_add.group_leader_addto());
    if (new_node == group_leader_addto) {
        Group new_group;
        new_group.setGroupLeader(group_leader_addto);
        new_group.addNode(new_node);
        dn_group->addGroup(new_group);
        group_leaders_change(LeadersChangeMess::LC_ADD, std::vector<DfsNode>{new_node});
    } else {
        for (auto & group : dn_group->groups) {
            if (group.getGroupLeader() == group_leader_addto) {
                //todo: need to be tested
                group.addNode(new_node);
            }
        }
    }

    dn_get_paxos_obj()->showmembership();
    dn_group->printGroup();
    return NGX_OK;
}
/**
 * 处理删除群组中的节点propose操作
 * @param gl_remove master传递来的删除信息
 * @return
 */
static int update_gl_remove(const GroupListRemove &gl_remove) {
    dbg(__func__);
    DfsNode remove_node(gl_remove.remove_node());
    DfsNode group_oldleader_rmfrom(gl_remove.group_oldleader_removefrom());
    DfsNode group_newleader_rmfrom(gl_remove.group_newleader_removefrom());
    dbg(remove_node.getNodeIp());
    dbg(group_oldleader_rmfrom.getNodeIp());
    for (auto itr_group = dn_group->groups.begin(); itr_group != dn_group->groups.end(); itr_group++) {
        if ((*itr_group).getGroupLeader() == group_oldleader_rmfrom) {
            if (group_oldleader_rmfrom == remove_node) {
                if ((*itr_group).getNodeList().size() == 1) {// if only one node in the group,remove the group
                    dn_group->groups.erase(itr_group);
                    group_leaders_change(LeadersChangeMess::LC_REMOVE, std::vector<DfsNode>{group_oldleader_rmfrom});
                } else {
                    (*itr_group).setGroupLeader(group_newleader_rmfrom);
                    (*itr_group).removeNodeFromIp(remove_node.getNodeIp());
                    group_leaders_change(LeadersChangeMess::LC_REPLACE,
                                         std::vector<DfsNode>{group_oldleader_rmfrom, group_newleader_rmfrom});
                }
            } else {
                (*itr_group).removeNodeFromIp(remove_node.getNodeIp());
            }
            break;
        }
    }
    // check when remove ,find it self removed ,then restart
    if(remove_node.getNodeIp() == dn_group->getOwn().getNodeIp() && dn_group->getGlobalStatus()!=GROUP_SHUTDOWN){
        int nn_id = process_get_nnpid();
        if(nn_id > 0){ // kill namenode first
            dn_kill_namenode();
            int res = kill(process_get_pid(dfs_cycle), SIGNAL_RECONF);
            dfs_log_error(dfs_cycle->error_log,DFS_LOG_INFO,errno,"%s send SIGNAL_RECONF %d\n",__func__,res);
        } else{ // if not namenode ,then do reconf
            int res = kill(process_get_pid(dfs_cycle), SIGNAL_RECONF);
            dfs_log_error(dfs_cycle->error_log,DFS_LOG_INFO,errno,"%s send SIGNAL_RECONF %d\n",__func__,res);
        }
    }
    return NGX_OK;
}
static int update_gl_killnode(const GroupListRemove &gl_remove) {
    dbg(__func__);
    DfsNode remove_node(gl_remove.remove_node());
    DfsNode group_oldleader_rmfrom(gl_remove.group_oldleader_removefrom());
    DfsNode group_newleader_rmfrom(gl_remove.group_newleader_removefrom());
    dbg(remove_node.getNodeIp());
    dbg(group_oldleader_rmfrom.getNodeIp());
    for (auto itr_group = dn_group->groups.begin(); itr_group != dn_group->groups.end(); itr_group++) {
        if ((*itr_group).getGroupLeader() == group_oldleader_rmfrom) {
            if (group_oldleader_rmfrom == remove_node) {
                if ((*itr_group).getNodeList().size() == 1) {// if only one node in the group,remove the group
                    dn_group->groups.erase(itr_group);
                    group_leaders_change(LeadersChangeMess::LC_REMOVE, std::vector<DfsNode>{group_oldleader_rmfrom});
                } else {
                    (*itr_group).setGroupLeader(group_newleader_rmfrom);
                    (*itr_group).removeNodeFromIp(remove_node.getNodeIp());
                    group_leaders_change(LeadersChangeMess::LC_REPLACE,
                                         std::vector<DfsNode>{group_oldleader_rmfrom, group_newleader_rmfrom});
                }
            } else {
                (*itr_group).removeNodeFromIp(remove_node.getNodeIp());
            }
            //(*itr_group).isfull=false;
            break;
        }
    }
    if(remove_node.getNodeIp()==dn_group->getOwn().getNodeIp()){
        gl_shutdown();
    }
    return NGX_OK;
}
/*
 * 执行关机propose操作
 */
void gl_shutdown() {
    dbg(__func__);
    if(dn_get_paxos_obj()->IsIMMaster()){
        sleep(10);
    }
    int pr = 0;
    int nn_id = process_get_nnpid();
    dn_group->setGlobalStatus(GROUP_SHOTDOWN);
    /*
     * This way is safe shotdown ,but has some problems to fix
     * */
//    if (dn_group->isLeader()) {
//        kill(nn_id, SIGNAL_QUIT);
//        do {
//            pr = waitpid(nn_id, nullptr, WNOHANG);
//
//        } while (pr == 0);
//        if(pr == -1){
////            strerror(errno);
////            dfs_log_error(dfs_cycle->error_log,DFS_LOG_ERROR,errno,"errno ");
//            if(errno == 10){ //10: No child processes
//                kill(process_get_pid(dfs_cycle), SIGNAL_QUIT);
//            }
//        }
//
//        if (pr == nn_id){
//            kill(process_get_pid(dfs_cycle), SIGNAL_QUIT);
////            process_quit();
//        }
//    }
    int ret;
    std::string execDir = getExecDir();
    ret = execl((execDir + "kill.sh").c_str(), "kill.sh", nullptr);
    if (ret == -1) {
        fprintf(stderr, "kill.sh error : %s\n", strerror(errno));
        exit(errno);
    }

}
