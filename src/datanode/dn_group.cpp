//
// Created by ginux on 2020/5/31.
//

#include <fstream>
#include <dfs_dbg.h>
#include <dfs_task.h>
#include "dn_group.h"
#include "dn_conf.h"
#include "dn_cycle.h"
#include "../../etc/config.h"
#include "dn_paxos.h"
#include "dn_net_response_handler.h"

DfsGroup *dn_group;
DfsGroup *old_dn_group;
DfsGroup *master_newest_group;
// random dispatch
/**
 * 随机初始化grouplist，但不直接在dn_group进行更改，将分配存入传入参数中返回
 * @param dn_group_init 随机划分群组的返回值
 */

void group_dispatch_init(DfsGroup *dn_group_init) {
    auto *sconf = static_cast<conf_server_t *>(dfs_cycle->sconf);
    int max_group_nodes = sconf->max_group_nodes;
    const std::vector<DfsNode> &nodelists = dn_group->getNodeList();
    int group_num = 0;
    if (nodelists.size() % max_group_nodes == 0) {
        group_num = (int) nodelists.size() / max_group_nodes;
    } else {
        group_num = (int) nodelists.size() / max_group_nodes + 1;
    }

    Group tmpgroup[group_num];

    for (int i = 0; i < nodelists.size(); ++i) {
        tmpgroup[i % group_num].addNode(dn_group->NodeList[i]);
    }
    for (int j = 0; j < group_num; ++j) {
        tmpgroup[j].setGroupLeader(tmpgroup[j].getNodeList()[0]);
        dn_group_init->addGroup(tmpgroup[j]);
    }

}

/**
 * 使用之前存在的old_dn_group作为划分依据进行群组划分
 */
void group_dispatch_init_asold() {
    auto *sconf = static_cast<conf_server_t *>(dfs_cycle->sconf);
    int max_group_nodes = sconf->max_group_nodes;
    set<DfsNode> findnodes;
    for (const auto& node:dn_group->initNodeList) {
        findnodes.insert(node);
        dbg(node.getNodeIp());
    }
    dbg(findnodes.size());
//    dbg(findnodes.size());
//    dn_get_paxos_obj()->showmembership();
//    for (auto member:dn_get_paxos_obj()->getMembers()) {
//        findnodes.insert(DfsNode(member.GetIP(),member.GetPort()));
//    }
    dbg(findnodes.size());
    auto &groups=old_dn_group->getGroups();
    for(auto & group : groups) {
        auto &NodeList=group.getNodeList();
        auto iter = NodeList.begin();
        while (iter != NodeList.end()) {
            dbg((*iter).getNodeIp());
            iter++;
        }
    }
    old_dn_group->dbgGroup();
    auto group_iter=groups.begin();
    while(group_iter!=groups.end()){
        if (findnodes.count((*group_iter).getGroupLeader())) {
            auto &NodeList=(*group_iter).getNodeList();
            auto iter = NodeList.begin();
            while (iter != NodeList.end()) {
                dbg((*iter).getNodeIp());
                if (findnodes.count((*iter))) {
                    findnodes.erase(findnodes.find(*iter));
                    iter++;
                    dbg("find");
                } else {
                    dbg("not find");
                    iter=NodeList.erase(iter);
                }
            }
            group_iter++;
        } else {
            group_iter=groups.erase(group_iter);
        }
    }
    printf("%s ,old_dn_group->printGroup()\n",__func__);
    old_dn_group->printGroup();
    dbg(findnodes.size());
    bool flag;
    for (const auto& node: findnodes) {
        flag = false;
        for (auto &group : old_dn_group->groups) {
            if (group.getGroupSize() < max_group_nodes) {
                group.addNode(node);
                flag = true;
                break;
            }
        }
        if (flag == false) {
            //need to create a new group
            Group newg;
            newg.addNode(node);
            newg.setGroupLeader(node);
            old_dn_group->addGroup(newg);
        }
    }
}

/**
 * 增加节点群组分配
 * @param node 新增节点
 * @return 新增节点被分配到群组的leader
 */
DfsNode group_dispatch_add(const DfsNode &node) {
    dbg(node.getNodeIp());
    auto *sconf = static_cast<conf_server_t *>(dfs_cycle->sconf);
    int max_group_nodes = sconf->max_group_nodes;
    master_newest_group->lockGroup();
    master_newest_group->dbgGroup();
    for (auto & group : master_newest_group->groups) {
        if(group.findNodeFromip(node.getNodeIp())!= nullptr){
            return group.getGroupLeader();
        }
    }
    for (auto &group : master_newest_group->groups) {
        dbg(group.getGroupLeader().getNodeIp());
        if (group.getGroupSize() < max_group_nodes) {
            //group.addNode(node);
            group.addNode(node);
            master_newest_group->unlockGroup();
            return group.getGroupLeader();
        }
    }
    Group new_group;
    new_group.setGroupLeader(node);
    new_group.addNode(node);
    master_newest_group->addGroup(new_group);
    master_newest_group->unlockGroup();
    return node;
}

/**
 * 从群组中删除节点
 * @param rmnode 被删除的节点
 * @param old_group_leader 被删除节点所属群组的leader
 * @param new_group_leader 被删除节点所属群组执行删除操作后的新leader（当被删除节点为leader时）
 */
bool group_handl_rm(DfsNode &rmnode, DfsNode *old_group_leader, DfsNode *new_group_leader) {
    master_newest_group->lockGroup();
    auto group_iter=master_newest_group->groups.begin();
    while (group_iter!=master_newest_group->groups.end()) {
        auto node_iter=(*group_iter).getNodeList().begin();
        while (node_iter!=(*group_iter).getNodeList().end()) {
            if ((*node_iter) == rmnode) {
                old_group_leader->setNodeIp((*group_iter).getGroupLeader().getNodeIp());
                old_group_leader->setNodePort((*group_iter).getGroupLeader().getNodePort());
                if (rmnode == (*group_iter).getGroupLeader()) {
                    if ((*group_iter).getGroupSize() == 1) {
                        group_iter=master_newest_group->groups.erase(group_iter);
                    } else {
                        node_iter=(*group_iter).getNodeList().erase(node_iter);
                        auto newleader=(*group_iter).getNodeList()[0];
                        (*group_iter).setGroupLeader(newleader);
                        new_group_leader->setNodeIp(newleader.getNodeIp());
                        new_group_leader->setNodePort(newleader.getNodePort());
                    }
                }else{
                    node_iter=(*group_iter).getNodeList().erase(node_iter);
                }
                master_newest_group->unlockGroup();
                return true;
            }
            node_iter++;
        }
        group_iter++;
    }
    master_newest_group->unlockGroup();
    return false;
}

/**
 * 群组的leader发生变化时的处理函数
 * @param op leader变化的类型
 * @param leaders_change 变化的leader群组，不同的op对应vector中的数据的含义不同
 */
void group_leaders_change(phxgrouplist::LeadersChangeMess_op_types op, std::vector<DfsNode> leaders_change) {
    //TODO
    task_t leader_change_task;
    bzero(&leader_change_task, sizeof(task_t));
    leader_change_task.cmd = LEADERS_CHANGE;
    phxgrouplist::LeadersChangeMess LCM;
    LCM.set_op_type(op);
    for (auto leader:leaders_change) {
        auto l = LCM.add_leaders();
        l->set_nodeip(leader.getNodeIp());
        l->set_nodeport(leader.getNodePort());
    }
    string taskdata;
    leader_change_task.data_len = LCM.ByteSizeLong();
//    leader_change_task.data = malloc(leader_change_task.data_len);
    LCM.SerializePartialToString(&taskdata);
//    memcpy(leader_change_task.data, taskdata.c_str(), leader_change_task.data_len);
    leader_change_task.data = (void *) taskdata.c_str();
    dn_trans_task(&leader_change_task, paxos_thread);
}

void group_master_change(DfsNode new_master) {
    dbg(__func__);
    dbg(new_master.getNodeIp());
    if(new_master==dn_group->getOwn()){
        master_newest_group=new DfsGroup(dn_group,"master_newest_group");
        dn_group->dbgGroup();
        master_newest_group->dbgGroup();
    }
    task_t master_change_task;
    bzero(&master_change_task, sizeof(task_t));
    master_change_task.cmd = DN_MASTER_CHANGE;
    master_change_task.data_len = new_master.getNodeIp().size() + 1;
    master_change_task.data = (void *) new_master.getNodeIp().c_str();
//    master_change_task.data = malloc(master_change_task.data_len);
//    memcpy(master_change_task.data, new_master.getNodeIp().c_str(), master_change_task.data_len);
    dn_trans_task(&master_change_task, paxos_thread);


}





