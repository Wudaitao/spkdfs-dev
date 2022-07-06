//
// Created by ginux on 2020/5/28.
//

#include "nn_group.h"
#include "nn_task_queue.h"
#include "nn_net_response_handler.h"
#include "nn_paxos.h"
#include "../../etc/config.h"
#include "dfs_dbg.h"
#include "nn_dn_index.h"

DfsGroup * nn_group;
int grouplist_change(task_t* task){
    dbg(__func__);
    auto *node = queue_data(task, task_queue_node_t, tk);
    //std::string group_from_dn = (char *)task->data ;
    DfsGroup tmpgroup;
    tmpgroup.decodeFromCharArray(static_cast<char *>(task->data), task->data_len);
    std::vector<DfsNode> old_leaders;
    if(nn_group->epoch < tmpgroup.epoch){
        old_leaders=nn_group->getleaders();
        nn_group->lockGroup();
        nn_group->decodeFromCharArray(static_cast<char *>(task->data), task->data_len);
        nn_group->epoch=tmpgroup.epoch;
        nn_group->unlockGroup();
        leaders_change(old_leaders);
    }
    // this means gl add new node , then we need check node status
    if(strlen(task->key)>0){
        dn_store_t *dns = get_dn_store_obj((uchar_t *) task->key);
        if (dns) { // check datanode timer
            dn_timer_update(dns);
        }
    }
    dbg("nn_group");
    nn_group->printGroup();

    task->data = nullptr;
    task->data_len = 0;
    task->ret=NGX_OK;
    return write_back(node);
}

int leaders_change(std::vector<DfsNode> old_leaders){
    dbg(__func__);
    std::vector<DfsNode> nochange_leaders;
    std::vector<DfsNode> add_leaders;
    std::vector<DfsNode> rm_leaders;
    std::vector<DfsNode> new_leaders=nn_group->getleaders();
    std::sort(old_leaders.begin(),old_leaders.end());
    std::sort(new_leaders.begin(),new_leaders.end());
    std::set_intersection(old_leaders.begin(),old_leaders.end(),new_leaders.begin(),new_leaders.end(),insert_iterator<vector<DfsNode>>(nochange_leaders,nochange_leaders.begin()));
    std::sort(nochange_leaders.begin(),nochange_leaders.end());
    std::set_difference(old_leaders.begin(),old_leaders.end(),nochange_leaders.begin(),nochange_leaders.end(),insert_iterator<vector<DfsNode>>(rm_leaders,rm_leaders.begin()));
    std::set_difference(new_leaders.begin(),new_leaders.end(),nochange_leaders.begin(),nochange_leaders.end(),insert_iterator<vector<DfsNode>>(add_leaders,add_leaders.begin()));
    for(int i=0;i<rm_leaders.size();i++){
        phxpaxos::NodeInfo rmmember(rm_leaders[i].getNodeIp(),NN_PAXOS_PORT);
        nn_get_paxos_obj()->removemember(rmmember);
    }
    nn_get_paxos_obj()->showmembership();
    for(int i=0;i<add_leaders.size();i++){
        phxpaxos::NodeInfo newmember(add_leaders[i].getNodeIp(),NN_PAXOS_PORT);
        nn_get_paxos_obj()->addmember(newmember);
    }
    nn_get_paxos_obj()->showmembership();
    return NGX_OK;
}

int check_nodestatus(task_t* task){
    dbg(__func__);
    task_queue_node_t *node = queue_data(task, task_queue_node_t, tk);
    string nodeip((char*)task->data);
    dbg(nodeip);
    dbg(nn_group->AliveNodeList.size());
    auto alivenode = nn_group->AliveNodeList.begin();
    while(alivenode!=nn_group->AliveNodeList.end()){
        dbg(alivenode->getNodeIp());
        alivenode++;
    }
    if(nn_group->AliveNodeList.count(DfsNode(nodeip,0))){
        dbg("ALIVE");
        task->ret=ALIVE;
    }else{
        dbg("DEAD");
        task->ret=DEAD;
    }
    task->data = nullptr;
    task->data_len = 0;
    return write_back(node);
}
int dn_master_change(task_t* task){
    dbg(__func__);
    task_queue_node_t *node = queue_data(task, task_queue_node_t, tk);
    string newmasterip((char*)task->data);
    dbg(newmasterip);
    int masterport=nn_group->getGroupMaster().getNodePort();
    nn_group->setGroupMaster(DfsNode(newmasterip,masterport));
    task->ret=SUCC;
    task->data = nullptr;
    task->data_len = 0;
    return write_back(node);
}