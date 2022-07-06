/*
Tencent is pleased to support the open source community by making 
PhxPaxos available.
Copyright (C) 2016 THL A29 Limited, a Tencent company. 
All rights reserved.

Licensed under the BSD 3-Clause License (the "License"); you may 
not use this file except in compliance with the License. You may 
obtain a copy of the License at

https://opensource.org/licenses/BSD-3-Clause

Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" basis, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
implied. See the License for the specific language governing 
permissions and limitations under the License.

See the AUTHORS file for names of contributors. 
*/

#include "PhxElection.h"
#include "../../etc/config.h"
#include <assert.h>
#include <string>
#include <utility>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dfs_types.h>
#include "dn_cycle.h"
#include "dfs_error_log.h"
#include <iostream>
#include <unistd.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <dfs_dbg.h>
#include <dn_group.h>
#include <dn_error_log.h>
#include <dfs_utils.h>


using namespace phxpaxos;
using namespace std;


void Getfilepath(const char *path, const char *filename, char *filepath) {
    strcpy(filepath, path);
    if (filepath[strlen(path) - 1] != '/')
        strcat(filepath, "/");
    strcat(filepath, filename);
    //printf("path is = %s\n",filepath);
}

bool DeleteFile(const char *path) {
    DIR *dir;
    struct dirent *dirinfo;
    struct stat statbuf;
    char filepath[256] = {0};
    lstat(path, &statbuf);
    //dbg(path);
    if (S_ISREG(statbuf.st_mode))//判断是否是常规文件
    {
        remove(path);
    } else if (S_ISDIR(statbuf.st_mode))//判断是否是目录
    {
        if ((dir = opendir(path)) == NULL)
            return 1;
        while ((dirinfo = readdir(dir)) != NULL) {
            Getfilepath(path, dirinfo->d_name, filepath);
            if (strcmp(dirinfo->d_name, ".") == 0 || strcmp(dirinfo->d_name, "..") == 0)//判断是否是特殊目录
                continue;
            DeleteFile(filepath);
            rmdir(filepath);
        }
        closedir(dir);
        rmdir(path);
    }
    return false;
}

PhxElection::PhxElection(const phxpaxos::NodeInfo &oMyNode, phxpaxos::NodeInfoList vecNodeList)
        : m_oMyNode(oMyNode), m_vecNodeList(std::move(vecNodeList)), m_poPaxosNode(nullptr) {
}

PhxElection::~PhxElection() {
    delete m_poPaxosNode;
}

int PhxElection::MakeLogStoragePath(std::string &sLogStoragePath) {
    char sTmp[128] = {0};
    std::string prefix = expand_user("~") +  PREFIX;
    snprintf(sTmp, sizeof(sTmp), "%s/data/datanode/logs/logpath_%s_%d", prefix.c_str(), m_oMyNode.GetIP().c_str(),
             m_oMyNode.GetPort());

    sLogStoragePath = string(sTmp);
    if (access(sLogStoragePath.c_str(), F_OK) != -1) {
        DeleteFile(sLogStoragePath.c_str());
        if (mkdir(sLogStoragePath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1) {
            printf("Create dir fail, path %s\n", sLogStoragePath.c_str());
            return -1;
        }
    } else {
        if (mkdir(sLogStoragePath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1) {
            printf("Create dir fail, path %s\n", sLogStoragePath.c_str());
            return -1;
        }
    }
//    if (access(sLogStoragePath.c_str(), F_OK) == -1)
//    {
//        if (mkdir(sLogStoragePath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1)
//        {
//            printf("Create dir fail, path %s\n", sLogStoragePath.c_str());
//            return -1;
//        }
//    }

    return 0;
}

int PhxElection::GetGroupIdx() {
    return 0;
}

void PhxElection::OnMasterChange(const int iGroupIdx, const NodeInfo &oNewMaster, const uint64_t llVersion) {
    printf("master change!!! groupidx %d newmaster ip %s port %d version %lu\n",
           iGroupIdx, oNewMaster.GetIP().c_str(), oNewMaster.GetPort(), llVersion);
    dn_group->setGroupMaster(DfsNode(oNewMaster.GetIP(), dfs_cycle->listening_open_port));
    group_master_change(DfsNode(oNewMaster.GetIP(), dfs_cycle->listening_open_port));
}

int PhxElection::RunPaxos() {
    Options oOptions;

    int ret = MakeLogStoragePath(oOptions.sLogStoragePath);
    if (ret != 0) {
        return ret;
    }

    oOptions.iGroupCount = 1;

    oOptions.oMyNode = m_oMyNode;
    dbg(m_vecNodeList.size());

    oOptions.vecNodeInfoList = m_vecNodeList;
    oOptions.bUseMembership = true;

    //open inside master state machine
    GroupSMInfo oSMInfo;
    oSMInfo.iGroupIdx = 0;
    oSMInfo.bIsUseMaster = true;
    oSMInfo.vecSMList.push_back(&m_oGrouplistSM);

    oOptions.vecGroupSMInfoList.push_back(oSMInfo);
    oOptions.bOpenChangeValueBeforePropose = true;
    oOptions.pMasterChangeCallback = PhxElection::OnMasterChange;
    oOptions.pLogFunc = dn_log_paxos;

    ret = Node::RunNode(oOptions, m_poPaxosNode);
    if (ret != 0) {
        printf("run paxos fail, ret %d\n", ret);

        return ret;
    }

    //you can change master lease in real-time.
    m_poPaxosNode->SetMasterLease(0, 3000);
    printf("dn run paxos ok\n");
    return 0;
}

const phxpaxos::NodeInfo PhxElection::GetMaster() {
    //only one group, so groupidx is 0.
    return m_poPaxosNode->GetMaster(0);
}

const phxpaxos::NodeInfo PhxElection::GetMasterWithVersion(uint64_t &llVersion) {
    return m_poPaxosNode->GetMasterWithVersion(0, llVersion);
}

const bool PhxElection::IsIMMaster() {
    if (m_poPaxosNode == nullptr) {
        return true;
    }
    return m_poPaxosNode->IsIMMaster(0);
}
const bool PhxElection::IsImInGroup(){
    if (m_poPaxosNode == nullptr) {
        return true;
    }
    NodeInfoList vecNodeInfoList;
    m_poPaxosNode->ShowMembership(0, vecNodeInfoList);
    for (int i = 0; i < (int) vecNodeInfoList.size(); i++) {
        NodeInfo node = vecNodeInfoList[i];
        if(node.GetIP()==m_oMyNode.GetIP()){
            return true;
        }
    }
    return false;
}
const void PhxElection::showmembership() {
    if (m_poPaxosNode == nullptr) {
        return;
    }
    NodeInfoList vecNodeInfoList;
    m_poPaxosNode->ShowMembership(0, vecNodeInfoList);
    for (int i = 0; i < (int) vecNodeInfoList.size(); i++) {
        NodeInfo node = vecNodeInfoList[i];
        printf("member %d : nodeid %lu ip %s port %d\n", i,
               node.GetNodeID(), node.GetIP().c_str(), node.GetPort());
    }
}
const NodeInfoList& PhxElection::getMembers() {
    NodeInfoList vecNodeInfoList;
    if (m_poPaxosNode == nullptr) {
        return vecNodeInfoList;
    }
    m_poPaxosNode->ShowMembership(0, vecNodeInfoList);
    return vecNodeInfoList;
}

const void PhxElection::addmember(phxpaxos::NodeInfo newmember) {
    if (m_poPaxosNode == nullptr) {
        return;
    }
    printf("Add member : nodeid %lu ip %s port %d\n",
           newmember.GetNodeID(), newmember.GetIP().c_str(), newmember.GetPort());
    m_poPaxosNode->AddMember(0, newmember);
}

const void PhxElection::removemember(phxpaxos::NodeInfo rmmember) {
    if (m_poPaxosNode == nullptr) {
        return;
    }
    printf("Remove member : nodeid %lu ip %s port %d\n",
           rmmember.GetNodeID(), rmmember.GetIP().c_str(), rmmember.GetPort());
    m_poPaxosNode->RemoveMember(0, rmmember);
}

phxpaxos::NodeInfo &PhxElection::getMyNode() {
    return m_oMyNode;
}

int PhxElection::Propose(const string &sPaxosValue,
                         PhxGrouplistSMCtx &oGrouplistSMCtx) {
    int iGroupIdx = GetGroupIdx(); //sKey 是目录, hash算法得到groupindex

    SMCtx oCtx;
    //smid must same to PhxEditlogSM.SMID().
    oCtx.m_iSMID = 1; //设置oCtx.m_iSMID为1，与我们刚刚编写的状态机的SMID()相对应，标识我们需要将这个请求送往SMID为1的状态机的Execute函数。
    oCtx.m_pCtx = (void *) &oGrouplistSMCtx;

    uint64_t llInstanceID = 0;
    int ret = m_poPaxosNode->Propose(iGroupIdx, sPaxosValue, llInstanceID, &oCtx);
    // try to re propose
    while (ret == PaxosTryCommitRet_Conflict){
        usleep(500);
        dbg("PaxosTryCommitRet_Conflict");
        ret = m_poPaxosNode->Propose(iGroupIdx, sPaxosValue, llInstanceID, &oCtx);
    }

    if (ret != NGX_OK)
    {
        if(ret == PaxosTryCommitRet_Im_Not_In_Membership){
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno,
                          "datanode: 该节点已被集群剔除，不允许Propose, ret %d", ret);

            return ret;
        }else if(ret == PaxosTryCommitRet_Value_Size_TooLarge){
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno,
                          "datanode: 提议的值超过大小限制, ret %d", ret);

            return ret;
        }
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno,
                      "datanode: paxos propose fail, ret %d", ret);

        return ret;
    }

    return NGX_OK;
}



