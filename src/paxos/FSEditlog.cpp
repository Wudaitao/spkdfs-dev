#include "FSEditlog.h"
#include <assert.h>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dfs_dbg.h>
#include "dfs_types.h"
#include "dfs_error_log.h"
#include "nn_cycle.h"
#include "nn_error_log.h"

FSEditlog::FSEditlog(int enableMaster,const NodeInfo & oMyNode, const NodeInfoList & vecNodeList,
    string & sPaxosLogPath, int iGroupCount) : m_oMyNode(oMyNode), 
    m_vecNodeList(vecNodeList), m_sPaxosLogPath(sPaxosLogPath), 
    m_iGroupCount(iGroupCount), m_poPaxosNode(nullptr)
{
    this->enableMaster = enableMaster;
}

FSEditlog::~FSEditlog()
{
    if (nullptr != m_poPaxosNode)
    {
        delete m_poPaxosNode;
    }
}

// llInstanceId 这里是 checkpoint id
void FSEditlog::setCheckpointInstanceID(const uint64_t llInstanceID)
{
    m_oEditlogSM.SyncCheckpointInstanceID(llInstanceID);
}

int FSEditlog::RunPaxos()
{
    Options oOptions;
    // MakeLogStoragePath函数生成我们存放PhxPaxos产生的数据的目录路径
    int ret = MakeLogStoragePath(oOptions.sLogStoragePath);
    if (ret != NGX_OK)
    {
        return ret;
    }
    //this groupcount means run paxos group count.
    //every paxos group is independent, there are no any communicate between any 2 paxos group.
    oOptions.iGroupCount = m_iGroupCount; //标识我们想同时运行多少个PhxPaxos实例
    oOptions.oMyNode = m_oMyNode;
    oOptions.vecNodeInfoList = m_vecNodeList;
    oOptions.bUseMembership = true;

    for (int iGroupIdx = 0; iGroupIdx < m_iGroupCount; iGroupIdx++)
    {
        GroupSMInfo oSMInfo;
        oSMInfo.iGroupIdx = iGroupIdx;
        //one paxos group can have multi state machine.
        oSMInfo.vecSMList.push_back(&m_oEditlogSM);
        if(this->enableMaster){
            oSMInfo.bIsUseMaster = true; //开启我们内置的一个Master状态机
        } else{
            oSMInfo.bIsUseMaster = false; //开启我们内置的一个Master状态机
        }

        oOptions.vecGroupSMInfoList.push_back(oSMInfo); //vecGroupSMInfoList 描述了多个PhxPaxos实例对应的状态机列表
    }

    oOptions.pLogFunc = nn_log_paxos;

    ret = Node::RunNode(oOptions, m_poPaxosNode); //通过Node::RunNode即可获得PhxPaxos的实例指针
    if (ret != NGX_OK)
    {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno, 
            "run paxos fail, ret %d", ret);
		
        return ret;
    }

    dfs_log_error(dfs_cycle->error_log, DFS_LOG_INFO, 0, "run namenode paxos ok...");
	
    return NGX_OK;
}

const NodeInfo FSEditlog::GetMaster(const string & sKey)
{
    int iGroupIdx = GetGroupIdx(sKey);
//    printf("## now master group id %d; is master: %d \n",iGroupIdx,m_poPaxosNode->IsIMMaster(iGroupIdx));
//    printf("## master ip:port %s:%d \n",m_poPaxosNode->GetMaster(iGroupIdx).GetIP().c_str(),m_poPaxosNode->GetMaster(iGroupIdx).GetPort());

    return m_poPaxosNode->GetMaster(iGroupIdx);
}

const bool FSEditlog::IsIMMaster(const string & sKey)
{
    int iGroupIdx = GetGroupIdx(sKey);


//     change this bcz we need each node can propose
//     but need master to do propose at the initial stage

    if(enableMaster){
        return m_poPaxosNode->IsIMMaster(iGroupIdx);
    } else{
        return true;
    }

//    return true;
}

const bool FSEditlog::IsIMMaster() {
    if (m_poPaxosNode == nullptr) {
        return true;
    }
    return m_poPaxosNode->IsIMMaster(0);
}

const void FSEditlog::showmembership() {
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

const void FSEditlog::addmember(phxpaxos::NodeInfo newmember) {
    if (m_poPaxosNode == nullptr) {
        return;
    }
    printf("Add member : nodeid %lu ip %s port %d\n",
           newmember.GetNodeID(), newmember.GetIP().c_str(), newmember.GetPort());
    m_poPaxosNode->AddMember(0, newmember);
}

const void FSEditlog::removemember(phxpaxos::NodeInfo rmmember) {
    if (m_poPaxosNode == nullptr) {
        return;
    }
    printf("Remove member : nodeid %lu ip %s port %d\n",
           rmmember.GetNodeID(), rmmember.GetIP().c_str(), rmmember.GetPort());
    m_poPaxosNode->RemoveMember(0, rmmember);
}
int FSEditlog::Propose(const string & sKey, const string & sPaxosValue, 
    PhxEditlogSMCtx & oEditlogSMCtx)
{
    int iGroupIdx = GetGroupIdx(sKey); //sKey 是目录, hash算法得到groupindex

    SMCtx oCtx;
    //smid must same to PhxEditlogSM.SMID().
    oCtx.m_iSMID = 1; //设置oCtx.m_iSMID为1，与我们刚刚编写的状态机的SMID()相对应，标识我们需要将这个请求送往SMID为1的状态机的Execute函数。
    oCtx.m_pCtx = (void *)&oEditlogSMCtx;

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
                          "namenode: 该节点已被集群剔除，不允许Propose, ret %d", ret);

            return ret;
        }else if(ret == PaxosTryCommitRet_Value_Size_TooLarge){
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno,
                          "namenode: 提议的值超过大小限制, ret %d", ret);

            return ret;
        }
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno, 
            "namenode: paxos propose fail, ret %d", ret);
		
        return ret;
    }

    return NGX_OK;
}

int FSEditlog::MakeLogStoragePath(std::string & sLogStoragePath)
{
    char sTmp[128] = {0};
    snprintf(sTmp, sizeof(sTmp), "%s/%s_%d", m_sPaxosLogPath.c_str(), 
		m_oMyNode.GetIP().c_str(), m_oMyNode.GetPort());

    sLogStoragePath = string(sTmp);

    if (NGX_ERROR == access(sLogStoragePath.c_str(), F_OK))
    {
        if (NGX_ERROR == mkdir(sLogStoragePath.c_str(),
			S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH))
        {       
            dfs_log_error(dfs_cycle->error_log, DFS_LOG_ALERT, errno, 
                "Create dir fail, path %s", sLogStoragePath.c_str());
			
            return NGX_ERROR;
        }       
    }

    return NGX_OK;
}

int FSEditlog::GetGroupIdx(const string & sKey)
{
    uint32_t iHashNum = 0;
	
    for (size_t i = 0; i < sKey.size(); i++)
    {
        iHashNum = iHashNum * 7 + ((int)sKey[i]);
    }

    return iHashNum % m_iGroupCount;
}

