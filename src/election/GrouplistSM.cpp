

#include <dn_paxos_handler.h>
#include <dfs_dbg.h>
#include <dn_group.h>
#include "dfs_types.h"
#include "GrouplistSM.h"
#include "dn_cycle.h"

#include "dfs_error_log.h"



PhxGrouplistSM::PhxGrouplistSM() : m_llCheckpointInstanceID(NoCheckpoint)
{
}

PhxGrouplistSM::~PhxGrouplistSM()
{
}

// propose 后执行excute
bool PhxGrouplistSM::Execute(const int iGroupIdx, const uint64_t llInstanceID, 
	const string & sPaxosValue, SMCtx * poSMCtx)
{
    dfs_log_error(dfs_cycle->error_log, DFS_LOG_DEBUG, 0, 
        "[SM Execute] ok, smid: %d, instanceid: %lu, value: %s", 
        SMID(), llInstanceID, sPaxosValue.c_str());
    dbg("Execute");
    if (poSMCtx != nullptr && poSMCtx->m_pCtx != nullptr)
    {
        PhxGrouplistSMCtx * poPhxGrouplistSMCtx = (PhxGrouplistSMCtx *)poSMCtx->m_pCtx; // 继承 SMCtx
        poPhxGrouplistSMCtx->iExecuteRet = NGX_OK;
        poPhxGrouplistSMCtx->llInstanceID = llInstanceID; // 提议的值
        // paxos handler
        group_paxos_handler(llInstanceID, sPaxosValue);
    }
	else 
	{
        group_paxos_handler(llInstanceID, sPaxosValue);
	}
    //printf("[SM Execute] ok, smid: %d, instanceid: %lu, value: %s\n",MID(), llInstanceID, sPaxosValue.c_str());
    return NGX_TRUE;
}

const int PhxGrouplistSM::SMID() const 
{ 
    return 1; 
}

const uint64_t PhxGrouplistSM::GetCheckpointInstanceID(const int iGroupIdx) const
{
    return m_llCheckpointInstanceID;
}

int PhxGrouplistSM::SyncCheckpointInstanceID(const uint64_t llInstanceID)
{
    m_llCheckpointInstanceID = llInstanceID;

    return NGX_OK;
}

