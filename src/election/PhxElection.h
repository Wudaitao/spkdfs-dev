
#pragma once

#include "phxpaxos/node.h"
#include <string>
#include <vector>
#include "phxpaxos/options.h"
#include "GrouplistSM.h"

using namespace std;



class PhxElection
{
public:
   PhxElection(const phxpaxos::NodeInfo & oMyNode, phxpaxos::NodeInfoList  vecNodeList);
   ~PhxElection();

    int RunPaxos();

    const phxpaxos::NodeInfo GetMaster();

    const phxpaxos::NodeInfo GetMasterWithVersion(uint64_t & llVersion);

    const bool IsIMMaster();
    const void showmembership();
    const NodeInfoList& getMembers();
    const bool IsImInGroup();
    const void addmember(phxpaxos::NodeInfo newmember);
    const void removemember(phxpaxos::NodeInfo rmmember);
    static void OnMasterChange(const int iGroupIdx, const phxpaxos::NodeInfo & oNewMaster, const uint64_t llVersion);
    phxpaxos::NodeInfo& getMyNode();
    int Propose( const string & sPaxosValue,
                PhxGrouplistSMCtx & oGrouplistSMCtx);

private:
    int MakeLogStoragePath(std::string & sLogStoragePath);
    int GetGroupIdx();

private:
    phxpaxos::NodeInfo m_oMyNode;
    phxpaxos::NodeInfoList m_vecNodeList;
    phxpaxos::Node * m_poPaxosNode;
    PhxGrouplistSM m_oGrouplistSM;
};



