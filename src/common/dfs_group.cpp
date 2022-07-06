
#include <fstream>
#include "dfs_group.h"
#include "../../etc/config.h"
#include "dfs_utils.h"
#include "dfs_dbg.h"


Group::Group() = default;

Group::~Group() = default;

static bool compare(const DfsNode& left,const DfsNode& right);

std::vector<DfsNode> &Group::getNodeList(){
    return NodeList;
}

int Group::addNode(const DfsNode &node) {
    NodeList.push_back(node);
    return NGX_OK;
}

int Group::removeNodeFromIp(const std::string &nodeip) {
    auto iter = NodeList.begin();
    while (iter != NodeList.end()) {
        if ((*iter).getNodeIp() == nodeip) {
            iter=NodeList.erase(iter);
            dbg(nodeip);
            return NGX_OK;
        }
        iter++;
    }
    return NGX_ERROR;
}

const DfsNode &Group::getGroupLeader() const {
    return groupLeader;
}

void Group::setGroupLeader(const DfsNode &groupLeader) {
    Group::groupLeader = groupLeader;
}


DfsNode *Group::findNodeFromip(const std::string &nodeip) {
    auto iter = NodeList.begin();
    while (iter != NodeList.end()) {
        if ((*iter).getNodeIp() == nodeip) {
            return &(*iter);
        }
        iter++;

    }
    return nullptr;
}

bool Group::operator==(Group &group) {
    if (groupLeader == group.groupLeader) {
        if (NodeList == group.NodeList) {
            return true;
        }
    }

    return false;
}

bool Group::operator==(const Group &group) {
    if (groupLeader == group.groupLeader) {
        if (NodeList == group.NodeList) {
            return true;
        }
    }

    return false;
}

int Group::getGroupSize() {
    return NodeList.size();
}
int Group::getGroupStatus() const {
    return groupStatus;
}

void Group::setGroupStatus(int groupStatus) {
    Group::groupStatus = groupStatus;
}

void Group::sortByremain() {
    std::sort(this->NodeList.begin(),this->NodeList.end(),compare);
}




// dfs group

DfsGroup::DfsGroup() = default;
DfsGroup::DfsGroup(std::string str_name){
    name=str_name;
}
DfsGroup::DfsGroup(const DfsGroup* DG,std::string strname){
    groups.assign(DG->groups.begin(),DG->groups.end());
    setGroupMaster(DG->getGroupMaster());
    epoch=DG->epoch;
    name=strname;
}
DfsGroup::~DfsGroup() {
    pthread_mutex_destroy(&mutex);
};
DfsGroup *DfsGroup::m_instance = nullptr;

std::vector<Group> &DfsGroup::getGroups(){
    return groups;
}

int DfsGroup::addGroup(const Group &group) {
    groups.push_back(group);
    return NGX_OK;
}

int DfsGroup::removeGroup(const Group &group) {
    auto iter = groups.begin();
    while (iter != groups.end()) {
        if ((*iter) == group) {
            iter=groups.erase(iter);
            return NGX_OK;
        }
        iter++;
    }
    return NGX_ERROR;
}

Group *DfsGroup::getGroupFromLeaderIp(const std::string &nodeip) {
    auto iter = groups.begin();
    while (iter != groups.end()) {
        if ((*iter).getGroupLeader().getNodeIp() == nodeip) {
            return &*iter;
        }
        iter++;
    }
    return nullptr;
}

// 默认一个数据节点只属于一个群组
Group *DfsGroup::findGroupFromNodeIp(const std::string &nodeip) {
    auto iter = groups.begin();
    while (iter != groups.end()) {
        DfsNode *node = nullptr;
        node = (*iter).findNodeFromip(nodeip);
        if (node != nullptr) {
            return &*iter;
        }
        iter++;
    }
    return nullptr;
}

const DfsNode &DfsGroup::getGroupMaster() const {
    return groupMaster;
}

void DfsGroup::setGroupMaster(const DfsNode &groupMaster) {
    DfsGroup::groupMaster = groupMaster;
}

int DfsGroup::getGlobalStatus() const {
    return globalStatus;
}

void DfsGroup::setGlobalStatus(int globalStatus) {
    DfsGroup::globalStatus = globalStatus;
}


int DfsGroup::getGroupNodeNum() {
    auto iter = groups.begin();
    int totalNum = 0;
    while (iter != groups.end()) {
        totalNum += (*iter).getGroupSize();
        iter++;
    }
    return totalNum;
}

void DfsGroup::lockGroup() {
    pthread_mutex_lock(&mutex);
}

void DfsGroup::unlockGroup() {
    pthread_mutex_unlock(&mutex);
}

int DfsGroup::checkAddrInNodeList(const std::string &nodeip, const int &port) {
    auto iter = NodeList.begin();
    while (iter != NodeList.end()) {
        if (iter->getNodeIp() == nodeip) { //  && iter->getNodePort() == port
            return NGX_OK;
        }
        iter++;
    }
    return NGX_ERROR;
}

int DfsGroup::addAddrToNodeList(const std::string &nodeip, const int &port) {
    DfsNode node;
    node.setNodeIp(nodeip);
    node.setNodePort(port);
    NodeList.push_back(node);
    return NGX_OK;
}

int DfsGroup::forceAddToNodeList(const std::string &nodeip, const int &port) {
    if (checkAddrInNodeList(nodeip, port) == NGX_ERROR) {
        return addAddrToNodeList(nodeip, port);
    }

    return NGX_OK;
}

int DfsGroup::rmAddrInNodeList(const std::string &nodeip, const int &port) {
    auto iter = NodeList.begin();
    while (iter != NodeList.end()) {
        if (iter->getNodeIp() == nodeip) { //&& iter->getNodePort() == port
            iter=NodeList.erase(iter);
            return NGX_OK;
        }
        iter++;
    }
    return NGX_ERROR;
}

const std::vector<DfsNode> &DfsGroup::getNodeList() const {
    return NodeList;
}

const DfsNode &DfsGroup::getOwn() const {
    return Own;
}

void DfsGroup::setOwn(const std::string &nodeip, const int &port) {
    Own.setNodeIp(nodeip);
    Own.setNodePort(port);
}

void DfsGroup::condWait() {
    DFS_COND_WAIT(cond, mutex);
}

void DfsGroup::condSignal() {
    DFS_COND_SIGNAL(cond);
}

// clear all data
int DfsGroup::resetNodeList() {
    std::vector<DfsNode>().swap(NodeList);
    return NGX_OK;
}

void DfsGroup::printGroup() {
    printf("Print %s : version:%4d\n",name.c_str(),epoch);
    for (int i = 0; i < groups.size(); ++i) {
        printf("group :%3d , leader ip: %s\n", i + 1, groups[i].getGroupLeader().getNodeIp().c_str());

        printf("members : \n");
        for (int j = 0; j < groups[i].getGroupSize(); ++j) {
            printf(" %s ", groups[i].getNodeList()[j].getNodeIp().c_str());
        }
        printf("\n\n");
    }
}
void DfsGroup::dbgGroup() {
    char buffer[100]={0};
    sprintf(buffer,"Print %s : version:%4d",name.c_str(),epoch);
    dbg(buffer);
    for (int i = 0; i < groups.size(); ++i) {
        sprintf(buffer,"group :%3d , leader ip: %s", i + 1, groups[i].getGroupLeader().getNodeIp().c_str());
        dbg(buffer);
        sprintf(buffer,"members : ");
        dbg(buffer);
        for (int j = 0; j < groups[i].getGroupSize(); ++j) {
            sprintf(buffer,"%s", groups[i].getNodeList()[j].getNodeIp().c_str());
            dbg(buffer);
        }
        sprintf(buffer,"-----------------");
        dbg(buffer);
    }
}

void DfsGroup::clear() {
    std::vector<Group>().swap(groups);
}

std::vector<DfsNode> DfsGroup::getleaders() {
    std::vector<DfsNode> leaders;
    for (auto & group : groups) {
        leaders.push_back(group.getGroupLeader());
    }
    return leaders;
}

// get all the paxos:ip:port
std::string DfsGroup::getleadersstring() {
    std::string leaders;
    int gsize = groups.size();
    for (int i = 0; i < gsize; ++i) {
        leaders += (groups[i].getGroupLeader().getNodeIp() + ':' +
                    std::to_string(groups[i].getGroupLeader().getNodePort()));
        if (i != gsize - 1) {
            leaders += ',';
        }
    }
    return leaders;
}

std::string DfsGroup::get_ns_leaders_string() {
    std::string leaders;
    int gsize = groups.size();
    for (int i = 0; i < gsize; ++i) {
        leaders += (groups[i].getGroupLeader().getNodeIp() + ':' +
                    std::to_string(NN_PAXOS_PORT));
        if (i != gsize - 1) {
            leaders += ',';
        }
    }
    return leaders;
}
phxgrouplist::GroupList DfsGroup::encodeToProtobufMess(){
    phxgrouplist::GroupList gl;
    gl.set_epoch(epoch);
    gl.mutable_master()->set_nodeport(this->getGroupMaster().getNodePort());
    gl.mutable_master()->set_nodeip(this->getGroupMaster().getNodeIp());
    for(auto itr_group:this->getGroups()){
        auto new_group=gl.add_groups();
        new_group->mutable_leader()->set_nodeport(itr_group.getGroupLeader().getNodePort());
        new_group->mutable_leader()->set_nodeip(itr_group.getGroupLeader().getNodeIp());
        for(auto itr_node:itr_group.getNodeList()){
            auto group_node=new_group->add_nodelist();
            group_node->set_nodeip(itr_node.getNodeIp());
            group_node->set_nodeport(itr_node.getNodePort());
        }
    }
    return gl;
}
void DfsGroup::decodeFromProtobufMess(phxgrouplist::GroupList &gl){
    epoch=gl.epoch();
    this->setGroupMaster(DfsNode(gl.master()));
    for(int i=0;i<gl.groups_size();i++){
        auto group_cur=gl.groups(i);
        Group new_group;
        for(int j=0;j< group_cur.nodelist_size();j++){
            DfsNode new_node(group_cur.nodelist(j));
            new_group.addNode(new_node);
        }
        new_group.setGroupLeader(DfsNode(group_cur.leader()));
        this->addGroup(new_group);
    }
}
std::string DfsGroup::encodeToString(){
    std::string sPaxosValue;
    encodeToProtobufMess().SerializeToString(&sPaxosValue);
    return sPaxosValue;
}
void DfsGroup::decodeFromCharArray(char* data,int size){
    this->clear();
    phxgrouplist::GroupList gl;
    gl.ParseFromArray(data,size);
    decodeFromProtobufMess(gl);
}
void DfsGroup::store_persistent_grouplist(){
    std::string persistent_gl_file;
    char sTmp[128] = {0};
    std::string prefix = expand_user("~") +  PREFIX;
    snprintf(sTmp, sizeof(sTmp), "%s/data/datanode/persistent_data/grouplist", prefix.c_str());

    persistent_gl_file = std::string(sTmp);
    std::fstream output(persistent_gl_file, std::ios::out | std::ios::binary |std::ios_base::trunc);
    phxgrouplist::GroupList gl=this->encodeToProtobufMess();
    gl.SerializeToOstream(&output);
    std::cout<<"store_persistent_grouplist finish!"<<std::endl;
}
bool DfsGroup::load_persistent_grouplist(){
    std::string persistent_gl_file;
    char sTmp[128] = {0};
    std::string prefix = expand_user("~") +  PREFIX;

    snprintf(sTmp, sizeof(sTmp), "%s/data/datanode/persistent_data/grouplist", prefix.c_str());
    persistent_gl_file = std::string(sTmp);
    std::fstream input(persistent_gl_file, std::ios::in | std::ios::binary);
    if(!input){
        return false;
    }
    phxgrouplist::GroupList gl;
    gl.ParseFromIstream(&input);
    this->decodeFromProtobufMess(gl);
    std::cout<<"load_persistent_grouplist finish!"<<std::endl;
    return true;
}
void DfsGroup::epochIncrease(){
    epoch++;
}
bool DfsGroup::isLeader(){
    for (auto & group : groups) {
        if(getOwn()== group.getGroupLeader()){
            return true;
        }
    }
    return false;
}
bool DfsGroup::isContain(DfsNode node){
    for (auto & group : groups) {
        if(group.findNodeFromip(node.getNodeIp())!= nullptr){
            return true;
        }
    }
    return false;
}


static bool compare(const DfsNode& left,const DfsNode& right){
    return left.getRemaining() > right.getRemaining(); //jiang序排列
}


