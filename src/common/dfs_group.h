
#ifndef NGXFS_DFS_GROUP_H
#define NGXFS_DFS_GROUP_H
#include <dfs_types.h>
#include <dfs_node.h>
#include <vector>

enum {
    GROUP_BUILD = 100, // init
    PAXOS_BUILD,
    THREAD_BUILD,
    GROUP_FINISH,      // finish
    GROUP_SHOTDOWN,

};

#define DFS_COND_SIGNAL(cond)             pthread_cond_signal (&(cond))
#define DFS_COND_WAIT(cond,mutex)         pthread_cond_wait (&(cond), &(mutex))

class Group{
private:
    std::vector<DfsNode> NodeList;
    DfsNode groupLeader;
    int     groupStatus;


public:
    int getGroupStatus() const;

    void setGroupStatus(int groupStatus);

public:
    const DfsNode &getGroupLeader() const;

    void setGroupLeader(const DfsNode &groupLeader);

public:
    Group();
    virtual ~Group();
    bool operator==(Group &group);
    bool operator==(const Group &group);

    std::vector<DfsNode> &      getNodeList() ;
    int                         getGroupSize();
    int                         addNode(const DfsNode &node);
    int                         removeNodeFromIp(const std::string &nodeip);
    DfsNode *                   findNodeFromip(const std::string &nodeip);
    void                        sortByremain();


};


class DfsGroup{

public:
    DfsGroup();
    DfsGroup(std::string name);
    DfsGroup(const DfsGroup* DG,std::string strname);
    ~DfsGroup();
    static DfsGroup * getInstance(std::string name){
        if(m_instance== nullptr){
            m_instance = new DfsGroup(name);
        }
        return m_instance;
    }

private:
    static DfsGroup * m_instance;
public:
    std::string name;
    std::vector<DfsNode> NodeList; // default only store ip
    // all the node lists
    std::vector<Group> groups;
    const std::vector<DfsNode> &getNodeList() const;
    std::vector<DfsNode> oldleaders;
    std::set<DfsNode> AliveNodeList;
    std::vector<DfsNode> initNodeList;
    // own Node
    DfsNode Own;
    int   epoch=0;
    bool    start_shutdown=false;

private:

    // the group has a real master , and the groupLeader is the namenode.
    DfsNode groupMaster;
    // the whold group status
    int     globalStatus = GROUP_BUILD; // DEFAULT NONE



public:
    const DfsNode &getOwn() const;

    void setOwn(const std::string& nodeip,const int &port);

private:
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t  cond = PTHREAD_COND_INITIALIZER;

public:
    int getGlobalStatus() const;

    void setGlobalStatus(int globalStatus);

public:
    const DfsNode &getGroupMaster() const;

    void setGroupMaster(const DfsNode &groupMaster);


public:
    void                      lockGroup();
    void                      unlockGroup();
    void                      condWait();
    void                      condSignal();
    std::vector<Group>&       getGroups();
    int                       getGroupNodeNum();
    int                       addGroup(const Group& group);
    int                       removeGroup(const Group& group);
    Group *                   getGroupFromLeaderIp(const std::string& nodeip);
    Group *                   findGroupFromNodeIp(const std::string& nodeip);
    void                      printGroup();
    void                      dbgGroup();
    void                      clear();
    std::vector<DfsNode>      getleaders();
    std::string               getleadersstring();
    std::string               get_ns_leaders_string();
    phxgrouplist::GroupList   encodeToProtobufMess();
    std::string               encodeToString();
    void                      decodeFromProtobufMess(phxgrouplist::GroupList &gl);
    void                      decodeFromCharArray(char* data,int size);
    void                      store_persistent_grouplist();
    bool                      load_persistent_grouplist();
    bool                      isLeader();
    bool                      isContain(DfsNode node);

    void                      epochIncrease();

    //
    int                      checkAddrInNodeList(const std::string& nodeip,const int &port);
    int                      addAddrToNodeList(const std::string& nodeip,const int &port);
    int                      forceAddToNodeList(const std::string& nodeip,const int &port);
    int                      rmAddrInNodeList(const std::string& nodeip,const int &port);
    int                      resetNodeList();// clear all data
};



#endif //NGXFS_DFS_GROUP_H
