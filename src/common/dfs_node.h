
#ifndef NGXFS_DFS_NODE_H
#define NGXFS_DFS_NODE_H
#include "GroupListMess.pb.h"
#include <string>

class DfsNode{
public:
    DfsNode();
    explicit DfsNode(phxgrouplist::NodeInfo);
    DfsNode(std::string ip,int port);
    virtual ~DfsNode();

public:
    bool operator==(DfsNode &node);
    bool operator==(const DfsNode &node);

    bool operator==(const DfsNode &node) const;

    bool operator<(const DfsNode &node) const;

    const std::string &getNodeIp() const;

    void setNodeIp(const std::string &NodeIp);

    int getNodePort() const;

    void setNodePort(int NodePort);


private:
    int         nodePort{};
    std::string nodeIp{};
    uint64_t capacity;// total
    uint64_t dfs_used; //
    uint64_t remaining; //

public:
    uint64_t getCapacity() const;

    void setCapacity(uint64_t capacity);

    uint64_t getDfsUsed() const;

    void setDfsUsed(uint64_t dfsUsed);

    uint64_t getRemaining() const;

    void setRemaining(uint64_t remaining);

};

#endif //NGXFS_DFS_NODE_H

