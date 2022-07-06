//
// Created by gogobody on 2020/5/22.
//

#include <dfs_types.h>
#include "dfs_node.h"

DfsNode::DfsNode() = default;

DfsNode::~DfsNode() = default;
DfsNode::DfsNode(phxgrouplist::NodeInfo nodeInfo){
    nodeIp=nodeInfo.nodeip();
    nodePort=nodeInfo.nodeport();
}
DfsNode::DfsNode(std::string ip,int port){
    nodeIp=ip;
    nodePort=port;
}
const std::string &DfsNode::getNodeIp() const {
    return nodeIp;
}

void DfsNode::setNodeIp(const std::string &NodeIp) {
    nodeIp = NodeIp;
}

int DfsNode::getNodePort() const {
    return nodePort;
}

void DfsNode::setNodePort(int NodePort) {
    nodePort = NodePort;
}

bool DfsNode::operator==(DfsNode &node) {
    return nodeIp==node.nodeIp;
}

bool DfsNode::operator==(const DfsNode &node) {
    return nodeIp==node.nodeIp;
}

bool DfsNode::operator==(const DfsNode &node) const {
    return nodeIp==node.nodeIp;
}
bool DfsNode::operator<(const DfsNode &node) const{
    return nodeIp<node.nodeIp;
}

uint64_t DfsNode::getCapacity() const {
    return capacity;
}

void DfsNode::setCapacity(uint64_t capacity) {
    DfsNode::capacity = capacity;
}

uint64_t DfsNode::getDfsUsed() const {
    return dfs_used;
}

void DfsNode::setDfsUsed(uint64_t dfsUsed) {
    dfs_used = dfsUsed;
}

uint64_t DfsNode::getRemaining() const {
    return remaining;
}

void DfsNode::setRemaining(uint64_t remaining) {
    DfsNode::remaining = remaining;
}

