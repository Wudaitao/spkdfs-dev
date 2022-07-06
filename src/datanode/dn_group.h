//
// Created by ginux on 2020/5/31.
//

#ifndef NGXFS_DN_GROUP_H
#define NGXFS_DN_GROUP_H
#include <dfs_group.h>

extern DfsGroup * dn_group;
extern DfsGroup * old_dn_group;
extern DfsGroup * master_newest_group;
typedef enum{
    LEADERS_INIT,
    LEADERS_ADD,
    LEADERS_REMOVE,
    LEADERS_REPLACE
}change_op;
void group_dispatch_init(DfsGroup *dn_group_init);
void group_dispatch_init_asold();
DfsNode group_dispatch_add(const DfsNode& node);
bool group_handl_rm(DfsNode &rmnode,DfsNode * old_group_leader,DfsNode *new_group_leader);
void group_leaders_change(phxgrouplist::LeadersChangeMess_op_types op,std::vector<DfsNode> leader_change);
void group_master_change(DfsNode new_master);

//std::string  group_encode(DfsGroup* dn_group);
//void group_decode(DfsGroup* dn_group,char* data,int size);
//phxgrouplist::GroupList  group_encode_proto(DfsGroup* dn_group);
//void group_decode_proto(DfsGroup* dn_group,phxgrouplist::GroupList& gl);

//void store_persistent_grouplist();
//void load_persistent_grouplist();


#endif //NGXFS_DN_GROUP_H
