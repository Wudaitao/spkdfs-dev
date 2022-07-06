//
// Created by ginux on 2020/5/28.
//

#ifndef NGXFS_NN_GROUP_H
#define NGXFS_NN_GROUP_H

#include <dfs_group.h>
#include <dfs_task.h>

extern DfsGroup * nn_group;
int grouplist_change(task_t* task);
int check_nodestatus(task_t* task);
int leaders_change(std::vector<DfsNode> old_leaders);
int dn_master_change(task_t* task);

#endif //NGXFS_NN_GROUP_H
