//
// Created by llp on 6/5/20.
//

#ifndef NGXFS_DN_PAXOS_HANDLER_H
#define NGXFS_DN_PAXOS_HANDLER_H


#include <cstdint>
#include <string>
#include "GroupListMess.pb.h"
using namespace phxgrouplist;
int group_paxos_handler(const uint64_t llInstanceID,
                        const std::string &sPaxosValue);

int update_gl_init(const GroupListInit& gl_init);

static int update_gl_add(const GrouplistAdd &gl_add);

static int update_gl_remove(const GroupListRemove &gl_remove);

static int update_gl_killnode(const GroupListRemove &gl_remove);

void gl_shutdown();

#endif //NGXFS_DN_PAXOS_HANDLER_H
