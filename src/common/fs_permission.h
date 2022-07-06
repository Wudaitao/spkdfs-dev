#ifndef FS_PERMISSION_H
#define FS_PERMISSION_H

#include "dfs_string.h"
#include "dfs_task_cmd.h"
#include "dfs_task.h"
//#include "nn_file_index.h"

#define NONE          0
#define EXECUTE       1
#define WRITE         2
#define WRITE_EXECUTE 3
#define READ          4
#define READ_EXECUTE  5
#define READ_WRITE    6
#define ALL           7
#define BLK_LIMIT 64


typedef struct fi_inode_s_
{
    char     key[KEY_LEN];
    uint64_t uid;
    short    permission;
    char     owner[OWNER_LEN];
    char     group[GROUP_LEN];
    uint64_t modification_time;
    uint64_t access_time;
    uint32_t is_directory:1;
    uint64_t length;
    uint64_t blk_size;
    short    blk_replication;
    uint64_t blks[BLK_LIMIT]; // 每块的hash
    uint32_t blk_seq; // 当前访问块 序列
    uint32_t total_blk; // 总的块数
    /*uint32_t t_blk_sz;
    uint32_t t_sub_sz;
    uint32_t t_help_sz;*/
} fi_inode_t_;

int check_permission(task_t *task, fi_inode_t_ *finode,
	short access, uchar_t *err);
int is_super(char user[], string_t *admin);
void get_permission(short permission, uchar_t *str);

#endif

