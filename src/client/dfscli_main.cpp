#include <cstdio>
#include <cstring>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <fcntl.h>
#include <unistd.h>
#include <pwd.h>
#include <grp.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <filetool.h>
#include <dfs_utils.h>
#include <dfs_group.h>
#include <sys/ioctl.h>
#include <libnet.h>
#include <dfs_dbg.h>
#include "dfscli_main.h"
#include "dfs_string.h"
#include "fs_permission.h"
#include "../../etc/config.h"
#include "dfscli_conf.h"
#include "dfscli_put.h"
#include "dfscli_get.h"
#include "cli_group.h"
#include "dfscli_cycle.h"


#define INVALID_SYMBOLS_IN_PATH "\\:*?\"<>|"
#define MY_LOG_RAW (1 << 10) // Modifier to log without timestamp

std::string DEFAULT_CONF_FILE;

#define DEFAULT_COUNT 5 // 默认文件切5个
#define NAME_NODE_PORT 8000

string_t config_file;
extern clicycle_t         *dfscli_cycle;
using namespace std;

static void log_raw(uint32_t level, const char *msg);

static void help(int argc, char **argv);

static int dfscli_mkdir(char *path);

static int dfscli_rmr(char *path);

static int dfscli_ls(char *path);

static int showDirsFiles(char *p, int len);

static int getTimeStr(uint64_t msec, char *str, int len);

static int isPathValid(char *path);

static int getValidPath(char *src, char *dst);

static int dfscli_rm(char *path);

static int dfscli_killall();

static int dfscli_kill(char *path);

static int dfscli_printGroup();

int dfscli_printGroup() {
    conf_server_t *sconf = nullptr;
    sconf = (conf_server_t *) dfscli_cycle->sconf;
    server_bind_t *nn_addr = nullptr;

    int res;
    char localIp[INET_ADDRSTRLEN];
    if (cli_get_local_ip(localIp) == NGX_ERROR) {
        dfscli_log(DFS_LOG_ERROR, "cli_get_local_ip error");
        return NGX_ERROR;
    }

    int sockfd = dfs_connect(localIp, DN_PORT);
    if (sockfd < 0) {
        dfscli_log(DFS_LOG_ERROR, "update_group_from_local dfs_connect error");
        close(sockfd);
        return NGX_ERROR;
    }

    data_transfer_header_t header;
    memset(&header, 0x00, sizeof(data_transfer_header_t));
    header.op_type = OP_CLI_UPDATE_GROUP_FROM_DN;

    res = send(sockfd, &header, sizeof(data_transfer_header_t), 0);
    if (res < 0) {
        dfscli_log(DFS_LOG_WARN, "send header to %s err, %s",
                   localIp, strerror(errno));
        return NGX_ERROR;
    }

    // then recv
    int pLen = 0;
    int rLen = recv(sockfd, &pLen, sizeof(int), MSG_PEEK);
    if (rLen < 0) {
        dfscli_log(DFS_LOG_FATAL,
                   "update_group_from_local recv err, rLen: %d", rLen);

        close(sockfd);

        return NGX_ERROR;
    }

    char *pNext = (char *) malloc(pLen);
    if (nullptr == pNext) {
        dfscli_log(DFS_LOG_FATAL,
                   "update_group_from_local malloc err, pLen: %d", pLen);

        close(sockfd);

        return NGX_ERROR;
    }

    rLen = read(sockfd, pNext, pLen);
    if (rLen < 0) {
        dfscli_log(DFS_LOG_FATAL,
                   "update_group_from_local read err, rLen: %d", rLen);

        close(sockfd);

        free(pNext);
        pNext = nullptr;

        return NGX_ERROR;
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(pNext, rLen, &in_t);

    if (in_t.ret != OP_STATUS_SUCCESS) {
        dfscli_log(DFS_LOG_FATAL, "dn_group build didnt finish , plz wait or check program status, ret: %d", in_t.ret);

        close(sockfd);

        free(pNext);
        pNext = nullptr;

        return NGX_ERROR;
    } else if (nullptr != in_t.data && in_t.data_len > 0) {
        cli_group->decodeFromCharArray(static_cast<char *>(in_t.data), in_t.data_len);
        if (!cli_group->groups.empty()) {
            printf("master %s\n", cli_group->getGroupMaster().getNodeIp().c_str());
            cli_group->printGroup();

            Group *tgroup = cli_group->findGroupFromNodeIp(localIp);
            if(!tgroup){
                dfscli_log(DFS_LOG_ERROR,"cli get group error");
                close(sockfd);
                return NGX_ERROR;
            }
            std::string myleader = tgroup->getGroupLeader().getNodeIp();
            nn_addr = (server_bind_t *) sconf->namenode_addr.elts;
            strcpy(reinterpret_cast<char *>(nn_addr[0].addr.data), myleader.c_str());
            strcpy(dfscli_cycle->namenode_addr,myleader.c_str());
            nn_addr[0].addr.len = myleader.length();
            nn_addr[0].port = NAME_NODE_PORT;

        }
        close(sockfd);
        return NGX_OK;
    }
    close(sockfd);
    dfscli_log(DFS_LOG_FATAL,
               "get group from datanode failed, ret: %d", in_t.ret);
    return NGX_ERROR;
}

int dfscli_daemon() {
    return 0;
}

void dfscli_log(int level, const char *fmt, ...) {
    va_list ap;
    char msg[LOGMSG_LEN] = "";
    conf_server_t *sconf = nullptr;

    sconf = (conf_server_t *) dfscli_cycle->sconf;

    if ((level & 0xff) > sconf->log_level) {
        return;
    }

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    log_raw(level, msg);
}

static void log_raw(uint32_t level, const char *msg) {
    const char *c = "#FEAWNICED";
    conf_server_t *sconf = nullptr;

    sconf = (conf_server_t *) dfscli_cycle->sconf;

    int rawmode = (level & MY_LOG_RAW);

    level &= 0xff; // clear flags
    if (level > sconf->log_level) {
        return;
    }

    FILE *fp = (0 == sconf->error_log.len)
               ? stdout : fopen((const char *) sconf->error_log.data, "a");
    if (nullptr == fp) {
        return;
    }

    if (rawmode) {
        fprintf(fp, "%s", msg);
    } else {
        int off = 0;
        char buf[64] = "";
        struct timeval tv{};

        gettimeofday(&tv, nullptr);
        off = strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S.",
                       localtime(&tv.tv_sec));
        snprintf(buf + off, sizeof(buf) - off, "%03d", (int) tv.tv_usec / 1000);

        switch (level) {
            case DFS_LOG_WARN:
                fprintf(fp, "%s[%ld] %s [%c] %s%s\n", ANSI_YELLOW,
                        syscall(__NR_gettid), buf, c[level], msg, ANSI_RESET);
                break;

            case DFS_LOG_ERROR:
                fprintf(fp, "%s[%ld] %s [%c] %s%s\n", ANSI_RED,
                        syscall(__NR_gettid), buf, c[level], msg, ANSI_RESET);
                break;

            default:
                fprintf(fp, "[%ld] %s [%c] %s\n",
                        syscall(__NR_gettid), buf, c[level], msg);
        }
    }

    fflush(fp);

    if (0 != sconf->error_log.len) {
        fclose(fp);
    }
}

static void help(int argc, char **argv) {
    fprintf(stderr, "Usage: %s cmd...\n"
                    "\t tip: if you use [-cutput local/file remote/file] then the file in remote will be stored in \n"
                    "\t file1,file2,...,file5 (default is 5), and then if you use [-merget remote/file local/file],that \n"
                    "\t (file1,...,file5 in remote) will be download and merge into local/file\n"
                    "\t -mkdir <path> \n"
                    "\t -rmr <path> \n"
                    "\t -ls <path> \n"
                    "\t -put <local path> <remote path> \n"
                    "\t -get <remote path> <local path> \n"
                    "\t -rm <path> \n"
                    "\t -cutput <local path> <remote path>  \n"
                    "\t -merget <remote path> <local path>  \n"
                    "\t -kill <node ip> \n"
                    "\t -killall \n"
                    "\t -pgroup \n",

            argv[0]);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        help(argc, argv);

        return NGX_ERROR;
    }

    int ret = NGX_OK; //0
    clicycle_t *cycle = nullptr;
    conf_server_t *sconf = nullptr;
    char cmd[16] = {0};
    char path[PATH_LEN] = {0};

    cycle = cycle_create();
    cli_group = DfsGroup::getInstance("cli_group");
    std::string prefix = expand_user("~") + PREFIX;
    DEFAULT_CONF_FILE  = prefix + "/etc/dfscli.conf";

    if (config_file.data == nullptr) {
        config_file.data = (uchar_t *) strndup(DEFAULT_CONF_FILE.c_str(),
                                               strlen(DEFAULT_CONF_FILE.c_str()));
        config_file.len = strlen(DEFAULT_CONF_FILE.c_str());
    }

    if ((ret = cycle_init(cycle)) != NGX_OK) {
        fprintf(stderr, "cycle_init fail\n");

        goto out;
    }
    //
    if (update_group_from_local() == NGX_ERROR) {
        fprintf(stderr, "update_group_from_local fail\n");
        goto out;
    }

    //
    strcpy(cmd, argv[1]);

    if (argc > 2 && strlen(argv[2]) > (int) PATH_LEN) {
        dfscli_log(DFS_LOG_WARN, "path's len is greater than %d",
                   (int) PATH_LEN);

        goto out;
    }
    if (argc > 2) {
        strcpy(path, argv[2]);
    }

    if (0 == strncmp(cmd, "-mkdir", strlen("-mkdir"))) {
        if (!isPathValid(path)) {
            dfscli_log(DFS_LOG_WARN,
                       "path[%s] is invalid, these symbols[%s] can't use in the path",
                       path, INVALID_SYMBOLS_IN_PATH);

            goto out;
        }

        char vPath[PATH_LEN] = {0};
        getValidPath(path, vPath);

        dfscli_mkdir(vPath);
    } else if (0 == strncmp(cmd, "-rmr", strlen("-rmr"))) {
        // check path's pattern

        char vPath[PATH_LEN] = {0};
        getValidPath(path, vPath);

        dfscli_rmr(vPath);
    } else if (0 == strncmp(cmd, "-ls", strlen("-ls"))) {
        // check path's pattern

        char vPath[PATH_LEN] = {0};
        getValidPath(path, vPath);

        dfscli_ls(vPath);
    } else if (4 == argc && 0 == strncmp(cmd, "-put", strlen("-put"))) //put local path ,remote path
    {
        char tmp[PATH_LEN] = {0};
        strcpy(tmp, argv[3]);

        char src[PATH_LEN] = {0};
        getValidPath(path, src);

        char dst[PATH_LEN] = {0};
        getValidPath(tmp, dst);
//        dbg(src);
        dfscli_put(src, dst, 1, 1);
    } else if (4 == argc && 0 == strncmp(cmd, "-get", strlen("-get"))) {
        //char tmp[PATH_LEN] = {0};
        //strcpy(tmp, argv[3]);

        char src[PATH_LEN] = {0};
        getValidPath(path, src);

        char dst[PATH_LEN] = {0};
        //getValidPath(tmp, dst);
        strcpy(dst, argv[3]);
        int blk_num = 0;
        dfscli_get(src, dst, &blk_num);
    } else if (0 == strncmp(cmd, "-rm", strlen("-rm"))) {
        // check path's pattern

        char vPath[PATH_LEN] = {0};
        getValidPath(path, vPath);

        dfscli_rm(vPath);
    } else if (0 == strncmp(cmd, "-cutput", strlen("-cutput"))) {
        char tmp[PATH_LEN] = {0};
        strcpy(tmp, argv[3]);

        char src[PATH_LEN] = {0};
        getValidPath(path, src);

        char dst[PATH_LEN] = {0};
        getValidPath(tmp, dst);

        // split and send file
        int count = DEFAULT_COUNT; //
        splitFile(src, count, src);

        // name start from "xxxx1,xxxx2 ...."
        for (int i = 1; i <= count; i++) {
            char splitedfilename[256];
            char spliteddstname[256];
            sprintf(splitedfilename, "%s_%d", src, i);
            sprintf(spliteddstname, "%s_%d", dst, i);
            dfscli_put(splitedfilename, spliteddstname, i, count);

        }

    } else if (4 == argc && 0 == strncmp(cmd, "-merget", strlen("-merget"))) {
        //char tmp[PATH_LEN] = {0};
        //strcpy(tmp, argv[3]);

        // remote path
        char src[PATH_LEN] = {0};
        getValidPath(path, src);

        // local path
        char dst[PATH_LEN] = {0};
        //getValidPath(tmp, dst);
        strcpy(dst, argv[3]);
        int blk_num = 0;
        int nret = 0;
        for (int i = 1; i <= DEFAULT_COUNT; i++) {
            char splitedfilename[256]; // remote
            char spliteddstname[256]; // local
            sprintf(splitedfilename, "%s_%d", src, i);
            sprintf(spliteddstname, "%s_%d", dst, i);
            nret = dfscli_get(splitedfilename, spliteddstname, &blk_num);
            if(nret == NGX_ERROR){
                break;
            }
        }
        if (blk_num == DEFAULT_COUNT) {
            dfscli_log(DFS_LOG_ERROR,
                       "DEFAULT_COUNT is 5, remote blk_num is %d, check if the blk num match\n",
                       blk_num);
            return NGX_ERROR;
        }
        mergeFile(dst, DEFAULT_COUNT, dst);
    } else if (0 == strncmp(cmd, "-kill", strlen(cmd))) {
        // check path's pattern

        dfscli_kill(path);
    } else if (0 == strncmp(cmd, "-killall", strlen(cmd))) {
        dfscli_killall();

    }else if (0 == strncmp(cmd, "-pgroup", strlen(cmd))) {
        dfscli_printGroup();

    } else {
        help(argc, argv);
    }

    out:
    if (config_file.data) {
        free(config_file.data);
        config_file.len = 0;
    }

    if (cycle) {
        cycle_free(cycle);
        cycle = nullptr;
    }

    return ret;
}

int dfs_connect(char *ip, int port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (-1 == sockfd) {
        dfscli_log(DFS_LOG_WARN, "socket() err: %s", strerror(errno));

        return NGX_ERROR;
    }

    int reuse = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in servaddr{};
    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    servaddr.sin_addr.s_addr = inet_addr(ip);

    int iRet = connect(sockfd, (struct sockaddr *) &servaddr, sizeof(servaddr));
    if (iRet < 0) {
        dfscli_log(DFS_LOG_WARN, "connect to %s:%d err: %s",
                   ip, port, strerror(errno));


        return NGX_ERROR;
    }

    return sockfd;
}


void getUserInfo(task_t *out_t) {
    struct passwd *passwd;
    passwd = getpwuid(getuid());
    strcpy(out_t->user, passwd->pw_name);
    struct group *group;
    group = getgrgid(passwd->pw_gid);
    strcpy(out_t->group, group->gr_name);

}

//
int update_group_from_local() {
    conf_server_t *sconf = nullptr;
    sconf = (conf_server_t *) dfscli_cycle->sconf;
    server_bind_t *nn_addr = nullptr;

    int res;
    char localIp[INET_ADDRSTRLEN];
    if (cli_get_local_ip(localIp) == NGX_ERROR) {
        dfscli_log(DFS_LOG_ERROR, "cli_get_local_ip error");
        return NGX_ERROR;
    }

    int sockfd = dfs_connect(localIp, DN_PORT);
    if (sockfd < 0) {
        dfscli_log(DFS_LOG_ERROR, "update_group_from_local dfs_connect error");
        close(sockfd);
        return NGX_ERROR;
    }

    data_transfer_header_t header;
    memset(&header, 0x00, sizeof(data_transfer_header_t));
    header.op_type = OP_CLI_UPDATE_GROUP_FROM_DN;

    res = send(sockfd, &header, sizeof(data_transfer_header_t), 0);
    if (res < 0) {
        dfscli_log(DFS_LOG_WARN, "send header to %s err, %s",
                   localIp, strerror(errno));
        return NGX_ERROR;
    }

    // then recv
    int pLen = 0;
    int rLen = recv(sockfd, &pLen, sizeof(int), MSG_PEEK);
    if (rLen < 0) {
        dfscli_log(DFS_LOG_FATAL,
                   "update_group_from_local recv err, rLen: %d", rLen);

        close(sockfd);

        return NGX_ERROR;
    }

    char *pNext = (char *) malloc(pLen);
    if (nullptr == pNext) {
        dfscli_log(DFS_LOG_FATAL,
                   "update_group_from_local malloc err, pLen: %d", pLen);

        close(sockfd);

        return NGX_ERROR;
    }

    rLen = read(sockfd, pNext, pLen);
    if (rLen < 0) {
        dfscli_log(DFS_LOG_FATAL,
                   "update_group_from_local read err, rLen: %d", rLen);

        close(sockfd);

        free(pNext);
        pNext = nullptr;

        return NGX_ERROR;
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(pNext, rLen, &in_t);

    if (in_t.ret != OP_STATUS_SUCCESS) {
        dfscli_log(DFS_LOG_FATAL, "dn_group build didnt finish , plz wait or check program status, ret: %d", in_t.ret);

        close(sockfd);

        free(pNext);
        pNext = nullptr;

        return NGX_ERROR;
    } else if (nullptr != in_t.data && in_t.data_len > 0) {
        cli_group->decodeFromCharArray(static_cast<char *>(in_t.data), in_t.data_len);
        if (!cli_group->groups.empty()) {

            Group *tgroup = cli_group->findGroupFromNodeIp(localIp);
            if(!tgroup){
                dfscli_log(DFS_LOG_ERROR,"cli get group error");
                close(sockfd);
                return NGX_ERROR;
            }
            std::string myleader = tgroup->getGroupLeader().getNodeIp();
            nn_addr = (server_bind_t *) sconf->namenode_addr.elts;
//            dbg(myleader);
            strcpy(reinterpret_cast<char *>(nn_addr[0].addr.data), myleader.c_str());
            strcpy(dfscli_cycle->namenode_addr,myleader.c_str());
            nn_addr[0].addr.len = myleader.length();
            nn_addr[0].port = NAME_NODE_PORT;

        }
        close(sockfd);
        return NGX_OK;
    }
    close(sockfd);
    dfscli_log(DFS_LOG_FATAL,
               "get group from datanode failed, ret: %d", in_t.ret);
    return NGX_ERROR;
}

static int dfscli_mkdir(char *path) {
    conf_server_t *sconf = nullptr;
    server_bind_t *nn_addr = nullptr;

    sconf = (conf_server_t *) dfscli_cycle->sconf;
    nn_addr = (server_bind_t *) sconf->namenode_addr.elts;

redo:
    int sockfd = dfs_connect(dfscli_cycle->namenode_addr, nn_addr[0].port);
    if (sockfd < 0) {
        // if conn to nn failed , then try to re get nn leader form dn
        update_group_from_local();
        close(sockfd);
        return NGX_ERROR;
    }

    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = NN_MKDIR;
    keyEncode((uchar_t *) path, (uchar_t *) out_t.key);

    getUserInfo(&out_t);

    out_t.permission = 755;

    char sBuf[BUF_SZ] = "";
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    int ws = write(sockfd, sBuf, sLen);
    if (ws != sLen) {
        dfscli_log(DFS_LOG_WARN, "write err, ws: %d, sLen: %d", ws, sLen);

        close(sockfd);

        return NGX_ERROR;
    }

    char rBuf[BUF_SZ] = "";
    int rLen = read(sockfd, rBuf, sizeof(rBuf));
    if (rLen < 0) {
        dfscli_log(DFS_LOG_WARN, "read err, rLen: %d", rLen);

        close(sockfd);

        return NGX_ERROR;
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(rBuf, rLen, &in_t);

    if (in_t.ret != NGX_OK) {
        if (in_t.ret == MASTER_REDIRECT){
            memcpy(dfscli_cycle->namenode_addr, in_t.data,in_t.data_len);
            nn_addr[0].addr.len = in_t.data_len;
            dbg(dfscli_cycle->namenode_addr);
            close(sockfd);
            goto redo;
        } else if(in_t.ret == NO_MASTER){
            dfscli_log(DFS_LOG_WARN, "error : no master now.");
        }else if (in_t.ret == KEY_EXIST) {
            dfscli_log(DFS_LOG_WARN, "mkdir err, path %s is exist.", path);
        } else if (in_t.ret == NOT_DIRECTORY) {
            dfscli_log(DFS_LOG_WARN,
                       "mkdir err, parent path is not a directory.");
        } else if (in_t.ret == PERMISSION_DENY) {
            dfscli_log(DFS_LOG_WARN, "mkdir err, permission deny.");
        } else {
            dfscli_log(DFS_LOG_WARN, "mkdir err, ret: %d", in_t.ret);
        }
    }

    close(sockfd);

    return NGX_OK;
}

static int dfscli_rmr(char *path) {
    conf_server_t *sconf = nullptr;
    server_bind_t *nn_addr = nullptr;

    sconf = (conf_server_t *) dfscli_cycle->sconf;
    nn_addr = (server_bind_t *) sconf->namenode_addr.elts;

redo:
    int sockfd = dfs_connect(dfscli_cycle->namenode_addr, nn_addr[0].port);
    if (sockfd < 0) {
        // if conn to nn failed , then try to re get nn leader form dn
        update_group_from_local();
        close(sockfd);

        return NGX_ERROR;
    }

    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = NN_RMR;
    keyEncode((uchar_t *) path, (uchar_t *) out_t.key);

    getUserInfo(&out_t);

    char sBuf[BUF_SZ] = "";
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    int ws = write(sockfd, sBuf, sLen);
    if (ws != sLen) {
        dfscli_log(DFS_LOG_WARN, "write err, ws: %d, sLen: %d", ws, sLen);

        close(sockfd);

        return NGX_ERROR;
    }

    char rBuf[BUF_SZ] = "";
    int rLen = read(sockfd, rBuf, sizeof(rBuf));
    if (rLen < 0) {
        dfscli_log(DFS_LOG_WARN, "read err, rLen: %d", rLen);

        close(sockfd);

        return NGX_ERROR;
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(rBuf, rLen, &in_t);

    if (in_t.ret != NGX_OK) {
        if (in_t.ret == MASTER_REDIRECT){
            memcpy(dfscli_cycle->namenode_addr, in_t.data,in_t.data_len);
            nn_addr[0].addr.len = in_t.data_len;
            close(sockfd);
            goto redo;
        } else if(in_t.ret == NO_MASTER){
            dfscli_log(DFS_LOG_WARN, "error : no master now.");
        } else if (in_t.ret == NOT_DIRECTORY) {
            dfscli_log(DFS_LOG_WARN,
                       "rmr err, the target is a file, you should use -rm instead.");
        } else if (in_t.ret == KEY_NOTEXIST) {
            dfscli_log(DFS_LOG_WARN, "rmr err, path %s doesn't exist.", path);
        } else if (in_t.ret == PERMISSION_DENY) {
            dfscli_log(DFS_LOG_WARN, "rmr err, permission deny.");
        } else {
            dfscli_log(DFS_LOG_WARN, "rmr err, ret: %d", in_t.ret);
        }
    }

    close(sockfd);

    return NGX_OK;
}

static int dfscli_ls(char *path) {
    conf_server_t *sconf = nullptr;
    server_bind_t *nn_addr = nullptr;

    sconf = (conf_server_t *) dfscli_cycle->sconf;
    nn_addr = (server_bind_t *) sconf->namenode_addr.elts;

    int sockfd = dfs_connect(dfscli_cycle->namenode_addr, nn_addr[0].port);
    if (sockfd < 0) {
        // if conn to nn failed , then try to re get nn leader form dn
        update_group_from_local();
        close(sockfd);

        return NGX_ERROR;
    }

    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = NN_LS;
    keyEncode((uchar_t *) path, (uchar_t *) out_t.key);

    getUserInfo(&out_t);

    char sBuf[BUF_SZ] = "";
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    int ws = write(sockfd, sBuf, sLen);
    if (ws != sLen) {
        dfscli_log(DFS_LOG_WARN, "write err, ws: %d, sLen: %d", ws, sLen);

        close(sockfd);

        return NGX_ERROR;
    }

    int pLen = 0;
    int rLen = recv(sockfd, &pLen, sizeof(int), MSG_PEEK|MSG_WAITALL);
//    dbg(pLen);
    if (rLen < 0) {
        dfscli_log(DFS_LOG_WARN, "recv err, rLen: %d", rLen);

        close(sockfd);

        return NGX_ERROR;
    }

    char *pNext = (char *) malloc(pLen);
    if (nullptr == pNext) {
        dfscli_log(DFS_LOG_WARN, "malloc err, pLen: %d", pLen);

        close(sockfd);

        return NGX_ERROR;
    }

    rLen = readn(sockfd, pNext, pLen);
//    dbg(rLen);
    if (rLen < 0) {
        dfscli_log(DFS_LOG_WARN, "read err, rLen: %d", rLen);

        close(sockfd);

        free(pNext);
        pNext = nullptr;

        return NGX_ERROR;
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(pNext, rLen, &in_t);
//    dbg(in_t.data_len);
//    dbg(in_t.data);
    if (in_t.ret != NGX_OK) {
        if (in_t.ret == KEY_NOTEXIST) {
            dfscli_log(DFS_LOG_WARN, "ls err, path %s doesn't exist.", path);
        } else if (in_t.ret == PERMISSION_DENY) {
            dfscli_log(DFS_LOG_WARN, "ls err, permission deny.");
        } else {
            dfscli_log(DFS_LOG_WARN, "ls err, ret: %d", in_t.ret);
        }
    } else if (nullptr != in_t.data && in_t.data_len > 0) {
//        dbg(in_t.data_len);
        showDirsFiles((char *) in_t.data, in_t.data_len);
    }

    close(sockfd);

    free(pNext);
    pNext = nullptr;

    return NGX_OK;
}

static int showDirsFiles(char *p, int len) {
    fi_inode_t_ fii;
    int fiiLen = sizeof(fi_inode_t_);
    uchar_t permission[16] = "";
    char mtime[64] = "";
    uchar_t path[PATH_LEN] = "";

    while (len > 0) {
        bzero(&fii, sizeof(fi_inode_t_));
        memcpy(&fii, p, fiiLen);
        printf("%s", fii.is_directory ? "d" : "-");
//        cout<< fii.is_directory ? "d" : "-";
        memset(permission, 0x00, sizeof(permission));
        get_permission(fii.permission, permission);
        printf("%s", permission);

        if (fii.is_directory) {
            printf("    -");
//            cout<<"    -";
        } else {
            printf("    %d", fii.blk_replication);
//            cout<<"    " << fii.blk_replication;
        }
        printf(" %s %s    %ld", fii.owner, fii.group, fii.length);
//        cout<<" "<<fii.owner <<" "<<fii.group<<"    "<<fii.length;
        memset(mtime, 0x00, sizeof(mtime));
        getTimeStr(fii.modification_time, mtime, sizeof(mtime));
        printf(" %s", mtime);
//        cout<<" "<<mtime;
        memset(path, 0x00, sizeof(path));
        keyDecode((uchar_t *) fii.key, path);
        printf(" %s\n", path);
//        dbg(path);
//        cout<<" "<< path <<endl;
        p += fiiLen;
        len -= fiiLen;
    }

    return NGX_OK;
}

static int getTimeStr(uint64_t msec, char *str, int len) {
    time_t t;
    struct tm *p;

    t = msec / 1000 + 28800;
    p = gmtime(&t);
    strftime(str, len, "%Y-%m-%d %H:%M:%S", p);

    return NGX_OK;
}

static int isPathValid(char *path) {
    char *p = path;

    while (*p != '\000') {
        if (*p == '\\' || *p == ':' || *p == '*' || *p == '?' || *p == '"'
            || *p == '<' || *p == '>' || *p == '|') {
            return NGX_FALSE;
        }

        p++;
    }

    return NGX_TRUE;
}

//check path is rigth format
static int getValidPath(char *src, char *dst) {
    char *s = src;
    char *d = dst;
    char l = '/';

    if (0 == strcmp(src, "/") || 0 == strcmp(src, "//")) {
        strcpy(dst, "/");

        return NGX_OK;
    }

    while (*s != '\000') {
        if (l == '/' && *s == '/') {
            l = *s++;

            continue;
        }

        if (l == '/') {
            *d++ = l;
        }

        *d++ = *s++;
        l = *s;
    }

    if (l == '/') {
        *(d--) = '\000';
    }

    return NGX_OK;
}

static int dfscli_rm(char *path) {
    conf_server_t *sconf = nullptr;
    server_bind_t *nn_addr = nullptr;

    sconf = (conf_server_t *) dfscli_cycle->sconf;
    nn_addr = (server_bind_t *) sconf->namenode_addr.elts;
redo:
    int sockfd = dfs_connect(dfscli_cycle->namenode_addr, nn_addr[0].port);
    if (sockfd < 0) {
        // if conn to nn failed , then try to re get nn leader form dn
        update_group_from_local();
        close(sockfd);

        return NGX_ERROR;
    }

    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = NN_RM;
    keyEncode((uchar_t *) path, (uchar_t *) out_t.key);

    getUserInfo(&out_t);

    char sBuf[BUF_SZ] = "";
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    int ws = write(sockfd, sBuf, sLen);
    if (ws != sLen) {
        dfscli_log(DFS_LOG_WARN, "write err, ws: %d, sLen: %d", ws, sLen);

        close(sockfd);

        return NGX_ERROR;
    }

    char rBuf[BUF_SZ] = "";
    int rLen = read(sockfd, rBuf, sizeof(rBuf));
    if (rLen < 0) {
        dfscli_log(DFS_LOG_WARN, "read err, rLen: %d", rLen);

        close(sockfd);

        return NGX_ERROR;
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(rBuf, rLen, &in_t);

    if (in_t.ret != NGX_OK) {
        if (in_t.ret == MASTER_REDIRECT){
            memcpy(dfscli_cycle->namenode_addr, in_t.data,in_t.data_len);
            nn_addr[0].addr.len = in_t.data_len;
            close(sockfd);
            goto redo;
        } else if(in_t.ret == NO_MASTER){
            dfscli_log(DFS_LOG_WARN, "error : no master now.");
        } else if (in_t.ret == NOT_FILE) {
            dfscli_log(DFS_LOG_WARN,
                       "rm err, the target is a directory, you should use -rmr instead.");
        } else if (in_t.ret == KEY_NOTEXIST) {
            dfscli_log(DFS_LOG_WARN, "rm err, path %s doesn't exist.", path);
        } else if (in_t.ret == PERMISSION_DENY) {
            dfscli_log(DFS_LOG_WARN, "rm err, permission deny.");
        } else {
            dfscli_log(DFS_LOG_WARN, "rm err, ret: %d", in_t.ret);
        }
    }

    close(sockfd);

    return NGX_OK;
}

static int dfscli_killall() {
    int res = 0;
    if (update_group_from_local() == NGX_ERROR) {
        return NGX_ERROR;
    }
    // conn to dn master
    if(cli_group->getGroupMaster().getNodeIp().empty()){
        dfscli_log(DFS_LOG_WARN, "dfscli_killall get master null");
        return NGX_ERROR;
    }
    int sockfd = dfs_connect((char *) cli_group->getGroupMaster().getNodeIp().c_str(), DN_PORT);
    if (sockfd < 0) {
        close(sockfd);

        return NGX_ERROR;
    }
    data_transfer_header_t header;
    memset(&header, 0x00, sizeof(data_transfer_header_t));
    header.op_type = OP_CLI_REQUEST_ALL;

    res = send(sockfd, &header, sizeof(data_transfer_header_t), 0);
    if (res < 0) {
        dfscli_log(DFS_LOG_WARN, "dfscli_killall send header to %s err, %s",
                   cli_group->getGroupMaster().getNodeIp().c_str(), strerror(errno));
        return NGX_ERROR;
    }

    // then recv
    int pLen = 0;
    int rLen = recv(sockfd, &pLen, sizeof(int), MSG_PEEK);
    if (rLen < 0) {
        dfscli_log(DFS_LOG_FATAL,
                   "dfscli_killall recv err, rLen: %d", rLen);

        close(sockfd);

        return NGX_ERROR;
    }

    char *pNext = (char *) malloc(pLen);
    if (nullptr == pNext) {
        dfscli_log(DFS_LOG_FATAL,
                   "dfscli_killall malloc err, pLen: %d", pLen);

        close(sockfd);

        return NGX_ERROR;
    }

    rLen = read(sockfd, pNext, pLen);
    if (rLen < 0) {
        dfscli_log(DFS_LOG_FATAL,
                   "dfscli_killall read err, rLen: %d", rLen);

        close(sockfd);

        free(pNext);
        pNext = nullptr;

        return NGX_ERROR;
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(pNext, rLen, &in_t);

    if (in_t.ret != OP_STATUS_SUCCESS) {
        dfscli_log(DFS_LOG_FATAL, "dfscli_killall error, plz wait or check program status, ret: %d", in_t.ret);

        close(sockfd);

        free(pNext);
        pNext = nullptr;

        return NGX_ERROR;
    }

    dfscli_log(DFS_LOG_INFO, "killall wait for close, ret: %d", in_t.ret);
    close(sockfd);
    free(pNext);
    pNext = nullptr;
    return NGX_ERROR;
};

static int dfscli_kill(char *path) {
    int res = 0;
    if (update_group_from_local() == NGX_ERROR) {
        return NGX_ERROR;
    }
    // conn to dn master
    if(cli_group->getGroupMaster().getNodeIp().empty()){
        dfscli_log(DFS_LOG_WARN, "dfscli_killall get master null");
        return NGX_ERROR;
    }
    int sockfd = dfs_connect((char *) cli_group->getGroupMaster().getNodeIp().c_str(), DN_OPEN_PORT);
    if (sockfd < 0) {
        close(sockfd);

        return NGX_ERROR;
    }

    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = KILL_NODE;

    out_t.data_len = strlen(path) + 1;
    out_t.data = malloc(out_t.data_len);
    memcpy(out_t.data,path,out_t.data_len);

    char sBuf[BUF_SZ] = "";
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    if(out_t.data != nullptr){
        free(out_t.data);
    }

    int ws = write(sockfd, sBuf, sLen);

    // then recv
    int pLen = 0;
    int rLen = recv(sockfd, &pLen, sizeof(int), MSG_PEEK);
    if (rLen < 0) {
        dfscli_log(DFS_LOG_FATAL,
                   "dfscli_killall recv err, rLen: %d", rLen);

        close(sockfd);

        return NGX_ERROR;
    }

    char *pNext = (char *) malloc(pLen);
    if (nullptr == pNext) {
        dfscli_log(DFS_LOG_FATAL,
                   "dfscli_killall malloc err, pLen: %d", pLen);

        close(sockfd);

        return NGX_ERROR;
    }

    rLen = read(sockfd, pNext, pLen);
    if (rLen < 0) {
        dfscli_log(DFS_LOG_FATAL,
                   "dfscli_killall read err, rLen: %d", rLen);

        close(sockfd);

        free(pNext);
        pNext = nullptr;

        return NGX_ERROR;
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(pNext, rLen, &in_t);

    if (in_t.ret != OP_STATUS_SUCCESS) {
        dfscli_log(DFS_LOG_FATAL, "dfscli_killall error, plz wait or check program status, ret: %d", in_t.ret);

        close(sockfd);

        free(pNext);
        pNext = nullptr;

        return NGX_ERROR;
    }

    dfscli_log(DFS_LOG_INFO, "wait for close, ret: %d", in_t.ret);
    close(sockfd);
    free(pNext);
    pNext = nullptr;
    return NGX_ERROR;
};
// 默认两张网卡的话用第一张
int cli_get_local_ip(char *ip) {
    int fd, intrface, retn = 0;
    struct ifreq buf[INET_ADDRSTRLEN];
    struct ifconf ifc{};
    if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) >= 0) {
        ifc.ifc_len = sizeof(buf);
        // caddr_t,linux内核源码里定义的：typedef void *caddr_t；
        ifc.ifc_buf = (caddr_t) buf;
        if (!ioctl(fd, SIOCGIFCONF, (char *) &ifc)) {
            intrface = ifc.ifc_len / sizeof(struct ifreq);
            //
            if (intrface > 1) {
                dfs_log_error(dfscli_cycle->error_log, DFS_LOG_DEBUG, 0, "you have used multiple net interface card,"
                                                                      "default use the second for program.(maybe not 127.0.0.1)");
                strcpy(ip, inet_ntoa(((struct sockaddr_in *) (&buf[1].ifr_addr))->sin_addr));
                close(fd);

                return NGX_OK;
            } else if (intrface == 1) {
                dfs_log_error(dfscli_cycle->error_log, DFS_LOG_ERROR, 0,
                              "detect only one net interface card,will use 127.0.0.1");
                strcpy(ip, inet_ntoa(((struct sockaddr_in *) (&buf[0].ifr_addr))->sin_addr));
                close(fd);

                return NGX_OK;
            } else {
                dfs_log_error(dfscli_cycle->error_log, DFS_LOG_ERROR, 0, "detect no net interface card , exit!");
                close(fd);
                return NGX_ERROR;
            }

            //
//            while (intrface-- > 0) {
//                    strcpy(ip, inet_ntoa(((struct sockaddr_in *) (&buf[intrface].ifr_addr))->sin_addr));
////                    ip=(inet_ntoa(((struct sockaddr_in*)(&buf[intrface].ifr_addr))->sin_addr));
//                    printf("IP:%s\n", ip);
//                }
//            }
        }
    }
    close(fd);
    dfs_log_error(dfscli_cycle->error_log, DFS_LOG_ERROR, 0, "get_local_ip func error");
    return NGX_ERROR;
}