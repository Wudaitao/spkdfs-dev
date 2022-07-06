//
// Created by ginux on 2020/5/28.
//


#include <dfs_error_log.h>
#include <netinet/in.h>
#include <libnet.h>
#include <vector>
#include "dfs_utils.h"
#include "dfs_dbg.h"
#include "random_int_func.h"



//
//int get_ns_srv_names(uchar_t *path, uchar_t names[][64]) {
//    uchar_t *str = nullptr;
//    char *saveptr = nullptr;
//    uchar_t *token = nullptr;
//    int i = 0;
//
//    for (str = path;; str = nullptr, token = nullptr, i++) {
//        token = (uchar_t *) strtok_r((char *) str, ",", &saveptr);
//        if (token == nullptr) {
//            break;
//        }
//
//        memset(names[i], 0x00, PATH_LEN);
//        strcpy((char *) names[i], (const char *) token);
//    }
//
//    return i;
//}
using namespace std;

int parseIpPort(char *ipstring, char *ip, int &port) {
    int count = sscanf(ipstring, "%[^':']:%d",
                       ip,
                       &port);
    if (count != 2) {
        return NGX_ERROR;
    }
    return NGX_OK;
}

//
int net_connect(char *ip, int port, log_t *log, int timeout) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (-1 == sockfd) {
        dfs_log_error(log, DFS_LOG_WARN, 0, "socket() err: %s", strerror(errno));

        return NGX_ERROR;
    }

    int reuse = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    if (timeout > 0) {
        struct timeval timeout_t = {timeout, 0};//3s
        int ret = setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, (const char *) &timeout_t, sizeof(timeout_t));
        ret = setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (const char *) &timeout_t, sizeof(timeout_t));
        if (-1 == ret) {
            dfs_log_error(log, DFS_LOG_WARN, 0, "net_connect timeout set err: %s", strerror(errno));

            return NGX_ERROR;
        }
    }


    struct sockaddr_in servaddr{};
    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    servaddr.sin_addr.s_addr = inet_addr(ip);

    int iRet = connect(sockfd, (struct sockaddr *) &servaddr, sizeof(servaddr));
    if (iRet < 0) {
        dbg(ip);
        dfs_log_error(log, DFS_LOG_WARN, 0, "connect to %s:%d err: %s",
                      ip, port, strerror(errno));

        return NGX_ERROR;
    }

    return sockfd;
}

int send_cmd_task(cmd_t cmd, int sockfd, log_t *log) {
    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = cmd;
    out_t.permission = 755;
    out_t.data_len = 0;
    out_t.data = nullptr;
    char sBuf[BUF_SZ] = "";
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    int ws = write(sockfd, sBuf, sLen);
    if (ws != sLen) {
        dfs_log_error(log, DFS_LOG_WARN, 0, "write err, ws: %d, sLen: %d", ws, sLen);

        close(sockfd);

        return NGX_ERROR;
    }
    return NGX_OK;
}

void keyEncode(uchar_t *path, uchar_t *key) {
    string_t src;
    string_set(src, path);

    string_t dst;
    string_set(dst, key);

    string_base64_encode(&dst, &src);
}

void keyDecode(uchar_t *key, uchar_t *path) {
    string_t src;
    string_set(src, key);

    string_t dst;
    string_set(dst, path);

    string_base64_decode(&dst, &src);
}

int send_data_task(cmd_t cmd, int sockfd, int data_len, void *data, log_t *log) {
    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = cmd;
    out_t.permission = 755;
    out_t.data_len = data_len;
    out_t.data = data;
    char sBuf[BUF_SZ] = "";
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    int ws = write(sockfd, sBuf, sLen);
    if (ws != sLen) {
        dfs_log_error(log, DFS_LOG_WARN, 0, "write err, ws: %d, sLen: %d", ws, sLen);

        close(sockfd);

        return NGX_ERROR;
    }
    return NGX_OK;
}

int send_key_data_task(cmd_t cmd, int sockfd, char *src, int data_len, void *data, log_t *log) {
    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = cmd;
    out_t.permission = 755;
    keyEncode((uchar_t *) src, (uchar_t *) out_t.key);

    out_t.data_len = data_len;
    out_t.data = data;
    char sBuf[BUF_SZ] = "";
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    int ws = write(sockfd, sBuf, sLen);
    if (ws != sLen) {
        dfs_log_error(log, DFS_LOG_WARN, 0, "write err, ws: %d, sLen: %d", ws, sLen);

        close(sockfd);

        return NGX_ERROR;
    }
    return NGX_OK;
}

task_t recv_task(int sockfd, log_t *log, pool_s *pool) {

    int pLen = 0;
    int rLen = recv(sockfd, &pLen, sizeof(int), MSG_PEEK);
    if (rLen < 0) {
        dfs_log_error(log, DFS_LOG_WARN, 0, "recv err, rLen: %d", rLen);

        close(sockfd);

        return task_t();
    }

    char *pNext = (char *) pool_alloc(pool, pLen);
    if (!pNext) {
        dfs_log_error(log, DFS_LOG_WARN, 0, "pNext pool_alloc err, pLen: %d", pLen);

        close(sockfd);

        return task_t();
    }

    rLen = read(sockfd, pNext, pLen);
    if (rLen < 0) {
        dfs_log_error(log, DFS_LOG_WARN, 0, "read err, rLen: %d", rLen);

        close(sockfd);

        return task_t();
    }

    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(pNext, rLen, &in_t);

    return in_t;
}

task_t
redirect_to_master(cmd_t cmd, char *masterIp, int masterPort, void *data, int data_len, log_t *log, pool_s *pool) {
    int ret = -1;
    int master_sockfd = net_connect(masterIp, masterPort, log);
    ret = send_data_task(cmd, master_sockfd, data_len, data, log);
    if (ret == NGX_OK) {
        task_t out_t;
        bzero(&out_t, sizeof(task_t));
        out_t = recv_task(master_sockfd, log, pool);
        return out_t;
    }
    return {};
}


int dfsnode_parse_ipport(const char *pcStr, DfsNode &oNodeInfo) {
    char sIP[32] = {0};
    int iPort = -1;

    int count = sscanf(pcStr, "%[^':']:%d", sIP, &iPort);
    if (count != 2) {
        return NGX_ERROR;
    }

    oNodeInfo.setNodeIp(sIP);
    oNodeInfo.setNodePort(iPort);

    return NGX_OK;
}

int parse_ipport_list(const char *pcStr,
                      std::vector<DfsNode> &vecNodeInfoList) {
    std::string sTmpStr;
    int iStrLen = strlen(pcStr);

    for (int i = 0; i < iStrLen; i++) {
        if (pcStr[i] == ',' || i == iStrLen - 1) {
            if (i == iStrLen - 1 && pcStr[i] != ',') {
                sTmpStr += pcStr[i];
            }

            DfsNode oNodeInfo;
            int ret = dfsnode_parse_ipport(sTmpStr.c_str(), oNodeInfo);
            if (ret != 0) {
                return ret;
            }

            vecNodeInfoList.push_back(oNodeInfo);

            sTmpStr = "";
        } else {
            sTmpStr += pcStr[i];
        }
    }

    return NGX_OK;
}


string getExecDir() {
    char abs_path[1024];
    int cnt = readlink("/proc/self/exe", abs_path, 1024);//获取可执行程序的绝对路径
    if (cnt < 0 || cnt >= 1024) {
        return string();
    }

    //最后一个'/' 后面是可执行程序名，去掉devel/lib/m100/exe，只保留前面部分路径
    for (int i = cnt; i >= 0; --i) {
        if (abs_path[i] == '/') {
            abs_path[i + 1] = '\0';
            break;
        }
    }

    string path(abs_path);

    return path;
}

std::string expand_user(std::string path) {
    if (not path.empty() and path[0] == '~') {
        assert(path.size() == 1 or path[1] == '/');  // or other error handling
        char const *home = getenv("HOME");
        if (home or ((home = getenv("USERPROFILE")))) {
            path.replace(0, 1, home);
        } else {
            char const *hdrive = getenv("HOMEDRIVE"),
                    *hpath = getenv("HOMEPATH");
            assert(hdrive);  // or other error handling
            assert(hpath);
            path.replace(0, 1, std::string(hdrive) + hpath);
        }
    }
    return path;
}

int get3RandInt(int *nums, int groupcnt) {
    std::random_device rd;                            // 1.
    std::mt19937 gen(rd());                           // 2.
    if (groupcnt < 3) {
        std::uniform_int_distribution<> dis(0, groupcnt - 1);
        nums[0] = dis(gen);
        nums[1] = dis(gen);
        nums[2] = dis(gen);

    } else {
        distinct_uniform_int_distribution<> dis(0, groupcnt - 1);  // 3.
        nums[0] = dis(gen);
        nums[1] = dis(gen);
        nums[2] = dis(gen);
    }

    return NGX_OK;
}

bool isIpFormatRight(char *ipstr) {
    int a, b, c, d = 0;
    return (sscanf(ipstr, "%d.%d.%d.%d", &a, &b, &c, &d) == 4)
           && (a >= 0 && a <= 255)
           && (b >= 0 && b <= 255)
           && (c >= 0 && c <= 255)
           && (d >= 0 && d <= 255);
}

int get1RandInt(int groupcnt) {
    if (groupcnt - 1 <= 0) return 0;

    std::random_device rd;                            // 1.
    std::mt19937 gen(rd());                           // 2.
    distinct_uniform_int_distribution<> dis(0, groupcnt - 1);  // 3.
    return dis(gen);;
}

// groupcnt must > 0
/**
 *
 * @param nums  nums len(nums) = cnt
 * @param cnt  total nums of randint (len(nums) = cnt <= groupcnt)
 * @param groupcnt (0 ~ groupcnt -1)
 * @return
 */
int get_distinct_randInt(int *nums, int cnt, int groupcnt) {
    if (cnt - 1 < 0) return 0;
    std::random_device rd;                            // 1.
    std::mt19937 gen(rd());                           // 2.
    if (cnt > groupcnt) return NGX_ERROR;
    distinct_uniform_int_distribution<> dis(0, groupcnt - 1);  // 3.

    for (int i = 0; i < cnt; i++) {
        nums[i] = dis(gen);
    }
    return NGX_OK;;
}

int get_random_randInt(int *nums, int cnt, int groupcnt) {
    if (cnt - 1 < 0) return 0;
    std::random_device rd;                            // 1.
    std::mt19937 gen(rd());                           // 2.
    std::uniform_int_distribution<> dis(0, groupcnt - 1);  // 3.

    for (int i = 0; i < cnt; i++) {
        nums[i] = dis(gen);
    }
    return NGX_OK;;
}

/* Read "n" bytes from a descriptor. */
ssize_t readn(int fd, void *vptr, size_t n) {
    size_t nleft;
    ssize_t nread;
    char *ptr;

    ptr = static_cast<char *>(vptr);
    nleft = n;

    while (nleft > 0) {
        if ((nread = read(fd, ptr, nleft)) < 0) {
            if (errno == EINTR)
                nread = 0;  /* and call read() again */
            else
                return (-1);
        } else if (nread == 0)

            break;    /* EOF */

        nleft -= nread;
        ptr += nread;
    }

    return (n - nleft);  /* return >= 0 */
}
