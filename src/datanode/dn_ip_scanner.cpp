#include <sys/ioctl.h>

#include <net/if.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstring>
#include <cstdio>
#include <zconf.h>

#include <strings.h>
#include <dfs_utils.h>
#include <dfs_dbg.h>
#include "dn_ip_scanner.h"
#include "dfs_task.h"
#include "dn_group.h"
#include "dfs_types.h"
#include "dn_cycle.h"
#include "dfs_error_log.h"
#include "dn_conf.h"
#include "../../etc/config.h"

#define BUF_SZ  4096

char localIp[INET_ADDRSTRLEN] = {0};
char localAddr[25] = {0};
//int openPort = 0;
bool stopScanFlag = false;
extern bool start_ns_thread_flag; // start ns thread

port_scan::port_scan(const string& host, int port, int useconfig, int utime_out) {
    useConfig = useconfig;
    scan_host = host;
    if ((s0 = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("error creating socket");
    }
    fcntl(s0, F_SETFL, O_NONBLOCK); // set to non-blocking
    scan_addr.sin_family = AF_INET;
    scan_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, (host).c_str(), &scan_addr.sin_addr) <= 0) {
        perror("error creating socket");
    }
    timeout.tv_sec = 0;
    timeout.tv_usec = utime_out;
    setsockopt(s0, SOL_SOCKET, SO_RCVTIMEO, (char *) &timeout, sizeof(timeout));
    setsockopt(s0, SOL_SOCKET, SO_SNDTIMEO, (char *) &timeout, sizeof(timeout));
}

bool port_scan::is_port_open() {
    fd_set s0_fd;
    connect(s0, (struct sockaddr *) &scan_addr, sizeof(scan_addr));
    FD_ZERO(&s0_fd);
    FD_SET(s0, &s0_fd);
    if (select(s0 + 1, 0, &s0_fd, 0, &timeout) == 1) {
        int so_error;
        socklen_t len = sizeof(so_error);
        getsockopt(s0, SOL_SOCKET, SO_ERROR, &so_error, &len);
        if (so_error == 0) {
            return true;
        }
    }
    return false;
}

// do remember free it !!!
//todo:扫描完了，只有自己
void *port_scan::send_http_request(cmd_t cmd, void *data, int data_len)  {
    // send task
    // send self ip and open port to remote
    task_t out_t;
    bzero(&out_t, sizeof(task_t));
    out_t.cmd = cmd;
    out_t.data = malloc(data_len);
    memcpy(out_t.data,data,data_len);
    out_t.data_len = data_len;

    char sBuf[BUF_SZ] = {0};
    int sLen = task_encode2str(&out_t, sBuf, sizeof(sBuf));
    if(out_t.data != nullptr){
        free(out_t.data);
        out_t.data = nullptr;
    }
    fcntl(s0, F_SETFL, fcntl(s0, F_GETFL, 0) & ~O_NONBLOCK); // set socket back to blocking
    int ret = send(s0, sBuf, sLen, MSG_WAITALL);
    if (ret < 0) {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "send err");
        return nullptr;
    }
    int pLen = 0;
    int rLen = 0;
    rLen = recv(s0, &pLen, sizeof(int), MSG_PEEK);

    if (rLen < 0)
    {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "recv err, rLen: %d", rLen);
        close(s0);
        return nullptr;
    }

    char *pNext = (char *)malloc(pLen);
    if (nullptr == pNext)
    {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "malloc err, pLen: %d", pLen);

        close(s0);

        return nullptr;
    }

    rLen = readn(s0, pNext, pLen);
//    dbg(rLen);
    if(rLen == 0) return nullptr;
    if (rLen < 0)
    {
        dfs_log_error(dfs_cycle->error_log, DFS_LOG_FATAL, errno,
                      "read err, rLen: %d", rLen);

        close(s0);

        free(pNext);
        pNext = nullptr;

        return nullptr;
    }

//    while ((rLen = recv(s0, buffer, sizeof(buffer),MSG_WAITALL) == -1 && errno == EINTR));


//    fprintf(stderr,"%s %d\n",strerror(errno),rLen);


    task_t in_t;
    bzero(&in_t, sizeof(task_t));
    task_decodefstr(pNext, rLen, &in_t);
    // test
//        printf("send_http_request recv: %d\n",in_t.ret);

    // end
    switch (in_t.ret) {
        case SUCC: {// scan success, return peers ip
            dbg((char *)in_t.data);
            if (in_t.data != nullptr) {
                if(isIpFormatRight((char *)in_t.data)) return (char *) in_t.data; // return ip
                else{
                    dfs_log_error(dfs_cycle->error_log,DFS_LOG_ERROR,0,"dn_ip_scanner recv bad ip\n");
                }
            }
            break;
        }
        case NODE_NEED_WAIT: {   // group is building ,wait to join
            //re send every 1s
            dbg("NODE_NEED_WAIT");
            sleep(1);
            this->send_http_request(NODE_WANT_JOIN, localAddr, strlen(localAddr));
            break;
        }
        case REDIRECT_TO_MASTER: {
            char masterIp[INET_ADDRSTRLEN] = {0};
            int master_sock;
            memcpy(masterIp,in_t.data,in_t.data_len);
            printf("REDIRECT_TO_MASTER, mster ip:%s\n",masterIp);
            close(this->s0);
            master_sock = net_connect(masterIp,dfs_cycle->listening_open_port,dfs_cycle->error_log);
            if(master_sock!=NGX_ERROR){ // change sock to master sock and redo send request
                this->s0 = master_sock;
                usleep(500);
                this->send_http_request(NODE_WANT_JOIN,localAddr,strlen(localAddr));
            }
            break;
        }
        case NODE_JOIN_FINISH:  {    // group build finish ,join success
            // ,start with blank NodeList(see phxpaxos add memeber)
            handle_init_node_join_finish(&in_t);
            stopScanFlag = true;
            break;
        }
        default:
            dfs_log_error(dfs_cycle->error_log,DFS_LOG_ERROR,0,"unknown ip scan cmd: %d\n",PENU(in_t.ret));
            break;
    }

    free(pNext);
    pNext = nullptr;
    return nullptr;
}

port_scan::~port_scan() {
    close(s0);
}

vector<int> misc::split_by_octet(string ip) {
    vector<int> octets;
    string octet;
    stringstream tmp_ss;
    tmp_ss << ip;
    while (getline(tmp_ss, octet, '.')) {
        octets.push_back(stoi(octet));
    }
    return octets;
}

vector<string> misc::iter_ips(string from, string to) {
    vector<string> ips;
    //cout << "Iterating from: " << from << " To: " << to << endl;
    vector<int> from_octets = split_by_octet(from);
    vector<int> to_octets = split_by_octet(to);
    if (from_octets.size() == 4 && to_octets.size() == 4) {
        unsigned char *ip;
        unsigned long from = INETADDR(from_octets[0], from_octets[1], from_octets[2], from_octets[3]),
                to = INETADDR(to_octets[0], to_octets[1], to_octets[2], to_octets[3]);
        while (from <= to) {
            char ip_addr[16] = {0};
            ip = (unsigned char *) &from;
            sprintf(ip_addr, "%u.%u.%u.%u", ip[3], ip[2], ip[1], ip[0]);
            ips.push_back(ip_addr);
            ++from;
        }
        return ips;
    } else {
        cout << "[-] ERROR MALFORMED IP RANGES" << endl;
        exit(EXIT_FAILURE);
    }
}

int start_scan::start(const string& ranges, const string& port, int useconfig) {
    useConfig = useconfig;
    misc m;
    vector<int> ports;
    // split ','
    if (port.find(",") != string::npos) {
        stringstream tmp_ports;
        string buff;
        tmp_ports << port;
        while (getline(tmp_ports, buff, ',')) {
            ports.push_back(stoi(buff));
        }
    } else {
        ports.push_back(stoi(port));
    }

    int split_ranges_pos = ranges.find('-');
    vector<string> ips = m.iter_ips(ranges.substr(0, split_ranges_pos),
                                    ranges.substr(split_ranges_pos + 1, ranges.size()));
    ipRangeNum = ips.size();
    cout << "[+] Number of IPs to be scanned: " << ips.size() << endl;
    //cout << ips.size()/thread_num << endl;
    if(get_local_ip(localIp)!= NGX_OK){ // get ip port from local net interface card

        // get local ip err
        return NGX_ERROR;
    }
    dn_group->setOwn(localIp,0);
    // check if the ip port is local ip port
    auto *sconf = (conf_server_t *)dfs_cycle->sconf;
    server_bind_t *listening_for_open = nullptr;
    listening_for_open = static_cast<server_bind_t *>(sconf->listen_open_port.elts);
//    openPort = listening_for_open[0].port;
    sprintf(localAddr,"%s:%d",localIp,dfs_cycle->listening_paxos_port);
    // change  my_paxos
    conf_set_my_paxos(localAddr,strlen(localAddr));

    for (string &ip : ips) {
        for (int &check_port: ports) {
//            cout << "start to scan " << ip << ":" << check_port << endl;
            if(useconfig){ // get ip port from config file
                strcpy(localIp,(char *)listening_for_open[0].addr.data);
            }

            port_scan ps(ip, check_port, useconfig); //209.85.200.100
            if (ps.is_port_open()) {
                // send scan task
                char *infoip = (char *) ps.send_http_request(TASK_IPSCAN,localAddr,strlen(localAddr));
                //
                if (infoip != nullptr) {

                    cout << "\r[+] Found Results on host: " << ip << ":" << check_port << " " << infoip << endl;

                    // single thread
                    dn_group->lockGroup();
                    // check if node exists
                    if (dn_group->checkAddrInNodeList(infoip, dfs_cycle->listening_paxos_port)==NGX_ERROR) {
                        dn_group->addAddrToNodeList(infoip, dfs_cycle->listening_paxos_port);
                    }
                    //free(infoip); // free
                    dn_group->unlockGroup();
                }
                // check stopScanFlag == 1, stop scan
                if(stopScanFlag == 1){
                    dn_group->resetNodeList();
                    return NODE_HAS_JOINED;
                }
            }
        }
        // cout<<ip<<check port << endl; // single ip multiple port finished
    }
    cout << endl << "[+] Done. Total Results found: " << dn_group->getNodeList().size() << endl;

    // only has self one
    if(dn_group->getNodeList().size() == 1){
        if(strcmp(dn_group->getNodeList()[0].getNodeIp().c_str(),localIp) == 0){
            // prepare ns_srv string
            string tmpstring;
            tmpstring = dn_group->getNodeList()[0].getNodeIp()+
                        ':'+std::to_string(dfs_cycle->listening_nssrv_port);
            sconf->ns_srv.data = reinterpret_cast<uchar_t *>(strdup(tmpstring.c_str()));
            // prepare ns paxos string
            dfs_cycle->leaders_paxos_ipport_string = dn_group->getNodeList()[0].getNodeIp()+
                    ':'+std::to_string(NN_PAXOS_PORT);
            dn_group->setOwn(localIp,dfs_cycle->listening_paxos_port);
            dbg(sconf->ns_srv.data);
            dbg(dfs_cycle->leaders_paxos_ipport_string);
        }
    }
    return dn_group->getNodeList().size();
}

int start_scan::start_oldgroup_scan(vector<string>& oldips,set<string>& oldleaders){
    if(get_local_ip(localIp)!= NGX_OK){ // get ip port from local net interface card
        // get local ip err
        return NGX_ERROR;
    }
    cout << endl << "[+] Done. Total old grouplist nodes is "<<oldips.size()<<" found: " << dn_group->getNodeList().size() << endl;
    dn_group->setOwn(localIp,0);
    int reconnect_oldleaders=0;
    int port=dfs_cycle->listening_open_port;
    // check if the ip port is local ip port
    auto *sconf = (conf_server_t *)dfs_cycle->sconf;
    server_bind_t *listening_for_open = nullptr;
    listening_for_open = static_cast<server_bind_t *>(sconf->listen_open_port.elts);
//    openPort = listening_for_open[0].port;
    sprintf(localAddr,"%s:%d",localIp,dfs_cycle->listening_paxos_port);
// change  my_paxos
    conf_set_my_paxos(localAddr,strlen(localAddr));

    for (string &ip : oldips) {
//        string ipstring = ip+to_string(port);
        port_scan ps(ip, port, false); //209.85.200.100
        if (ps.is_port_open()) {
            // send scan task
            char *infoip = (char *) ps.send_http_request(TASK_IPSCAN,localAddr,strlen(localAddr));
            //
            if (infoip != nullptr) {

                cout << "\r[+] Found Results on host: " << ip << ":" << port << " " << infoip << endl;
                // single thread
                dn_group->lockGroup();
//                dbg("dn_group locked");
                // check if node exists
                if (dn_group->checkAddrInNodeList(infoip, dfs_cycle->listening_paxos_port)==NGX_ERROR) {
                    dn_group->addAddrToNodeList(infoip, dfs_cycle->listening_paxos_port);
                }
                //free(infoip); // free
                dn_group->unlockGroup();
//                dbg("dn_group unlocked");
                if(oldleaders.count(infoip)==1){
                    reconnect_oldleaders++;
                }
            }
            // check stopScanFlag == 1, stop scan
            if(stopScanFlag == 1){
                dn_group->resetNodeList();
                return NODE_HAS_JOINED;
            }
        }
        // cout<<ip<<check port << endl; // single ip multiple port finished
    }
    cout << endl << "[+] Done. Total old grouplist nodes is "<<oldips.size()<<" found: " << dn_group->getNodeList().size() << endl;
    cout << endl << "[+] Done. Total old grouplist leaders is " <<oldleaders.size()<< " found: " << reconnect_oldleaders << endl;
    return reconnect_oldleaders;
}


int get_local_ip(char *ip) {
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
                dfs_log_error(dfs_cycle->error_log, DFS_LOG_DEBUG, 0, "you have used multiple net interface card,"
                                                                      "default use the second for program.(maybe not 127.0.0.1)");
                strcpy(ip, inet_ntoa(((struct sockaddr_in *) (&buf[1].ifr_addr))->sin_addr));
                close(fd);

                return NGX_OK;
            } else if (intrface == 1) {
                dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, 0,
                              "detect only one net interface card,will use 127.0.0.1");
                strcpy(ip, inet_ntoa(((struct sockaddr_in *) (&buf[0].ifr_addr))->sin_addr));
                close(fd);

                return NGX_OK;
            } else {
                dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, 0, "detect no net interface card , exit!");
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
    dfs_log_error(dfs_cycle->error_log, DFS_LOG_ERROR, 0, "get_local_ip func error");
    return NGX_ERROR;
}

int handle_init_node_join_finish(task_t *task) {
    char leaderIp[INET_ADDRSTRLEN] = {0};

    conf_server_t *sconf = nullptr;
    std::vector<DfsNode> tmplist ;
    std::string ns_srv_string;
    sconf = static_cast<conf_server_t *>(dfs_cycle->sconf);
    strcpy(leaderIp,task->key);
    // get all the leader
    dn_group->lockGroup();
    dn_group->decodeFromCharArray(static_cast<char *>(task->data), task->data_len);
    dn_group->unlockGroup();
    // save, using it to start ns server if need
    // set ns_srv_string here
    parse_ipport_list(dn_group->getleadersstring().c_str(),tmplist);
    int cnt = 0;
    for (auto & i : tmplist) {
        cnt++;
        ns_srv_string += i.getNodeIp()+':'+ std::to_string(dfs_cycle->listening_nssrv_port);
        // below is for starting namenode
        dfs_cycle->leaders_paxos_ipport_string += i.getNodeIp() + ':' + std::to_string(NN_PAXOS_PORT);
        if(cnt!=tmplist.size()){
            ns_srv_string += ',';
            // below is for starting namenode
            dfs_cycle->leaders_paxos_ipport_string += ',';
        }
    }
    sconf->ns_srv.data = reinterpret_cast<uchar_t *>(strdup(ns_srv_string.c_str()));
    sconf->ns_srv.len = ns_srv_string.length();
    // start start_ns_thread_flag
    start_ns_thread_flag = true;

    //
//    dbg((char *)task->data);
    printf("[ + ]start with blank, get leader ip: %s\n",leaderIp);

    //check if is leader, then start ns server
    dbg(localIp);
    dbg(leaderIp);

    if(strcmp(localIp,leaderIp) == 0){
        dn_group->setOwn(localIp,dfs_cycle->listening_paxos_port);

    }

    return 0;
}
