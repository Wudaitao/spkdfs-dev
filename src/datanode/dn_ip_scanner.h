//
// Created by ginux on 2020/5/30.
//

#ifndef NGXFS_DN_IP_SCANNER_H
#define NGXFS_DN_IP_SCANNER_H


#include <iostream>
#include <string>
#include <cstdlib>
#include <fstream>
#include <vector>
#include <set>
#include <sstream>
#include <ctime>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>

using namespace std;
enum {
    NODE_HAS_JOINED = -10
};

#define INETADDR(a, b, c, d) (d + (c << 8) + (b << 16) + (a << 24)) //http://www.rohitab.com/discuss/topic/34061-cc-ip-address-algorithm/

class port_scan {
public:
    port_scan(const string& host, int port, int useconfig,int utime_out = 250000);  // .5 seconds aka 500 miliseconds 500000


    bool is_port_open(void);

    void* send_http_request(cmd_t cmd, void *data, int data_len);

    ~port_scan(void);

private:
    struct timeval timeout = {0};
    struct sockaddr_in scan_addr = {0};
    int s0;
    string scan_host;
    int useConfig = true;

};

class misc {
public:
    vector<int> split_by_octet(string ip);

    vector<string> iter_ips(string from, string to);

};

class start_scan {
public:
    int start(const string& ranges, const string& port, int useconfig = true);
    int start_oldgroup_scan(vector<string>& oldips, set<string>& oldleaders);
    int useConfig = false;
    int ipRangeNum = 0;
};


int get_local_ip(char *ip);
int handle_init_node_join_finish(task_t *task);




//int main(int argc, char **argv) //80,443,8080,280,4443
//{
//    start_scan ss;
//    if (argc == 3) {
//        ss.start(argv[1], argv[2]);
//        return EXIT_SUCCESS;
//    } else {
//        cout << "ERROR correct usage: " << argv[0] << " IPRange ListOfPorts" << endl
//             << "Example: " << argv[0] << " \"127.0.0.0-127.0.0.255\" \"80,443,8080,280,4443\"" << endl;
//        return EXIT_FAILURE;
//    }
//}
#endif //NGXFS_DN_IP_SCANNER_H
