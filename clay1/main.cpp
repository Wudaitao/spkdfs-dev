#include <stdlib.h>
#include <fstream>
#include <unistd.h>
#include <ctime>
#include <sstream>
#include <sys/time.h>
#include <malloc.h>
#include "Clay.h"
#define LARGE_ENOUGH 2048
//using namespace std;
int blocksize,subchunk_size,helper_blocksize;////È«ŸÖ±äÁ¿ÉùÃ÷
void EXPECT_EQ(int a, int b)
{
    if(a != b)
        std::cout << "false" << std::endl;
    else
        std::cout << "true" << std::endl;
}
int main()
{
    //测试k=3，m=3的编码，测试读写文件
    Clay clay("erasure_code_dir");
    ErasureCodeProfile profile;
    profile["k"] = "3";
    profile["m"] = "2";
    profile["d"] = "4";
    struct timeval t1, t2;
    struct timezone tz;
    double tsec;
gettimeofday(&t1, &tz);
    int r= clay.init(profile, &cerr);
    int flag;
    int want_to_encode[] = { 0, 1, 2, 3, 4 };
    char *fname = "4.txt";
//for(int i = 0;i<256;i++){
//cout<<i<<endl;
    EXPECT_EQ(0, clay.encode(set<int>(want_to_encode, want_to_encode+5),fname));
//}
gettimeofday(&t2, &tz);
 ////计算时间
    tsec = 0.0;
    tsec += t2.tv_usec;
    tsec -= t1.tv_usec;
    tsec /= 1000000.0;
    tsec += t2.tv_sec;
    tsec -= t1.tv_sec;
    printf("En_Total (KB/sec): %0.10f\n", (256*1024)/tsec);

    ////测试解码，文件读写功能
    int datasize = 0;

    //while(1)
    //{
cin>>flag;
//if(flag==1){
gettimeofday(&t1, &tz);
        int want_to_decode[] = {0,1};
        int avail[clay.get_chunk_count()];
        int erasures_count = 2;

        for(int i = 0; i<clay.get_chunk_count(); i++)
        {
            avail[i] = i;
        }
        set<int> available(avail, avail+5);
available.erase(0);
//available.erase(2);
available.erase(1);
        ////解码
        if(erasures_count==0){
            //continue;
        }
        else if(erasures_count==1)
        {
            EXPECT_EQ(0, clay.decode(set<int>(want_to_decode, want_to_decode+erasures_count), available, blocksize,fname));////根据helper进行解码，decoded只包含待修复块
        }
        else
        {
            EXPECT_EQ(0, clay._decode(set<int>(want_to_decode, want_to_decode+erasures_count),fname));
        }
gettimeofday(&t2, &tz);
 ////计算时间
    tsec = 0.0;
    tsec += t2.tv_usec;
    tsec -= t1.tv_usec;
    tsec /= 1000000.0;
    tsec += t2.tv_sec;
    tsec -= t1.tv_sec;
    printf("De_Total (KB/sec): %0.10f\n", (256*1024)/tsec);

//}
//else
   //break;
//return 0;
    //}


    return 0;
}

