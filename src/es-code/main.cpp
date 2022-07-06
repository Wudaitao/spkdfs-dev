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
    profile["m"] = "3";
    profile["d"] = "4";
    int r= clay.init(profile, &cerr);
    int flag;
    int want_to_encode[] = { 0, 1, 2, 3, 4, 5 };
    char *fname = "100.mp4";

    EXPECT_EQ(0, clay.encode(set<int>(want_to_encode, want_to_encode+6),fname));


    ////测试解码，文件读写功能
    int datasize = 0;

    //while(1)
    //{
cin>>flag;
//if(flag==1){

        int want_to_decode[] = {0,1,2};
        int avail[clay.get_chunk_count()];
        int erasures_count = 3;
        for(int i = 0; i<clay.get_chunk_count(); i++)
        {
            avail[i] = i;
        }
        set<int> available(avail, avail+6);
available.erase(0);
available.erase(1);
available.erase(2);

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

//}
//else
   //break;
//return 0;
    //}


    return 0;
}

