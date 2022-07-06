#include <memory.h>
#include <malloc.h>
#include <string.h>
#include <iostream>
#include <map>
#include <set>
#include <vector>
#include <memory>
#include <string>
#include <cerrno>
#include <errno.h>
#include <algorithm>
#include <assert.h>
#include "common.h"
#define SIMD_ALIGN 32
using namespace std;
void substr_of1(bufferlist* a, const bufferlist& other, unsigned off, unsigned length,int blocksize,int flag)
{
    char *b = (char*)malloc(length + 1);
    memset(b, 0, length + 1);
    auto curbuf = other.cbegin();

    while (off > 0 && off >= blocksize)
    {
        off -= blocksize;
        ++curbuf;
    }

//    for(int x = 0; x<length; x++)
//    {
//        b[x] = (*curbuf)[off+x];
//    }
    memcpy(b,(*curbuf)+off,length);
    a->push_back(std::move(b));

}
////ȡ??bufferlist other?ƫ?off????ength???ַ?????off???char*???????ݺ??????????????????ָ??list????
////blocksizeĬ?=blocksize?????ist?ÿ??char*???Ȳ?Ϊblocksize?ģ?????????
void substr_of(bufferlist* a, const bufferlist& other, unsigned off, unsigned length,int blocksize,int flag)
{
    char *b = (char*)malloc(length + 1);
    memset(b, 0, length + 1);
    auto curbuf = other.cbegin();

    while (off > 0 && off >= blocksize)   ////????????????
    {
        off -= blocksize;
        ++curbuf;
    }
    if(len(other,flag) == 0)
    {
        while (length > 0 && curbuf != other.cend())
        {
            // ȡ?????
            if (off + length <= blocksize)////????????blocksize=pow_int(q, t-1-y_lost)*sc_size???epair_blocksize
            {
//                for(int x = 0; x<length; x++)
//                {
//                    b[x] = (*curbuf)[off+x];
//                }
                memcpy(b,(*curbuf)+off,length);
                if(strlen(b) == 0)
                {
                    memset(b, 0, length + 1);
                    a->push_back(std::move(b));
                    return;
                }
                else
                {
                    a->push_back(std::move(b));////?˽????ĩβ???Ӵ?,b????har*??ͣ??????
                    return;
                }
            }
        }
    }

    if(off +length == len(other,flag))   ////????????????
    {
//        for(int x = 0; x<length; x++)
//        {
//            b[x] = (*curbuf)[off+x];
//        }
        memcpy(b,(*curbuf)+off,length);
        a->push_back(std::move(b));////?˽????ĩβ???Ӵ?,b????har*??ͣ??????
        return;
    }

    ////?????⣬???һ??????0?????????ݣ??????ȡ
    else if(off +length >len(other,flag))
    {
//        for(int x = 0; x<length; x++)
//        {
//            //printf("@%d\n",x);
//            b[x] = (*curbuf)[off+x];
//        }
        memcpy(b,(*curbuf)+off,length);
        ////??ȡ?ĵ????Ԫ?Ϊ\0?????????????Ϊ0?ˣ????
        if(strlen(b) == 0)
        {
            memset(b, 0, length + 1);
        }
        a->push_back(std::move(b));
        return;
    }

    // ȡ?????
    else
    {
//        for(int x = 0; x<length; x++)
//        {
//            b[x] = (*curbuf)[off+x];
//        }
        memcpy(b,(*curbuf)+off,length);
        a->push_back(std::move(b));////?˽????ĩβ???Ӵ?,b????har*??ͣ??????
    }
}
//??therƫ?Ϊoff???????len?ĵط???ֵ??ԴbufferlistΪa
void set_U(bufferlist a, bufferlist* other, int off, int length, int blocksize)
{
    // offָlist???????????off????tr??curbuf??????ptr?ĵ????
    auto curbuf = other->begin();////cbegin???????????Ԫ???onst???????ֻ??
    while (off > 0 && off >= blocksize)   ////????????????
    {
        off -= blocksize;
        ++curbuf;
    }

    while (length > 0)
    {
        // ȡ?????
        if (off + length <= blocksize)
        {
//            for(int x = 0; x<length; x++)
//            {
//                (*curbuf)[off+x] = a.front()[x];
////                if((*curbuf)[off+x] == '\0')
////                    printf("%c",'&');
////                else
//                //printf("%c",(*curbuf)[off+x]);
//            }
            memcpy((*curbuf)+off,a.front(),length);
            //printf("\n");
            break;
        }
    }
}

////???ȡ??ƫ?Ϊoff???????length???ַ??????ŵ?otherƫ?=off?ĵط???һ??Ƿ?c_sizeС??
void set_U1(char* a, bufferlist* other, int off, int length)
{
// offָlist???????????off????tr??curbuf??????ptr?ĵ????
    auto curbuf = other->begin();////cbegin???????????Ԫ???onst???????ֻ??
    while (off > 0 && off >= blocksize)   ////????????????
    {
        off -= blocksize;
        ++curbuf;
    }
    assert(length == 0 || curbuf != other->cend());

    while (length > 0)
    {
//        for(int x = 0; x<length; x++)
//        {
//            (*curbuf)[off+x] = a[off+x];
//            //printf("%c",(*curbuf)[off+x]);
//        }
        memcpy((*curbuf)+off,a+off,length);
        //printf("\n");
        break;
    }
}
////bufferlist??ܳ???һ??ufferlistֻ?һ??char*Ԫ?

////ע???ԭʼ??en??????len??????ubchunk_sizeΪ0ʱ
////????0????len?????????????Ϊ\0????ݵ?????len?????ȷ?ģ???????0?ĳ???
unsigned int len(const bufferlist buffers,int flag)
{
    unsigned len = 0;
    for (auto it = buffers.begin(); it != buffers.end(); it++)
    {
        ////Ӧ?û?????????elper?ڵ??????
        int j = 0;
        if(strlen(*it)<=blocksize&&flag==0)
        {
            for(int i = 0; i<blocksize; i++)
            {
                if((*it)[i]!='\0')
                    j = i;
            }
            len = len + j + 1;
            //printf("%d\n",len);
        }
        else
        {
            len += strlen(*it);
            if(helper_blocksize!=0)
            {
                if(len%helper_blocksize!=0)
                    len = len+helper_blocksize-len%helper_blocksize;
            }
        }
    }
    ////Ϊ??????Ĭ?len???????
    if(subchunk_size!=0)
    {
        if(len%subchunk_size!=0)
            len = len+subchunk_size-len%subchunk_size;
    }

    return len;
}

////??list<std::char *>ת??Ϊstring
char*  c_str(bufferlist p, int bsize)
{
    char *list_to_string = (char*)malloc(bsize+1);
    memset(list_to_string, 0, bsize+1);
    auto it = p.begin();
    int i;

    //for(i = 0; i<bsize; i++)
    //{
        //list_to_string[i] = (*it)[i];
    //}
    memcpy(list_to_string,*it,bsize);
    list_to_string[bsize] = '\0';

    return list_to_string;
}

////???ufferlist????????ʱ????list<std::char *>ת??Ϊstring
char*  c_str1(bufferlist p, int bsize)
{
    char *list_to_string = (char*)malloc(bsize+1);
    memset(list_to_string, 0, bsize);
    auto it = p.begin();
    int sub_bsize = bsize/(p.size());
    int i,j = 0;


    for(; it!=p.end(); it++)
    {
        //for(i = 0; i<sub_bsize; i++)
        //{
            //list_to_string[i+sub_bsize*j] = (*it)[i];
        //}
        memcpy(list_to_string+sub_bsize*j,*it,sub_bsize);
        j++;
    }
    list_to_string[bsize] = '\0';

    return list_to_string;
}

