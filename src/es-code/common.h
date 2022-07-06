#ifndef COMMON_H_INCLUDED
#define COMMON_H_INCLUDED
#include <list>
#include <stdio.h>
extern int blocksize,subchunk_size,helper_blocksize;
typedef std::list<char *> bufferlist;////对buffelist定义，即为多个字符串的列表
void substr_of1(bufferlist* a, const bufferlist& other, unsigned off, unsigned length,int blocksize = blocksize,int flag = 0);

void substr_of(bufferlist* a, const bufferlist& other, unsigned off, unsigned length,int blocksize = blocksize,int flag = 0);
////bufferlist的总长度
unsigned int len(const bufferlist buffers,int flag = 0);
////将list<std::char *>转换为string,list只有一个元素
char*  c_str(bufferlist p,int bsize) ;
////将list<std::char *>转换为string,list有多个元素
char*  c_str1(bufferlist p,int bsize) ;
void set_U(bufferlist a,bufferlist* other,int off,int length,int blocksize);
void set_U1(char* a, bufferlist* other, int off, int length);
#endif // COMMON_H_INCLUDED
