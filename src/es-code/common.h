#ifndef COMMON_H_INCLUDED
#define COMMON_H_INCLUDED
#include <list>
#include <stdio.h>
extern int blocksize,subchunk_size,helper_blocksize;
typedef std::list<char *> bufferlist;////��buffelist���壬��Ϊ����ַ������б�
void substr_of1(bufferlist* a, const bufferlist& other, unsigned off, unsigned length,int blocksize = blocksize,int flag = 0);

void substr_of(bufferlist* a, const bufferlist& other, unsigned off, unsigned length,int blocksize = blocksize,int flag = 0);
////bufferlist���ܳ���
unsigned int len(const bufferlist buffers,int flag = 0);
////��list<std::char *>ת��Ϊstring,listֻ��һ��Ԫ��
char*  c_str(bufferlist p,int bsize) ;
////��list<std::char *>ת��Ϊstring,list�ж��Ԫ��
char*  c_str1(bufferlist p,int bsize) ;
void set_U(bufferlist a,bufferlist* other,int off,int length,int blocksize);
void set_U1(char* a, bufferlist* other, int off, int length);
#endif // COMMON_H_INCLUDED
