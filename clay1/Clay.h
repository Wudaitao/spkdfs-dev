#ifndef CLAY_H_INCLUDED
#define CLAY_H_INCLUDED
#include <iostream>
#include <map>
#include <set>
#include <vector>
#include <list>
#include <memory>
#include <string.h>
#include <string>
#include <cerrno>
#include <unistd.h>
#include <sys/time.h>
#include <errno.h>
#include <algorithm>
#include <malloc.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <fstream>
#include <ctime>
#include <sstream>
#include "Jerasure.h"
typedef std::map<std::string, std::string> ErasureCodeProfile;

class Clay
{
public:
    std::string DEFAULT_K{"4"};
    std::string DEFAULT_M{"2"};
    std::string DEFAULT_W{"8"};
    int k = 0, m = 0, d = 0, w = 8;////k要大于或等于2，m大于或等于1
    int q = 0, t = 0, nu = 0;/////n=nu+k+m=q*t
    int sub_chunk_no = 0;//////子块数=q^t
    //int blocksize = 0;////添加的变量
    std::map<int, bufferlist> U_buf;////未解耦的buffer

    struct ScalarMDS
    {
        char* erasure_code;
        ErasureCodeProfile profile;
    };
    ScalarMDS mds;
    ScalarMDS pft;
    const std::string directory;

    explicit Clay(const std::string& dir)
        : directory(dir)
    {}

    ~Clay() ;
    /* */
    void set_blocksize(int chunk_size);

    int init(ErasureCodeProfile &profile, std::ostream *ss) ;

    virtual int parse(ErasureCodeProfile &profile, std::ostream *ss);

    unsigned int get_chunk_count() const
    {
        return k + m;
    }

    unsigned int get_data_chunk_count() const
    {
        return k;
    }

    int get_sub_chunk_count()
    {
        return sub_chunk_no;
    }

    unsigned  int get_chunk_size(unsigned int object_size) ;

    int encode_prepare(const bufferlist &raw, std::map<int, bufferlist> &encoded, unsigned int data_len) ;////

    int encode(const std::set<int> &want_to_encode, char *fname) ;////

    int encode_chunks(const std::set<int> &want_to_encode, std::map<int, bufferlist> *encoded) ;

    int _decode(const set<int> &want_to_read,char* fname);////

    int decode_chunks(const std::set<int> &want_to_read,const std::map<int, bufferlist> &chunks,std::map<int, bufferlist> *decoded) ;

    int minimum_to_decode(const std::set<int> &want_to_read,const std::set<int> &available,std::map<int, std::vector<std::pair<int, int>>> *minimum);

    int minimum_to_decode1(const set<int> &want_to_read,const set<int> &available_chunks,map<int, vector<pair<int, int>>> *minimum);////

    int is_repair(const std::set<int> &want_to_read,const std::set<int> &available_chunks);

    int decode(const std::set<int> &want_to_read,set<int> &available, int chunk_size,char* fname);

    int get_repair_sub_chunk_count(const std::set<int> &want_to_read);

public:
    /* */
    //int decode_erasures(const std::set<int>& erased_chunks, int z, std::map<int, bufferlist>* chunks, int sc_size);

    int decode_uncoupled(const std::set<int>& erasures, int z, int sc_size);

    int decode_layered(std::set<int>& erased_chunks, std::map<int, bufferlist>* chunks);

    void set_planes_sequential_decoding_order(int* order, std::set<int>& erasures);

    void recover_type1_erasure(std::map<int, bufferlist>* chunks, int x, int y, int z, int* z_vec, int sc_size);

     void get_uncoupled_from_coupled(std::map<int, bufferlist>* chunks, int x, int y, int z, int* z_vec, int sc_size);

    void get_coupled_from_uncoupled(std::map<int, bufferlist>* chunks, int x, int y, int z, int* z_vec, int sc_size);

    void get_plane_vector(int z, int* z_vec);

    int get_max_iscore(std::set<int>& erased_chunks);

    int minimum_to_repair(const std::set<int> &want_to_read,const std::set<int> &available_chunks,std::map<int, std::vector<std::pair<int, int>>> *minimum);

    void get_repair_subchunks(const int &lost_node,std::vector<std::pair<int, int>> &repair_sub_chunks_ind);

    int _minimum_to_decode(const set<int> &want_to_read,const set<int> &available_chunks,set<int> *minimum);////

    int repair(const std::set<int> &want_to_read,const std::map<int, bufferlist> &chunks,std::map<int, bufferlist> *recovered, int chunk_size);

    int repair_one_lost_chunk(std::map<int, bufferlist> &recovered_data, std::set<int> &aloof_nodes,std::map<int, bufferlist> &helper_data, int repair_blocksize,std::vector<std::pair<int,int>> &repair_sub_chunks_ind);

};

#endif // CLAY_H_INCLUDED

