#include <dfs_dbg.h>
#include "Clay.h"
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)
#define LARGEST_VECTOR_WORDSIZE 16
#define talloc(type, num) (type *) malloc(sizeof(type)*(num))//宏定义，取别名
#define SIMD_ALIGN 32
extern int blocksize,subchunk_size,helper_blocksize;////引用main里的全局变量，并进行修改，全局生效
extern void EXPECT_EQ(int a, int b);
////计算a^x
static int pow_int(int a, int x)
{
    int power = 1;
    while (x)
    {
        if (x & 1)
            power *= a;
        x /= 2;
        a *= a;
    }
    return power;
}

static int to_int(const std::string &name, ErasureCodeProfile &profile, int *value, const std::string &default_value)
{
    if (profile.find(name) == profile.end() ||
            profile.find(name)->second.size() == 0)
        profile[name] = default_value;
    std::string p = profile.find(name)->second;
    *value = atoi(p.c_str());////p要求char*，不知道会不会有问题？
    return 0;
}

int Clay::parse(ErasureCodeProfile & profile, ostream * ss)
{
    to_int("k", profile, &k, DEFAULT_K);
    to_int("m", profile, &m, DEFAULT_M);
    to_int("d", profile, &d, std::to_string(k + m - 1));

    // check for scalar_mds in profile input
    if (profile.find("scalar_mds") == profile.end() || profile.find("scalar_mds")->second.empty())
    {
        mds.profile["plugin"] = "jerasure";
        pft.profile["plugin"] = "jerasure";
    }
    else
    {
        std::string p = profile.find("scalar_mds")->second;
        if ((p == "jerasure") || (p == "isa") || (p == "shec"))
        {
            mds.profile["plugin"] = p;
            pft.profile["plugin"] = p;
        }
        else
        {
            *ss << "scalar_mds " << mds.profile["plugin"] <<
                "is not currently supported, use one of 'jerasure'," <<" 'isa', 'shec'" << std::endl;
            return 1;
        }
    }

    if (profile.find("technique") == profile.end() ||
            profile.find("technique")->second.empty())
    {
        if ((mds.profile["plugin"] == "jerasure") || (mds.profile["plugin"] == "isa") )
        {
            mds.profile["technique"] = "reed_sol_van";
            pft.profile["technique"] = "reed_sol_van";
        }
        else
        {
            mds.profile["technique"] = "single";
            pft.profile["technique"] = "single";
        }
    }
    else
    {
        std::string p = profile.find("technique")->second;
        if (mds.profile["plugin"] == "jerasure")
        {
            if ( (p == "reed_sol_van") || (p == "reed_sol_r6_op") || (p == "cauchy_orig") || (p == "cauchy_good") || (p == "liber8tion"))
            {
                mds.profile["technique"] = p;
                pft.profile["technique"] = p;
            }
            else
            {
                *ss << "technique " << p << "is not currently supported, use one of "
                    << "reed_sol_van', 'reed_sol_r6_op','cauchy_orig',"
                    << "'cauchy_good','liber8tion'" << std::endl;
                return 1;
            }
        }
        else if (mds.profile["plugin"] == "isa")
        {
            if ( (p == "reed_sol_van") || (p == "cauchy"))
            {
                mds.profile["technique"] = p;
                pft.profile["technique"] = p;
            }
            else
            {
                *ss << "technique " << p << "is not currently supported, use one of"
                    << "'reed_sol_van','cauchy'" << std::endl;
                return 1;
            }
        }
        else
        {
            if ( (p == "single") || (p == "multiple"))
            {
                mds.profile["technique"] = p;
                pft.profile["technique"] = p;
            }
            else
            {
                *ss << "technique " << p << "is not currently supported, use one of" <<
                    "'single','multiple'" << std::endl;
                return 1;
            }
        }
    }
    if ((d < k) || (d > k + m - 1))
    {
        *ss << "value of d " << d
            << " must be within [ " << k << "," << k + m - 1 << "]" << std::endl;
        return 1;
    }

    ////clay再生码和其他码的区别，多了q和t
    q = d - k + 1;////设置d时q不能为0，程序会终止
    ////nu
    if ((k + m) % q)
    {
        nu = q - (k + m) % q;
    }
    else
    {
        nu = 0;////控制d，让nu为0，程序编码才正确
    }

    if (k + m + nu > 254)
    {
        return 1;
    }

    if (mds.profile["plugin"] == "shec")
    {
        mds.profile["c"] = "2";
        pft.profile["c"] = "2";
    }
    mds.profile["k"] = std::to_string(k + nu);////2转换为"2"
    mds.profile["m"] = std::to_string(m);
    mds.profile["w"] = "8";

    pft.profile["k"] = "2";
    pft.profile["m"] = "2";
    pft.profile["w"] = "8";

    t = (k + m + nu) / q;////t
    sub_chunk_no = pow_int(q, t);////子块个数sub_chunk_no和d有关

    cout << "Parse parameters:" << " (k,m,w,d,q,t,nu,sub_chunk_no)=(" << k << "," << m << "," << w << "," << d << "," << q << "," << t << "," << nu << "," << sub_chunk_no << ")" << endl;

    return 0;
}

int Clay::init(ErasureCodeProfile & profile, ostream * ss)
{
    int r;
    r = parse(profile, ss);

    return r;

}

Clay::~Clay()
{
    for (int i = 0; i < q * t; i++)
    {
        if (len(U_buf[i]) != 0)
            U_buf[i].clear();
    }
}
void Clay::set_blocksize(int chunk_size)
{
    blocksize = chunk_size;
}

int round_up_to(int n, int d)
{
    return (n % d ? (n + d - n % d) : n);
}

unsigned int Clay::get_chunk_size(unsigned int object_size)
{
    unsigned int sub_size = object_size / (sub_chunk_no * k);
    if(object_size % (sub_chunk_no * k) != 0)
        sub_size++;
    blocksize = sub_size * sub_chunk_no;
    subchunk_size = sub_size;
    //std::cout << "Get_chunk_size:" << blocksize << std::endl;

    return blocksize;//包含结束符
}
////把数据放入encoded里，先根据blocksize算出有多少完整块，完整块直接放入encoded里
////第一个块为其分配blocksize的内存，后面加0放到encoded，后面的padding块全部置0安装blocksize放到encoded里
////布置好k+m个块
int Clay::encode_prepare(const bufferlist &raw, map<int, bufferlist> &encoded, unsigned int data_len)
{
    unsigned int k = get_data_chunk_count();
    unsigned int m = get_chunk_count() - k;
    //unsigned int data_len = len(raw);
    unsigned chunk_size = get_chunk_size(data_len);
    unsigned padded_chunks = data_len >= chunk_size ? k - data_len / chunk_size : k ;
    bufferlist prepared = raw;////原始数据
    std::cout << "encode_prepare"<< endl;
    for (unsigned int i = 0; i < k - padded_chunks; i++)
    {
        substr_of1(&encoded[i], prepared, i * chunk_size, chunk_size,data_len); ////将数据块放入encoded数组中
        for (auto it = encoded[i].begin(); it != encoded[i].end(); it++)
        {
            //cout << *it << endl;
        }
    }
    if (padded_chunks)
    {
        unsigned remainder = data_len - (k - padded_chunks) * chunk_size;
        char* buf = (char*)malloc(chunk_size + 1);
        char* ending = raw.back();
        memcpy(buf, ending + data_len - remainder, remainder);
        memset(buf + remainder, 0, chunk_size - remainder);  ////将padding部分置0
        encoded[k - padded_chunks].push_back(std::move(buf)); ////////添加padding的第一个数据块放入encoded
        int length;
        for (unsigned int i = k - padded_chunks + 1; i < k; i++)
        {
            char* buf = (char*)malloc(chunk_size + 1); ////对齐
            memset(buf, 0, chunk_size + 1);
            encoded[i].push_back(std::move(buf));////////添加padding的后面的块全部为0，放入encoded
        }
    }
    for (unsigned int i = k; i < k + m; i++)
    {
        char* buf = (char*)malloc(chunk_size + 1);
        memset(buf, 0, chunk_size + 1);
        encoded[i].push_back(buf);////校验块
    }
    int length = len(encoded[0]);
    return 0;
}


int Clay::encode(const set<int> &want_to_encode,char *fname)
{
    unsigned int k = get_data_chunk_count();
    unsigned int m = get_chunk_count() - k;
    ////读待编码文件
    map<int, bufferlist> encoded;
    unsigned int data_size = 0;
    std::ifstream fp;////必须得是ifstream不能是fstream
    

    fp.open(fname, ios::ate| ios::binary);
    if(!fp)
    {
        std::cerr << "open file error: file " << fname << std::endl;
    }
    data_size = fp.tellg();
	char *payload[data_size+1];
    
    memset(payload,0,data_size+1);
    fp.seekg(0, fp.beg);////beg代表文件开头，从开头移动0，即将is定位到开头
    fp.read(payload, data_size);
    fp.close();
    //cout<<"length:"<<data_size<<endl;
    bufferlist in;
    in.push_back(payload);
    char *data = c_str(in,data_size);

    int err = encode_prepare(in, encoded,data_size);
	if (err)
        return err;
    ////写入编码块变量s
    std::ofstream fp2;
	char fname2[256];
        
    encode_chunks(want_to_encode, &encoded);

    //std::cout<<"encode"<<std::endl;
    //std::cout << "encode:end" << std::endl;


//    ////测试
//    for (int i = 0; i < k + m; i++)
//    {
//        //printf("%s:%s\n","U_buf",c_str(U_buf[i], blocksize + 1));
//    }

    //EXPECT_EQ(6u, encoded.size());
    unsigned length =  len(encoded[0]);
	cout<<blocksize<<" "<<length<<" "<<endl;
    //printf("%s\n%s\n",encoded[0].front(), data);
    EXPECT_EQ(0, memcmp(encoded[0].front(), data,length));
    EXPECT_EQ(0, memcmp(encoded[1].front(), data+length, length));
    EXPECT_EQ(0, memcmp(encoded[2].front(), data+2*length,data_size-2*length));

//    for (int i = 0; i < k + m; i++)
//    {
//        if (i < k)
//        {
//            //printf("%s:%s\n","data block",c_str((*encoded)[i], blocksize ));
//        }
//        else
//        {
//            //printf("%s:%s\n","parity block",c_str((*encoded)[i], blocksize ));
//        }
//    }

    for	(int i = 0; i <get_chunk_count(); i++)
    {
        sprintf(fname2, "%s_%d",fname, i+1);
		//cout << fname2 << " " << blocksize <<std::endl;
        fp2.open(fname2, ios::ate | ios::binary);
        if(!fp2)
            std::cerr << "create file failed. maybe wrong directory." << std::endl;
		dbg(encoded[i].front());
	
        fp2.write(encoded[i].front(),blocksize);
        fp2.close();
        free(encoded[i].front());
        encoded[i].clear();

    }
    
    in.clear();
    free(data);

    ////free U_buf
    for (int i = 0; i < q*t; i++)
    {
        if(U_buf[i].size()!=0)
        {
            free(U_buf[i].front());
            U_buf[i].clear();
        }
    }

    return 0;

}
////布置好k+m+nu个块
int Clay::encode_chunks(const set<int> &want_to_encode, map<int, bufferlist> *encoded)
{
    map<int, bufferlist> chunks;////暂存encoded，中间加了0，共有k+m+nu个元素,修改chunks会改变encoded的数据
    set<int> parity_chunks;
    int chunk_size = blocksize;
    //std::cout << "encode_chunks:" << chunk_size << " " << sub_chunk_no << std::endl;
    for (int i = 0; i < k + m; i++)
    {
        if (i < k)
        {
            chunks[i] = (*encoded)[i];////
        }
        else
        {
            chunks[i + nu] = (*encoded)[i]; ////nu是什么意思？？？赋的是校验块的部分
            parity_chunks.insert(i + nu); ////校验块的索引位置
        }
    }
    int length;
    for (int i = k; i < k + nu; i++)
    {
        char *buf = (char*)malloc(chunk_size + 1);
        memset(buf, 0, chunk_size + 1);
        chunks[i].push_back(std::move(buf));////在vector的尾部添加char*数据，0~k-1部分是数据块，k~k+nu-1是0，k+nu~k+m-1+nu是校验块
    }

    for (int i = 0; i < q * t; i++)   /////对为空的未解耦buffer设置0
    {
        if(U_buf[i].size()!=0) ////有问题！！！
        {
            U_buf[i].erase(U_buf[i].begin());
        }
        char *buf = (char*)malloc(chunk_size + 1);
        memset(buf, 0, chunk_size + 1);
        U_buf[i].push_back(std::move(buf));////修改buf的内存地址，由U_buf[i]看管

    }

    int res = decode_layered(parity_chunks, &chunks);
    for (int i = k ; i < k + nu; i++)
    {
        // need to clean some of the intermediate chunks here!!
        free(chunks[i].front());
        chunks[i].clear();////去掉中间0的部分
    }

    for (int i = 0; i < k + m; i++)
    {
        (*encoded)[i] = chunks[i];
    }

    return res;
}
////编码erased_chunks是2,3，解码是丢失的块的索引
int Clay::decode_layered(set<int> &erased_chunks, map<int, bufferlist> *chunks)   ////erased应该对应的是擦除的数据块，但是encode_chunks调用这个函数为什么要用这个擦除的？？？
{
    int num_erasures = erased_chunks.size();////校验块个数

    int chunk_size = blocksize;
    assert(chunk_size % sub_chunk_no == 0); /////表达式为true则继续往下执行
    int sc_size = chunk_size / sub_chunk_no;////计算子块size
    //std::cout << "decode_layered########" << std::endl;

    assert(num_erasures > 0);

    for (int i = k + nu; (num_erasures < m) && (i < q * t); i++)
    {
        erased_chunks.emplace(i);////会把中间nu个空节点加入待修复节点队列
        num_erasures++; // silence -Wunused-variable
    }

    assert(num_erasures == m);////必须得m个块丢失,上面的for循环可以保证num_erasures = m

    int max_iscore = get_max_iscore(erased_chunks);////设置IS，获取最大的IS数

    int order[sub_chunk_no];
    int z_vec[t];
    int length;

    set_planes_sequential_decoding_order(order, erased_chunks);////根据IS设置解码顺序
    for (int iscore = 0; iscore <= max_iscore; iscore++)
    {
        for (int z = 0; z < sub_chunk_no; z++)
        {
            if (order[z] == iscore)
            {
                //std::cout << "iscore："<< iscore <<" 第z次："<< z << std::endl;
                decode_erasures(erased_chunks, z, chunks, sc_size);////调用get_uncoupled_from_coupled函数，每个平面按照mds的解码函数解码,得到所有U
            }
        }

        for (int z = 0; z < sub_chunk_no; z++)  ////未解耦的z
        {
            if (order[z] == iscore)
            {
                get_plane_vector(z, z_vec);////给z和z_vec赋值，z_vec数组根据z值获得
                for (auto node_xy : erased_chunks)  ////块索引和x，y的转换
                {
                    int x = node_xy % q;////x
                    int y = node_xy / q;////y
                    int node_sw = y * q + z_vec[y];
                    if (z_vec[y] != x)
                    {
                        if (erased_chunks.count(node_sw) == 0)
                        {
                            //cout<<"decode_layered "<<1<<endl;
                            recover_type1_erasure(chunks, x, y, z, z_vec, sc_size);////调用pft的解码函数解码
                        }
                        else if (z_vec[y] < x)
                        {
                            assert(erased_chunks.count(node_sw) > 0);
                            assert(z_vec[y] != x);
                            //cout<<"decode_layered "<<2<<endl;
                            get_coupled_from_uncoupled(chunks, x, y, z, z_vec, sc_size);////调用get_coupled_from_uncoupled函数,从U到C，用pft解码
                        }
                    }
                    else
                    {
                        ////x=z_vec[y]对应论文x=zy,这部分顶点是未配对的，C=U
                        //cout<<"decode_layered "<<3<<" node_xy: "<<node_xy<<" node_sw: "<<node_sw<<" z: "<<z<<endl;
                        char* U = c_str(U_buf[node_xy], blocksize + 1);
                        //std::cout<<"设置第"<<node_xy<<"个C_buf偏移为"<<z * sc_size<<"长度为"<<sc_size<<" "<<std::endl;
                        set_U1(U,&(*chunks)[node_xy], z * sc_size, sc_size);////对耦合块chunks赋值
                        free(U);
                    }
                }
            }
        } // plane 得到了该平面的所有C
    } // iscore, order

    return 0;
}

int Clay::decode_erasures(const set<int>& erased_chunks, int z, map<int, bufferlist>* chunks, int sc_size)
{
    int z_vec[t];
    //std::cout << "#decode_erasures#" << std::endl;
    get_plane_vector(z, z_vec);
    for (int x = 0; x < q; x++)
    {
        for (int y = 0; y < t; y++)
        {
            int node_xy = q * y + x;
            int node_sw = q * y + z_vec[y];
            ////未丢失耦合块得出相应的未耦合块
            if (erased_chunks.count(node_xy) == 0)
            {
                if (z_vec[y] < x)
                {
                    //cout << 1 << " x,y,z_vec[y] " << x << y << z_vec[y] << endl;
                    get_uncoupled_from_coupled(chunks, x, y, z, z_vec, sc_size);
                }
                else if (z_vec[y] == x)    ////x=z_vec[y]对应论文x=zy,这部分顶点是未配对的，C=U
                {
                    //cout << 2 << " x,y,z_vec[y] " << x << y << z_vec[y] << endl;
                    //cout << "node_xy: " << node_xy << " z: " << z << endl;
                    char* coupled_chunk = c_str((*chunks)[node_xy], blocksize + 1);////看看coupled_chunk是不是为null
                    //std::cout<<"设置第"<<node_xy<<"个U_buf偏移为"<<z * sc_size<<"长度为"<<sc_size<<" "<<std::endl;
                    set_U1(coupled_chunk, &U_buf[node_xy], z * sc_size, sc_size);////设置U   C和U的偏移均为z*sc_size
                    free(coupled_chunk);
                }
                else
                {
                    //cout << 3 << " x,y,z_vec[y] " << x << y <<  z_vec[y] << endl;
                    if (erased_chunks.count(node_sw) > 0)
                    {
                        //cout << 3 << " x,y,z_vec[y] " << x << y <<  z_vec[y] << endl;
                        get_uncoupled_from_coupled(chunks, x, y, z, z_vec, sc_size);
                    }
                }
            }
        }
    }
    //cout << "end" << endl;
    return decode_uncoupled(erased_chunks, z, sc_size);////mds的解码
}

void Clay::get_coupled_from_uncoupled(map<int, bufferlist>* chunks, int x, int y, int z, int* z_vec, int sc_size)
{
    ////0,1是两个coupled块，2,3是两个uncoupled块，已知uncoupled块，调用（4,2）纠删码来恢复两个coupled块
    //std::cout << "get_coupled_from_uncoupled" << std::endl;
    int a[] = {0, 1};
    set<int> erased_chunks(a, a + 2); ////对应pftsubchunks的0,1块丢失即要求的coupled块

    int node_xy = y * q + x;
    int node_sw = y * q + z_vec[y];
    int z_sw = z + (x - z_vec[y]) * pow_int(q, t - 1 - y);

    assert(z_vec[y] < x);
    map<int, bufferlist> uncoupled_subchunks;
    substr_of(&uncoupled_subchunks[2], U_buf[node_xy], z * sc_size, sc_size);////
    substr_of(&uncoupled_subchunks[3], U_buf[node_sw], z_sw * sc_size, sc_size);

    map<int, bufferlist> pftsubchunks;
    substr_of(&pftsubchunks[0], (*chunks)[node_xy], z * sc_size, sc_size);
    substr_of(&pftsubchunks[1], (*chunks)[node_sw], z_sw * sc_size, sc_size);
    pftsubchunks[2] = uncoupled_subchunks[2];
    pftsubchunks[3] = uncoupled_subchunks[3];

    scalar_decode_chunks(atoi(pft.profile["k"].c_str()), atoi(pft.profile["m"].c_str()), atoi(pft.profile["w"].c_str()), erased_chunks, uncoupled_subchunks, &pftsubchunks, blocksize / sub_chunk_no); ////0,1块丢失，根据2,3块来恢复C，为什么pft的转换使用纠删码来做？？？？
    //cout << "node_xy:" << node_xy << "node_sw:" << node_sw << "z_sw:" << z_sw << "z:" << z << endl;
    //std::cout<<"设置第"<<node_xy<<"个C_buf偏移为"<<z * sc_size<<"长度为"<<sc_size<<" "<<std::endl;
    set_U(pftsubchunks[0], &(*chunks)[node_xy], z * sc_size, sc_size, blocksize);

    //std::cout<<"设置第"<<node_sw<<"个C_buf偏移为"<<z_sw * sc_size<<"长度为"<<sc_size<<" "<<std::endl;
    set_U(pftsubchunks[1], &(*chunks)[node_sw], z_sw * sc_size, sc_size, blocksize);

    free(pftsubchunks[0].front());
    free(pftsubchunks[1].front());
    free(uncoupled_subchunks[2].front());
    free(uncoupled_subchunks[3].front());
}

void Clay::get_uncoupled_from_coupled(map<int, bufferlist>* chunks, int x, int y, int z, int* z_vec, int sc_size)
{
    //std::cout << "get_uncoupled_from_coupled" << std::endl;
    set<int> erased_chunks = {2, 3}; ////对应pftsubchunks的2,3块丢失
    int node_xy = y * q + x;
    int node_sw = y * q + z_vec[y];
    int z_sw = z + (x - z_vec[y]) * pow_int(q, t - 1 - y);

    int i0 = 0, i1 = 1, i2 = 2, i3 = 3;
    if (z_vec[y] > x)
    {
        i0 = 1;
        i1 = 0;
        i2 = 3;
        i3 = 2;
    }
    map<int, bufferlist> coupled_subchunks;
    //std::cout << "node_xy: " << node_xy << " node_sw: " << node_sw << " z_sw: " << z_sw << " z: " << z << std::endl;
    substr_of(&coupled_subchunks[i0], (*chunks)[node_xy], z * sc_size, sc_size);
    substr_of(&coupled_subchunks[i1], (*chunks)[node_sw], z_sw * sc_size, sc_size);

    map<int, bufferlist> pftsubchunks;
    pftsubchunks[0] = coupled_subchunks[0];
    pftsubchunks[1] = coupled_subchunks[1];
    substr_of(&pftsubchunks[i2], U_buf[node_xy], z * sc_size, sc_size);
    substr_of(&pftsubchunks[i3], U_buf[node_sw], z_sw * sc_size, sc_size);

    scalar_decode_chunks(atoi(pft.profile["k"].c_str()), atoi(pft.profile["m"].c_str()), atoi(pft.profile["w"].c_str()), erased_chunks, coupled_subchunks, &pftsubchunks, blocksize / sub_chunk_no); //////与get_coupled_from_uncoupled相反

    //std::cout << "node_xy: " << node_xy << " node_sw: " << node_sw << " z_sw: " << z_sw << " z: " << z <<" i2: "<<i2<<" i3: "<<i3<<std::endl;
    //std::cout<<"设置第"<<node_xy<<"个U_buf偏移为"<<z * sc_size<<"长度为"<<sc_size<<" "<<std::endl;
    set_U(pftsubchunks[i2], &U_buf[node_xy], z * sc_size, sc_size, blocksize);
    //std::cout<<"设置第"<<node_sw<<"个U_buf偏移为"<<z_sw * sc_size<<"长度为"<<sc_size<<" "<<std::endl;
    set_U(pftsubchunks[i3], &U_buf[node_sw], z_sw * sc_size, sc_size, blocksize);
    free(pftsubchunks[2].front());
    free(pftsubchunks[3].front());
    free(coupled_subchunks[0].front());
    free(coupled_subchunks[1].front());
}

int Clay::decode_uncoupled(const set<int>& erased_chunks, int z, int sc_size)
{
    map<int, bufferlist> known_subchunks;
    map<int, bufferlist> all_subchunks;
    //std::cout << "decode_uncoupled" << std::endl;

    for (int i = 0; i < q * t; i++)
    {
        if (erased_chunks.count(i) == 0)
        {
            substr_of(&known_subchunks[i], U_buf[i], z * sc_size, sc_size); ////后赋给前
            all_subchunks[i] = known_subchunks[i];/////对all_subchunks赋值
        }
        else
            substr_of(&all_subchunks[i], U_buf[i], z * sc_size, sc_size);
        assert(all_subchunks[i].size() <= 1);
    }

    scalar_decode_chunks(atoi(mds.profile["k"].c_str()), atoi(mds.profile["m"].c_str()), atoi(mds.profile["w"].c_str()), erased_chunks, known_subchunks, &all_subchunks, blocksize / sub_chunk_no); ////调用Jerasue的jerasure_matrix_decode函数，返回all_subchunks包含所有块，

    ////新添加
    for (int i = 0; i < q * t; i++)
    {
        if (erased_chunks.count(i) != 0)
        {
            //cout<<"设置第"<<i<<"个U_buf偏移为"<<z * sc_size<<"长度为"<<sc_size<<" "<<endl;
            set_U(all_subchunks[i], &U_buf[i], z * sc_size, sc_size, blocksize);
            free(all_subchunks[i].front());
        }
        else
        {
            free(known_subchunks[i].front());
        }
    }

    return 0;
}

void Clay::recover_type1_erasure(map<int, bufferlist>* chunks, int x, int y, int z, int* z_vec, int sc_size)
{
    set<int> erased_chunks;
    //std::cout<<"#recover_type1_erasure#"<<std::endl;
    int node_xy = y * q + x;
    int node_sw = y * q + z_vec[y];
    int z_sw = z + (x - z_vec[y]) * pow_int(q, t - 1 - y);
    int length;
    map<int, bufferlist> known_subchunks;
    map<int, bufferlist> pftsubchunks;
    char *ptr = (char*)malloc(sc_size * sizeof(char)); //修改上面的对齐函数
    ////修改上面的置0函数
    memset(ptr, 0, sc_size);
    int i0 = 0, i1 = 1, i2 = 2, i3 = 3;
    if (z_vec[y] > x)
    {
        i0 = 1;
        i1 = 0;
        i2 = 3;
        i3 = 2;
    }

    erased_chunks.insert(i0);
    substr_of(&pftsubchunks[i0], (*chunks)[node_xy], z * sc_size, sc_size); ////(x,y,z)对应的块
    substr_of(&known_subchunks[i1], (*chunks)[node_sw], z_sw * sc_size, sc_size); ////添加substr_of函数，取src的子字符串到dest：（dest，src，off，len）
    substr_of(&known_subchunks[i2], U_buf[node_xy], z * sc_size, sc_size);
    pftsubchunks[i1] = known_subchunks[i1];
    pftsubchunks[i2] = known_subchunks[i2];
    pftsubchunks[i3].push_back(ptr);////pftsubchunks[i0,i1,i2,i3]赋值

    ////erased_chunks丢失的块,known_subchunks现有的块,pftsubchunks恢复的原始数据块
    scalar_decode_chunks(atoi(pft.profile["k"].c_str()), atoi(pft.profile["m"].c_str()), atoi(pft.profile["w"].c_str()), erased_chunks, known_subchunks, &pftsubchunks, blocksize / sub_chunk_no); ////jerasure_matrix_decode

    //cout << "node_xy:" << node_xy << "node_sw:" << node_sw << "z_sw:" << z_sw << "z:" << z << endl;
    //std::cout<<"设置第"<<node_xy<<"个C_buf偏移为"<<z * sc_size<<"长度为"<<sc_size<<" "<<std::endl;
    set_U(pftsubchunks[i0], &(*chunks)[node_xy], z * sc_size, sc_size, blocksize);
    free(ptr);
    free(pftsubchunks[i0].front());
    free(known_subchunks[i1].front());
    free(known_subchunks[i2].front());
}

void Clay::set_planes_sequential_decoding_order(int* order, set<int>& erasures)
{
    int z_vec[t];
    for (int z = 0; z < sub_chunk_no; z++)
    {
        get_plane_vector(z, z_vec); ////对z和z_vev赋值
        order[z] = 0;
        for (auto i : erasures)
        {
            if (i % q == z_vec[i / q])
            {
                order[z] = order[z] + 1;////IS加一
            }
        }
    }
}

int Clay::get_max_iscore(set<int>& erased_chunks)
{
    int weight_vec[t];
    int iscore = 0;
    memset(weight_vec, 0, sizeof(int)*t);

    for (auto i : erased_chunks)
    {
        if (weight_vec[i / q] == 0)
        {
            weight_vec[i / q] = 1;
            iscore++;////IS没看懂？？？按照既定规则自己设计的？？？
        }
    }
    return iscore;
}

void Clay::get_plane_vector(int z, int* z_vec)
{
    for (int i = 0; i < t; i++)
    {
        z_vec[t - 1 - i] = z % q;
        z = (z - z_vec[t - 1 - i]) / q;
    }
}

////解码，chunks已知的块，decoded是解码的所有块
int Clay::_decode(const set<int> &want_to_read,char* fname)
{
    char *curdir1 = (char*)malloc(sizeof(char)*1000);
    getcwd(curdir1, 1000);
    map<int, bufferlist> degraded;
map<int, bufferlist> decoded;
    std::ifstream fp3;////必须得是ifstream不能是fstream
    char *fname3 = (char*)malloc(sizeof(char)*(strlen(curdir1)+10));
    memset(fname3,0,strlen(curdir1)+10);
        ////解码文件
        char *fname4 = (char*)malloc(sizeof(char)*(strlen(curdir1)+10));
        memset(fname4,0,strlen(curdir1)+10);
        std::ofstream fp4;
char *filename = (char*)malloc(sizeof(char)*strlen(fname)+1);
int i;
for(i = 0;i<strlen(fname);i++){
if(fname[i]!='.')
filename[i] = fname[i];
else
break;
}
filename[i] = '\0';


    for (int i = 0; i < get_chunk_count(); i++)
    {
        sprintf(fname3, "%s/Coding/%s_%d.txt", curdir1, filename, i+1); 
        fp3.open(fname3);
        if(!fp3)
        {
            continue;
        }
        char *buf = (char*)malloc(sizeof(char)*(1+blocksize));
        memset(buf,0,blocksize+1);
        fp3.read(buf,blocksize);
        //printf("%s\n",buf);
        degraded[i].push_back(buf);
        fp3.close();
    }

    vector<int> have;
    have.reserve(degraded.size());//为vector预分配空间，但是不初始化，无法访问其数据

    for (map<int, bufferlist>::const_iterator i = degraded.begin(); i != degraded.end(); ++i)
    {
        have.push_back(i->first);////把已有的块序号存入have
    }
    ////想读取的块未丢失，直接赋值返回
    if (includes(have.begin(), have.end(), want_to_read.begin(), want_to_read.end()))////includes函数判断后者是否为前者的子序列,参数为迭代器
    {
        for (set<int>::iterator i = want_to_read.begin(); i != want_to_read.end(); ++i)
        {
            decoded[*i] = degraded.find(*i)->second;////将已有的块放入decoded中,包括want_to_decode和已有的块
        }
        return 0;
    }

    unsigned int k = get_data_chunk_count();
    unsigned int m = get_chunk_count() - k;

    for (unsigned int i =  0; i < k + m; i++)
    {
        if (degraded.find(i) == degraded.end())
        {
            char *tmp= (char *) malloc((blocksize+1)*sizeof(char));
            memset(tmp, 0, blocksize+1);
            decoded[i].push_back(std::move(tmp));////丢失的块初始化为0
        }
        else
        {
            decoded[i] = degraded.find(i)->second;////未丢失的块赋值
        }
    }

    int err = decode_chunks(want_to_read, degraded, &decoded);

        for (set<int>::iterator it = want_to_read.begin();it!=want_to_read.end();it++)
        {

            sprintf(fname4, "%s/Coding/%s_%d.txt", curdir1, filename, (*it)+1);
            fp4.open(fname4);
            if(!fp4)
                std::cerr << "create file failed. maybe wrong directory." << std::endl;
            fp4.write(decoded[*it].front(),blocksize);
            fp4.close();
        }
        for(int i = 0; i<get_chunk_count(); i++)
        {
            if(decoded.find(i)==decoded.end())
                continue;
            else
            {
                free(decoded[i].front());
                decoded[i].clear();
            }
        }
            free(curdir1);
free(fname3);
        free(fname4);

return err;
}

////chunks已知的块，decoded是解码的所有块
int Clay::decode_chunks(const set<int> &want_to_read,const map<int, bufferlist> &chunks,map<int, bufferlist> *decoded)
{
    //cout<<"decode_chunks"<<endl;
    set<int> erasures;////丢失的块索引
    map<int, bufferlist> coded_chunks;////保存现有的块
    ////erasures和coded_chunks均添加中间的nu位
    for (int i = 0; i < k + m; i++)
    {
        if (chunks.count(i) == 0)
        {
            erasures.insert(i < k ? i : i+nu);
        }
        coded_chunks[i < k ? i : i+nu] = (*decoded)[i];
    }
    int chunk_size = blocksize;

    ////中间的nu位置0
    for (int i = k; i < k+nu; i++)
    {
        char *buf = (char*)malloc(chunk_size*sizeof(char));
        memset(buf,0,chunk_size);
        coded_chunks[i].push_back(buf);
    }


    for (int i = 0; i < q * t; i++)   /////对为空的未解耦buffer设置0
    {
        if(U_buf[i].size()!=0) ////有问题！！！
        {
            U_buf[i].erase(U_buf[i].begin());
        }
        char *buf = (char*)malloc(blocksize + 1);
        memset(buf, 0, blocksize + 1);
        U_buf[i].push_back(std::move(buf));////修改buf的内存地址，由U_buf[i]看管
    }

    ////调用decode_layered，再删除中间的nu位
    int res = decode_layered(erasures, &coded_chunks);
    for (int i = k; i < k+nu; i++)
    {
        free(coded_chunks[i].front());
        coded_chunks[i].clear();
    }

    for (int i = 0; i < k + m; i++)
    {
        (*decoded)[i] = coded_chunks[i];
    }

    ////测试
    for (int i = 0; i < k + m; i++)
    {
        //printf("%s:%s\n","U_buf",c_str(U_buf[i], blocksize + 1));
    }

    for (int i = 0; i < k + m; i++)
    {
        if (i < k)
        {
            //printf("%s:%s\n","数据块",c_str((*decoded)[i], blocksize + 1));
        }
        else
        {
            //printf("%s:%s\n","校验块",c_str((*decoded)[i], blocksize + 1));
        }
    }

    ////free U_buf
    for (int i = 0; i < q*t; i++)
    {
        if(U_buf[i].size()!=0)
        {
            free(U_buf[i].front());
            U_buf[i].clear();
        }
    }


    return res;
}

////单节点修复直接调用的函数
int Clay::minimum_to_decode(const set<int> &want_to_read,const set<int> &available,map<int, vector<pair<int, int>>> *minimum)
{
    if (is_repair(want_to_read, available))  ////一个节点失效时返回1
    {
        return minimum_to_repair(want_to_read, available, minimum);
    }
    else    ////多节点失效时再生码效率不如普通纠删码，只取前k个可用块进行修复即可，每个块的所有子块参与修复
    {
        return minimum_to_decode1(want_to_read, available, minimum);////调用默认minimum_to_decode
    }
}
////对能否进行单节点修复进行判断，包括需要用到的节点是否可用，是否有>=d个节点可用
int Clay::is_repair(const set<int> &want_to_read,const set<int> &available_chunks)
{
    if (includes(available_chunks.begin(), available_chunks.end(), want_to_read.begin(), want_to_read.end())) return 0;////后面的是前面的子序列
    if (want_to_read.size() > 1) return 0;////多个节点失效排除

    int i = *want_to_read.begin();
    int lost_node_id = (i < k) ? i: i+nu;
    for (int x = 0; x < q; x++)
    {
        int node = (lost_node_id/q)*q+x;
        node = (node < k) ? node : node-nu;
        if (node != i)   // node in the same group other than erased node
        {
            if (available_chunks.count(node) == 0) return 0;////node应该是helper节点的索引
        }
    }

    if (available_chunks.size() < (unsigned)d) return 0;////helper节点至少d个
    return 1;
}
////确定修复节点及其参与修复的子块的索引
int Clay::minimum_to_repair(const set<int> &want_to_read,const set<int> &available_chunks,map<int, vector<pair<int, int>>> *minimum)
{
    int i = *want_to_read.begin();
    int lost_node_index = (i < k) ? i : i+nu;
    int rep_node_index = 0;

    // add all the nodes in lost node's y column.
    vector<pair<int, int>> sub_chunk_ind;////丢失节点的子块索引
    get_repair_subchunks(lost_node_index, sub_chunk_ind);
    if (available_chunks.size() >= (unsigned)d)
    {
        for (int j = 0; j < q; j++)
        {
            if (j != lost_node_index%q)  ////lost_node_index%q对应丢失节点的x坐标
            {
                rep_node_index = (lost_node_index/q)*q+j;////helper节点的索引
                if (rep_node_index < k)
                {
                    minimum->insert(make_pair(rep_node_index, sub_chunk_ind));////map可直接用make_pair
                }
                else if (rep_node_index >= k+nu)
                {
                    minimum->insert(make_pair(rep_node_index-nu, sub_chunk_ind));
                }
            }
        }
        for (auto chunk : available_chunks)
        {
            if (minimum->size() >= (unsigned)d)
            {
                break;////保证minimum里的helper节点至少d个
            }
            if (!minimum->count(chunk))
            {
                minimum->emplace(chunk, sub_chunk_ind);////插入新helper节点，保证d个
            }
        }
    }
    else    ////可用块小于d个，不可修复
    {
        std::cout << "minimum_to_repair: shouldn't have come here" << std::endl;
        return 1;
    }
    assert(minimum->size() == (unsigned)d);

    return 0;
}
////得到需要修复的子块：vector<off,len>
void Clay::get_repair_subchunks(const int &lost_node,vector<pair<int, int>> &repair_sub_chunks_ind)
{
    const int y_lost = lost_node / q;////丢失节点的y坐标
    const int x_lost = lost_node % q;////丢失节点的x坐标

    const int seq_sc_count = pow_int(q, t-1-y_lost);
    const int num_seq = pow_int(q, y_lost);

    int index = x_lost * seq_sc_count;
    for (int ind_seq = 0; ind_seq < num_seq; ind_seq++)
    {
        //printf("num_seq:%d\n",num_seq);
        repair_sub_chunks_ind.push_back(make_pair(index, seq_sc_count));////每次加入off为index，长度为seq_sc_count*sc_size的子块
        index += q * seq_sc_count;
    }
    helper_blocksize = seq_sc_count*subchunk_size;////helper节点保存的bufferlist有多个字符串，每个字符串大小为helper_blocksize<blocksize
    //cout<<"helper_blocksize: "<<helper_blocksize<<endl;
}
int Clay::_minimum_to_decode(const set<int> &want_to_read,const set<int> &available_chunks,set<int> *minimum)
{
    if (includes(available_chunks.begin(), available_chunks.end(),want_to_read.begin(), want_to_read.end()))
    {
        *minimum = want_to_read;////前者包括后者
    }
    else
    {
        unsigned int k = get_data_chunk_count();
        if (available_chunks.size() < (unsigned)k)////可用块<k，不可修复
            return -EIO;////可能有问题
        set<int>::iterator i;
        unsigned j;
        for (i = available_chunks.begin(), j = 0; j < (unsigned)k; ++i, j++)
            minimum->insert(*i);////可用块>=k,只取前k个块进行修复
    }

    return 0;
}
//////多节点失效时调用的minimum_to_decode
int Clay::minimum_to_decode1(const set<int> &want_to_read,const set<int> &available_chunks,map<int, vector<pair<int, int>>> *minimum)
{
    set<int> minimum_shard_ids;
    int r = _minimum_to_decode(want_to_read, available_chunks, &minimum_shard_ids);
    if (r != 0)
    {
        return r;
    }
    vector<pair<int, int>> default_subchunks;////vector<pair<off,len>>，每个helper节点参与修复的子块集合，从off开始，算len长度
    default_subchunks.push_back(make_pair(0, get_sub_chunk_count()));////默认每个helper节点所有子块参与修复
    for (auto &&id : minimum_shard_ids)
    {
        minimum->insert(make_pair(id, default_subchunks));////minimum生成=map<块序号minimum_shard_ids,块对应的所有子块>
    }
    return 0;
}
////单节点失效时根据再生码理论修复入口
int Clay::decode(const set<int> &want_to_read, set<int> &available, int chunk_size, char* fname)
{
        
map<int, bufferlist> degraded;
map<int, bufferlist> decoded;
        map<int, vector<pair<int,int>>> minimum;
        map<int, bufferlist> helper;
        std::ifstream fp3;////必须得是ifstream不能是fstream
        char fname3[256];
        //memset(fname3,0,strlen(curdir1)+10);
        ////解码文件
        char fname4[256];
        std::ofstream fp4;

       for (int i = 0; i < get_chunk_count(); i++)
        {
            
sprintf(fname3, "%s_%d", fname,i+1);        
            fp3.open(fname3);
            if(!fp3)
            {   
                continue;
            }
            char *buf = (char*)malloc(sizeof(char)*(1+blocksize));
            memset(buf,0,blocksize+1);
            fp3.read(buf,blocksize);
            //printf("%s\n",buf);
            degraded[i].push_back(buf);
            fp3.close();
        }

           minimum_to_decode(want_to_read, available, &minimum);
           for (map<int, vector<pair<int,int>>>::iterator h=minimum.begin(); h!= minimum.end(); ++h)
           {
                for(vector<pair<int,int>>::iterator ind=h->second.begin(); ind != h->second.end(); ++ind)
                {
                    bufferlist temp;
                    substr_of(&temp, degraded[h->first], ind->first*subchunk_size, ind->second*subchunk_size);
                    char *buf = c_str(temp,ind->second*subchunk_size);
                    helper[h->first].push_back(buf);////将minimum里子块集合按块索引存取到helper中，对于h->first编码块，需要temp子块集合参与修复,bufferlist有很多个char*
                    free(temp.front());
                }
            }
            EXPECT_EQ((unsigned)d, helper.size());

    set<int> avail;
    for (map<int, bufferlist>::const_iterator temp = helper.begin(); temp!=helper.end(); temp++)
    {
        avail.insert(temp->first);////将chunks里的块号加入avail
        //(void)bl;  // silence -Wunused-variable
    }

    if (is_repair(want_to_read, avail) && ((unsigned int)chunk_size > len(helper.begin()->second,1))) ////单节点失效chunk_size>每个helper节点的子块总长度
    {
        int i = repair(want_to_read, helper, &decoded, chunk_size);

        for (set<int>::iterator it = want_to_read.begin();it!=want_to_read.end();it++)
        {      
            sprintf(fname4, "%s_%d", fname, (*it)+1);
            fp4.open(fname4);
            if(!fp4)
                std::cerr << "create file failed. maybe wrong directory." << std::endl;
            fp4.write(decoded[*it].front(),blocksize);
            fp4.close();
        }

        //free(fname4);
        for(int i = 0; i<get_chunk_count(); i++)
        {
            if(decoded.find(i)==decoded.end())
                continue;
            else
            {
                free(decoded[i].front());
                decoded[i].clear();
            }
        }
            for(int i = 0; i<get_chunk_count(); i++)
            {
                if(degraded.find(i)==degraded.end())
                    continue;
                else
                {
                    free(degraded[i].front());
                    degraded[i].clear();
                }
            }
for(int i = 0; i<get_chunk_count(); i++)
            {
                if(helper.find(i)==helper.end())
                    continue;
                else
                {
                    for(bufferlist::iterator it = helper[i].begin(); it!=helper[i].end(); it++)
                    {
                        //printf("%d\n",i);
                        free(*it);
                    }
                    helper[i].clear();

                }
            }
            //free(curdir1);

        return i;
    }
    else
    {
        //return _decode(want_to_read, helper, decoded);////调用Clay::_decode,可能有问题????，因为decode_layered只能处理m个块丢失的情况
    }
}
////参与修复的子块数目
int Clay::get_repair_sub_chunk_count(const set<int> &want_to_read)
{
    int weight_vector[t];
    std::fill(weight_vector, weight_vector + t, 0);
    for (auto to_read : want_to_read)
    {
        weight_vector[to_read / q]++;
    }

    int repair_subchunks_count = 1;
    for (int y = 0; y < t; y++)
    {
        repair_subchunks_count = repair_subchunks_count*(q-weight_vector[y]);
    }

    return sub_chunk_no - repair_subchunks_count;
}

int Clay::repair(const set<int> &want_to_read,const map<int, bufferlist> &chunks,map<int, bufferlist> *repaired, int chunk_size)
{
    ////进行一系列验证
    assert((want_to_read.size() == 1) && (chunks.size() == (unsigned)d));

    int repair_sub_chunk_no = get_repair_sub_chunk_count(want_to_read);////参与修复的子块数目
    vector<pair<int, int>> repair_sub_chunks_ind;////待修复的子块：<off，len>

    unsigned repair_blocksize = len(chunks.begin()->second,1);////hepler节点所有参与修复的子块的总大小
cout<<repair_blocksize<<" "<<repair_sub_chunk_no<<endl;
    assert(repair_blocksize%repair_sub_chunk_no == 0);

    unsigned sub_chunksize = repair_blocksize/repair_sub_chunk_no;
    unsigned chunksize = sub_chunk_no*sub_chunksize;////子块*子块size

    assert(chunksize == (unsigned)chunk_size);////验证是否sub_chunksize正确

    map<int, bufferlist> recovered_data;
    map<int, bufferlist> helper_data;
    set<int> aloof_nodes;////既不参与修复也不是待修复节点，无关节点

    for (int i =  0; i < k + m; i++)
    {
        // included helper data only for d+nu nodes.
        ////将chunks数据放入helper_data，块索引加nu
        auto found = chunks.find(i);
        if (found != chunks.end())   // i is a helper
        {
            if (i<k)
            {
                helper_data[i] = found->second;
                //printf("%d\n",len(helper_data[i]));
            }
            else
            {
                helper_data[i+nu] = found->second;
                //printf("%d\n",len(helper_data[i+nu]));
            }
        }
        else
        {
            if (i != *want_to_read.begin())   // aloof node case.
            {
                int aloof_node_id = (i < k) ? i: i+nu;
                aloof_nodes.insert(aloof_node_id);
            }
            else    ////i是需修复的节点
            {
                char *ptr = (char*)malloc((chunksize+1)*sizeof(char));
                memset(ptr,0,chunksize+1);
                int lost_node_id = (i < k) ? i : i+nu;
                (*repaired)[i].push_back(ptr);////repaired只包含一个节点，即修复节点，而且bufferlist置为空
                recovered_data[lost_node_id] = (*repaired)[i];////recovered_data等价于repaired，bufferlist赋值
                get_repair_subchunks(lost_node_id, repair_sub_chunks_ind);
            }
        }
    }

    // this is for shortened codes i.e., when nu > 0
    for (int i=k; i < k+nu; i++)
    {
        char *ptr = (char*)malloc((repair_blocksize+1)*sizeof(char));
        memset(ptr,0,repair_blocksize+1);
        helper_data[i].push_back(ptr);////中间nu个节点设为helper节点
    }

    assert(helper_data.size()+aloof_nodes.size()+recovered_data.size() == (unsigned) q*t);////总节点数目为k+m+nu=q*t

    int r = repair_one_lost_chunk(recovered_data, aloof_nodes, helper_data, repair_blocksize, repair_sub_chunks_ind);////recovered_data修改了，repaired也会变

    // clear buffers created for the purpose of shortening
    for (int i = k; i < k+nu; i++)
    {
        free(helper_data[i].front());
        helper_data[i].clear();////将中间nu个节点删除
    }

    for(int i=0; i<k+m; i++)
    {
        //printf("%s:%s\n","U_buf",c_str(U_buf[i],blocksize));
    }

    return r;
}
////修复单个节点的数据，主要修复函数
int Clay::repair_one_lost_chunk(map<int, bufferlist> &recovered_data,set<int> &aloof_nodes,map<int, bufferlist> &helper_data,int repair_blocksize,vector<pair<int,int>> &repair_sub_chunks_ind)
{
    unsigned repair_subchunks = (unsigned)sub_chunk_no / q;////每个helper节点参与修复的子块数目
    unsigned sub_chunksize = repair_blocksize / repair_subchunks;////子块大小

    int z_vec[t];
    map<int, set<int>> ordered_planes;
    map<int, int> repair_plane_to_ind;
    int count_retrieved_sub_chunks = 0;////修复成功的子块数目
    int plane_ind = 0;

    char *buf = (char*)malloc((sub_chunksize+1)*sizeof(char));
    memset(buf,0,sub_chunksize+1);
    bufferlist temp_buf;
    temp_buf.push_back(buf);////temp_buf置0

    for (vector<pair<int,int>>::iterator temp = repair_sub_chunks_ind.begin(); temp!=repair_sub_chunks_ind.end(); temp++)
    {
        for (int j = temp->first; j < temp->first + temp->second; j++)
        {
            get_plane_vector(j, z_vec);
            int order = 0;
            // check across all erasures and aloof nodes
            for (map<int, bufferlist>::iterator it = recovered_data.begin(); it!=recovered_data.end(); it++)
            {
                if ((it->first) % q == z_vec[(it->first) / q]) order++;
                //(void)bl;  // silence -Wunused-variable
            }
            for (auto node : aloof_nodes)
            {
                if (node % q == z_vec[node / q]) order++;
            }
            assert(order > 0);
            ordered_planes[order].insert(j);
            // to keep track of a sub chunk within helper buffer recieved
            repair_plane_to_ind[j] = plane_ind;
            plane_ind++;
        }
    }
    assert((unsigned)plane_ind == repair_subchunks);

    int plane_count = 0;

    for (int i = 0; i < q*t; i++)
    {
        if (U_buf[i].size() != 0)////U不应该全部初始化？？？？
        {
            U_buf.erase(U_buf.begin());
        }
        char *buf = (char*)malloc((sub_chunk_no*sub_chunksize+1)*sizeof(char));
        memset(buf,0,sub_chunk_no*sub_chunksize+1);
        U_buf[i].push_back(std::move(buf));
    }

    ////测试
//    for (int i = 0; i < k + m; i++)
//    {
//        printf("%s:%s\n","U_buf",c_str(U_buf[i], blocksize + 1));
//    }

    int lost_chunk;
    int count = 0;
    for (map<int, bufferlist>::iterator it = recovered_data.begin(); it!=recovered_data.end(); it++)
    {
        lost_chunk = it->first;
        count++;
        ////把bufferlist强制转换为void???
        //(void)bl;  // silence -Wunused-variable
    }
    assert(count == 1);

    set<int> erasures;////看不懂erasures指代什么？？？
    for (int i = 0; i < q; i++)
    {
        erasures.insert(lost_chunk - lost_chunk % q + i);
    }
    for (auto node : aloof_nodes)
    {
        erasures.insert(node);
    }

    for (int order = 1; ; order++)
    {
        if (ordered_planes.count(order) == 0)
        {
            break;
        }
        plane_count += ordered_planes[order].size();
        for (auto z : ordered_planes[order])
        {
            get_plane_vector(z, z_vec);

            for (int y = 0; y < t; y++)
            {
                for (int x = 0; x < q; x++)  ////共遍历q*t次=k+m+nu
                {
                    int node_xy = y*q + x;
                    map<int, bufferlist> known_subchunks;
                    map<int, bufferlist> pftsubchunks;
                    set<int> pft_erasures;
                    if (erasures.count(node_xy) == 0)
                    {
                        assert(helper_data.count(node_xy) > 0);
                        int z_sw = z + (x - z_vec[y])*pow_int(q,t-1-y);
                        int node_sw = y*q + z_vec[y];
                        int i0 = 0, i1 = 1, i2 = 2, i3 = 3;
                        if (z_vec[y] > x)
                        {
                            i0 = 1;
                            i1 = 0;
                            i2 = 3;
                            i3 = 2;
                        }
                        ////计算没损坏节点的U_buf[node_xy],off= z*sub_chunksize, len=sub_chunksize，记得赋值U_buf
                        ////只计算了U_buf的两个块
                        if (aloof_nodes.count(node_sw) > 0)
                        {
                            assert(repair_plane_to_ind.count(z) > 0);
                            assert(repair_plane_to_ind.count(z_sw) > 0);
                            pft_erasures.insert(i2);////i2丢失
                            substr_of(&known_subchunks[i0],helper_data[node_xy], repair_plane_to_ind[z]*sub_chunksize, sub_chunksize,helper_blocksize,1);
                            substr_of(&known_subchunks[i3],U_buf[node_sw], z_sw*sub_chunksize, sub_chunksize);
                            pftsubchunks[i0] = known_subchunks[i0];
                            pftsubchunks[i1] = temp_buf;
                            substr_of(&pftsubchunks[i2],U_buf[node_xy], z*sub_chunksize, sub_chunksize);////恢复U_buf[node_xy]
                            pftsubchunks[i3] = known_subchunks[i3];
                            scalar_decode_chunks(atoi(pft.profile["k"].c_str()), atoi(pft.profile["m"].c_str()), atoi(pft.profile["w"].c_str()), pft_erasures, known_subchunks, &pftsubchunks, sub_chunksize); ////调用MDS的解码函数
//                            printf("1\n");
//                            std::cout<<"设置第"<<node_xy<<"个U_buf偏移为"<<z*sub_chunksize<<"长度为"<<sub_chunksize<<std::endl;
                            set_U(pftsubchunks[i2], &U_buf[node_xy], z*sub_chunksize, sub_chunksize, sub_chunk_no*sub_chunksize);////设置U
                            free(pftsubchunks[i2].front());
                            free(known_subchunks[i0].front());
                            free(known_subchunks[i3].front());
                        }
                        else
                        {
                            assert(helper_data.count(node_sw) > 0);
                            assert(repair_plane_to_ind.count(z) > 0);
                            if (z_vec[y] != x)
                            {
                                pft_erasures.insert(i2);
                                assert(repair_plane_to_ind.count(z_sw) > 0);
                                substr_of(&known_subchunks[i0],helper_data[node_xy], repair_plane_to_ind[z]*sub_chunksize, sub_chunksize,helper_blocksize,1);
                                substr_of(&known_subchunks[i1],helper_data[node_sw], repair_plane_to_ind[z_sw]*sub_chunksize, sub_chunksize,helper_blocksize,1);
                                pftsubchunks[i0] = known_subchunks[i0];
                                pftsubchunks[i1] = known_subchunks[i1];
                                substr_of(&pftsubchunks[i2],U_buf[node_xy], z*sub_chunksize, sub_chunksize);////恢复U_buf[node_xy]
                                substr_of(&pftsubchunks[i3],temp_buf, 0, sub_chunksize);////temp_buf
                                scalar_decode_chunks(atoi(pft.profile["k"].c_str()), atoi(pft.profile["m"].c_str()), atoi(pft.profile["w"].c_str()), pft_erasures, known_subchunks, &pftsubchunks, sub_chunksize); ////调用MDS的解码函数
//                                printf("2\n");
//                                std::cout<<"设置第"<<node_xy<<"个U_buf偏移为"<<z*sub_chunksize<<"长度为"<<sub_chunksize<<" "<<std::endl;
                                set_U(pftsubchunks[i2], &U_buf[node_xy], z*sub_chunksize, sub_chunksize, sub_chunk_no*sub_chunksize);////设置U
                                free(pftsubchunks[i3].front());
                                free(pftsubchunks[i2].front());
                                free(known_subchunks[i0].front());
                                free(known_subchunks[i1].front());
                            }
                            else
                            {
                                char* uncoupled_chunk = c_str(U_buf[node_xy],sub_chunk_no*sub_chunksize);
                                char* coupled_chunk = c_str1(helper_data[node_xy], repair_blocksize);////注意repair_blocksize是否正确？？？
                                memcpy(&uncoupled_chunk[z*sub_chunksize],&coupled_chunk[repair_plane_to_ind[z]*sub_chunksize],sub_chunksize);////后者赋给前者
//                                printf("3\n");
//                                printf("%s\n",coupled_chunk);
//                                printf("%d\n",helper_data[node_xy].size());
//                                for(auto it = helper_data[node_xy].begin();it!=helper_data[node_xy].end();it++){
//                                    printf("%s\n",*it);
//                                }
                                //std::cout<<"设置第"<<node_xy<<"个U_buf偏移为"<<z*sub_chunksize<<"长度为"<<sub_chunksize<<" "<<std::endl;
                                set_U1(uncoupled_chunk, &U_buf[node_xy], z*sub_chunksize, sub_chunksize);////设置U   C和U的偏移均为z*sc_size
                                free(uncoupled_chunk);
                                free(coupled_chunk);
                            }
                        }
                    }
                } // x
            } // y
            assert(erasures.size() <= (unsigned)m);
            decode_uncoupled(erasures, z, sub_chunksize);////根据上面的U_buf修复剩下的U_buf

            for (auto i : erasures)
            {
                int x = i % q;
                int y = i / q;
                int node_sw = y*q+z_vec[y];
                int z_sw = z + (x - z_vec[y]) * pow_int(q,t-1-y);
                set<int> pft_erasures;
                map<int, bufferlist> known_subchunks;
                map<int, bufferlist> pftsubchunks;
                int i0 = 0, i1 = 1, i2 = 2, i3 = 3;
                if (z_vec[y] > x)
                {
                    i0 = 1;
                    i1 = 0;
                    i2 = 3;
                    i3 = 2;
                }
                // make sure it is not an aloof node before you retrieve repaired_data
                ////计算C只用到了U中的两个块，即上面求出的两个块，不需要全部块，这应该就是解码效率提高的地方！！！
                if (aloof_nodes.count(i) == 0)
                {
                    if (x == z_vec[y])   // hole-dot pair (type 0)
                    {
                        char* uncoupled_chunk = c_str(U_buf[i], sub_chunk_no*sub_chunksize);
//                        printf("1\n");
//                        std::cout<<"设置第"<<i<<"个C_buf偏移为"<<z*sub_chunksize<<"长度为"<<sub_chunksize<<" "<<std::endl;
                        set_U1(uncoupled_chunk,&recovered_data[i], z * sub_chunksize, sub_chunksize);////对recovered_data赋值
                        count_retrieved_sub_chunks++;
                        free(uncoupled_chunk);
                    }
                    else
                    {
                        assert(y == lost_chunk / q);
                        assert(node_sw == lost_chunk);
                        assert(helper_data.count(i) > 0);
                        pft_erasures.insert(i1);
                        substr_of(&known_subchunks[i0],helper_data[i], repair_plane_to_ind[z]*sub_chunksize, sub_chunksize,helper_blocksize,1);
                        substr_of(&known_subchunks[i2],U_buf[i], z*sub_chunksize, sub_chunksize);
                        pftsubchunks[i0] = known_subchunks[i0];
                        substr_of(&pftsubchunks[i1], recovered_data[node_sw], z_sw*sub_chunksize, sub_chunksize);////待解码数据
                        pftsubchunks[i2] = known_subchunks[i2];
                        pftsubchunks[i3] = temp_buf;
                        scalar_decode_chunks(atoi(pft.profile["k"].c_str()), atoi(pft.profile["m"].c_str()), atoi(pft.profile["w"].c_str()), pft_erasures, known_subchunks, &pftsubchunks, sub_chunksize); ////调用MDS的解码函数
                        //std::cout<<"设置第"<<node_sw<<"个C_buf偏移为"<<z_sw*sub_chunksize<<"长度为"<<sub_chunksize<<" "<<std::endl;
                        set_U(pftsubchunks[i1], &recovered_data[node_sw], z_sw*sub_chunksize, sub_chunksize, sub_chunk_no*sub_chunksize);////设置C
                        free(known_subchunks[i0].front());
                        free(known_subchunks[i2].front());
                        free(pftsubchunks[i1].front());
                    }
                }
            } // recover all erasures
        } // planes of particular order
    } // order

    free(buf);
    ////free U_buf
    for (int i = 0; i < q*t; i++)
    {
        if(U_buf[i].size()!=0)
        {
            free(U_buf[i].front());
            U_buf[i].clear();
        }
    }

    return 0;
}

