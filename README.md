how to run:
1. you should compile phxpaxos(github), then replace the lib/* and include/* to what you have compiled.
2. modify /etc/config.h and /etc/*.conf , then run prepareDir.sh
3. cmake . and make 
4. run namenode -h , you should see how to format dfs system
5. run datanode
6. run client -h to see how to use cmds

//
datanode dn_ns_service 172  324//

## important
如果使用系统提前分配的task，那么在write back之前必须将task->data 指向 nullptr

因为直接检测网卡ip地址，所以配置文件一些关于ip的设置会失效

项目编译流程。
开发工具clion， 推荐使用clion

# 1. phxpaxos 编译流程
1. 解压提供的phxpaxos.zip

## 编译leveldb
1. 进入third_party/leveldb目录。
2. 执行 
`mkdir -p build && cd build`
`cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .`

3. 在leveldb目录 mkdir lib建立一个lib目录，然后将build里的libleveldb.a复制到lib目录里，PhxPaxos通过lib这个目录来寻址静态库。

## 编译protobuf
需要安装以下软件：
ubuntu:
`sudo apt-get install autoconf automake libtool curl make g++ unzip`

1. 进入`third_party/protobuf`目录。
2. `./autogen.sh`
3. `./configure CXXFLAGS=-fPIC --prefix=[当前目录绝对路径]`, 这一步CXXFLAGS和--prefix都必须设置对。
4. `make && make install`
编译完成后检查是否在当前目录成功生成bin,include,lib三个子目录。

## 编译PhxPaxos静态库
1. 进入PhxPaxos根目录。
2. `./autoinstall.sh`
3. `make && make install `(如需编译debug版本，则命令为make debug=y)
4. 编译完成后检查是否在当前目录成功生成lib子目录，并检查在lib目录是否成功生成静态库libphxpaxos.a.

# 2.替换库文件
将`leveldb/lib/libleveldb.a`,  `protobuf/lib/protobuf.a`, `PhxPaxos根目录/lib/libprotobuf.a` 复制到项目目录 /lib 文件夹里替换原文件。
将 `phxpaxos/include/phxpaxos` , `phxpaxos/third_party/leveldb/include/leveldb` , `phxpaxos/third_party/protobuf/include` 文件夹中的头文件复制到项目 /include目录，注意保持目录结构一致。

# 3.编译 proto文件 
将项目目录`/proto_data/phxeditlog.proto` 拷贝到  `phxpaxos/third_party/protobuf/bin` 目录下，执行:
		`./protoc -I=./ --cpp_out=./ ./phxeditlog.proto`
		`mv phxeditlog.pb.cc phxeditlog.pb.cpp`
将生成的phxeditlog.pb.h、 phxeditlog.pb.cpp 拷贝到项目目录 `/src/paxos` 目录下替换原文件。

# 4.编译
`cmake . && make`
编译完成后在 CMakeFiles 可以找到执行文件。

# 5.运行项目
在 项目目录里运行 `./prepareDir.sh` 生成项目运行所需目录
运行编译好的 `./namenode -h` 查看帮助说明，以及如何 `format system`
运行 namenode : `./namenode`
运行 datanode: `./datanode`
运行cli: `./client -h`

### 注意：配置文件默认路径，/home/用户/opendfs/etc/*




