cmake_minimum_required(VERSION 3.10)

project(ngxfs LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CAMKE_CXX_FLAGS} -std=c++11 -pthread -lm -g")
set(CMAKE_EXE_LINKER_FLAGS "-pthread -lm")
set(CMAKE_C_FLAGS "-std=c++11 -Wall -Wpointer-arith -fPIC -g")

if(USE_DEBUG)
    SET(CMAKE_BUILD_TYPE "Debug")
    SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -O0 -Wall -g2 -ggdb")
endif()
if(USE_Release)
    SET(CMAKE_BUILD_TYPE "Debug")
    SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS} -O0 -Wall")
endif()

FIND_PACKAGE(Threads REQUIRED)
SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

FIND_LIBRARY(libleveldb ldb lib/libleveldb.a )
FIND_LIBRARY(libphxpaxos lphx lib/libphxpaxos.a )
FIND_LIBRARY(libprotobuf lproto lib/libprotobuf.a )
#FIND_LIBRARY(libprotobuf lphx_plugin lib/libphxpaxos_plugin.a )

link_directories(lib ${libleveldb} ${libphxpaxos} ${libprotobuf})

#
#link_libraries(libleveldb.a)
#link_libraries(libprotobuf.a)
#link_libraries(libphxpaxos.a)

#include(FindProtobuf)
#find_package(Protobuf REQUIRED
#        HINT
#        "lib")
#include_directories(${PROTOBUF_INCLUDE_DIR})
#message(${PROTOBUF_INCLUDE_DIR})

#include_directories(${Protobuf_INCLUDE_DIRS})
#include_directories(${CMAKE_CURRENT_BINARY_DIR})

include_directories(include)


include_directories(src/paxos)
include_directories(src/core)
include_directories(src/client)
include_directories(src/cfs)
include_directories(src/common)
include_directories(src/faio)
include_directories(src/datanode)
include_directories(default)
include_directories(src/namenode)
include_directories(src/election)


#查找当前目录下的所有源文件将名称保存到 DIR_SRCS 变量
AUX_SOURCE_DIRECTORY(src/paxos PAXOS)
AUX_SOURCE_DIRECTORY(src/cfs DIR_SRCS)
AUX_SOURCE_DIRECTORY(src/common DIR_SRCS)
AUX_SOURCE_DIRECTORY(src/core DIR_SRCS)
AUX_SOURCE_DIRECTORY(src/faio DIR_SRCS)
AUX_SOURCE_DIRECTORY(src/election ELECTION)

AUX_SOURCE_DIRECTORY(src/namenode NAMENODE)
AUX_SOURCE_DIRECTORY(src/datanode DATANODE)
AUX_SOURCE_DIRECTORY(src/client CLIENT)



# debug here
#SET(CMAKE_BUILD_TYPE "Debug")
#SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb")
#SET(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall")

## cmake 将所有c文件视为c++文件
#file(GLOB_RECURSE CFILES "${CMAKE_SOURCE_DIR}/*.c")
#SET_SOURCE_FILES_PROPERTIES(${CFILES} PROPERTIES LANGUAGE CXX )

add_executable(namenode ${DIR_SRCS} ${PAXOS} ${NAMENODE})
TARGET_LINK_LIBRARIES(namenode ${CMAKE_THREAD_LIBS_INIT} ${Protobuf_LIBRARIES})
TARGET_LINK_LIBRARIES(namenode m libphxpaxos.a libprotobuf.a libleveldb.a -latomic)


add_executable(datanode ${DIR_SRCS} ${ELECTION} ${DATANODE})
TARGET_LINK_LIBRARIES(datanode ${CMAKE_THREAD_LIBS_INIT} ${Protobuf_LIBRARIES})
TARGET_LINK_LIBRARIES(datanode m libphxpaxos.a libleveldb.a libprotobuf.a -latomic)

add_executable(client ${DIR_SRCS} ${CLIENT} )
TARGET_LINK_LIBRARIES(client ${CMAKE_THREAD_LIBS_INIT} ${Protobuf_LIBRARIES})
TARGET_LINK_LIBRARIES(client m libprotobuf.a libphxpaxos.a libleveldb.a)

# set install
SET(CMAKE_INSTALL_PREFIX ./install)

install(TARGETS namenode datanode client
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        PUBLIC_HEADER DESTINATION include
        )