#!/bin/sh
filename=GroupListMess
./protoc -I=./ --cpp_out=./ ./$filename.proto
mv $filename.pb.cc $filename.pb.cpp
mv $filename.pb.h $filename.pb.cpp ../src/common/

filename=phxeditlog
./protoc -I=./ --cpp_out=./ ./$filename.proto
mv $filename.pb.cc $filename.pb.cpp
mv $filename.pb.h $filename.pb.cpp ../src/paxos/
