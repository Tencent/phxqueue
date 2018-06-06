.SECONDEXPANSION:
where-am-i = $(abspath $(word $(words $(MAKEFILE_LIST)), $(MAKEFILE_LIST)))

PHXQUEUE_BASE_DIR := $(dir $(call where-am-i))

PREFIX := $(PHXQUEUE_BASE_DIR)

PROTOBUF_BIN_DIR := $(PHXQUEUE_BASE_DIR)/third_party/protobuf/bin

GFLAGS_INCLUDE_DIR := $(PHXQUEUE_BASE_DIR)/third_party/gflags/include
GLOG_INCLUDE_DIR := $(PHXQUEUE_BASE_DIR)/third_party/glog/include
PROTOBUF_INCLUDE_DIR := $(PHXQUEUE_BASE_DIR)/third_party/protobuf/include
LEVELDB_INCLUDE_DIR := $(PHXQUEUE_BASE_DIR)/third_party/leveldb/include
LIBCO_INCLUDE_DIR := $(PHXQUEUE_BASE_DIR)/third_party/colib
PHXRPC_INCLUDE_DIR := $(PHXQUEUE_BASE_DIR)/third_party/phxrpc
PHXPAXOS_INCLUDE_DIR := $(PHXQUEUE_BASE_DIR)/third_party/phxpaxos/include
PHXPAXOS_PLUGIN_INCLUDE_DIR := $(PHXQUEUE_BASE_DIR)/third_party/phxpaxos/plugin/include
PHXQUEUE_INCLUDE_DIR := $(PHXQUEUE_BASE_DIR)

GFLAGS_LIB_DIR := $(PHXQUEUE_BASE_DIR)/third_party/gflags/lib
GLOG_LIB_DIR := $(PHXQUEUE_BASE_DIR)/third_party/glog/lib
PROTOBUF_LIB_DIR := $(PHXQUEUE_BASE_DIR)/third_party/protobuf/lib
LEVELDB_LIB_DIR := $(PHXQUEUE_BASE_DIR)/third_party/leveldb/lib
LIBCO_LIB_DIR := $(PHXQUEUE_BASE_DIR)/third_party/colib/lib
PHXRPC_LIB_DIR := $(PHXQUEUE_BASE_DIR)/third_party/phxrpc/lib
PHXPAXOS_LIB_DIR := $(PHXQUEUE_BASE_DIR)/third_party/phxpaxos/lib
PHXPAXOS_PLUGIN_LIB_DIR := $(PHXQUEUE_BASE_DIR)/third_party/phxpaxos/plugin/lib
PHXQUEUE_LIB_DIR := $(PHXQUEUE_BASE_DIR)/lib

#BOOST_ROOT_DIR := $(PHXQUEUE_BASE_DIR)/third_party/boost

CXX := g++
AR := ar cruv
LINKER := $(CXX)
MAKE := make

ifeq ($(debug), y)
# (1) Debug
	OPT := -g2
else
# (2) Production
	OPT := -O2
endif

CXXFLAGS := -std=c++11 -Wall -D_REENTRANT -D_GNU_SOURCE -D_XOPEN_SOURCE -fPIC -m64 -I$(PROTOBUF_INCLUDE_DIR) $(OPT)
LDFLAGS := -L$(PHXRPC_LIB_DIR) -lphxrpc \
           -L$(PHXPAXOS_LIB_DIR) -lphxpaxos \
		   -L$(LEVELDB_LIB_DIR) -lleveldb \
		   -L$(LIBCO_LIB_DIR) -lcolib \
		   $(PROTOBUF_LIB_DIR)/libprotobuf.a \
		   $(GLOG_LIB_DIR)/libglog.a \
		   $(GFLAGS_LIB_DIR)/libgflags.a \
           -Wl,-Bdynamic \
		   -lrt -lz -ldl -lpthread
#PBFLAGS = --proto_path=$(PROTOBUF_INCLUDE_DIR) --proto_path=$(PHXQUEUE_INCLUDE_DIR) --cpp_out=.
PBFLAGS := --proto_path=$(PROTOBUF_INCLUDE_DIR) --proto_path=. --cpp_out=.
INC_COMM := -I$(PHXQUEUE_INCLUDE_DIR) -I$(GFLAGS_INCLUDE_DIR) -I$(GLOG_INCLUDE_DIR) -I$(PROTOBUF_INCLUDE_DIR) -I$(LEVELDB_INCLUDE_DIR) -I$(LIBCO_INCLUDE_DIR) -I$(PHXPAXOS_INCLUDE_DIR) \
		-I$(PHXPAXOS_INCLUDE_DIR) -I$(PHXRPC_INCLUDE_DIR) -I$(PHXPAXOS_PLUGIN_INCLUDE_DIR)

