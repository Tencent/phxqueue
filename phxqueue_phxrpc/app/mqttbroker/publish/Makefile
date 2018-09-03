include ../../../../makefile.mk

all: test_publish_memory

test_publish_memory: test_publish_memory.o
	$(CXX) $^ $(LDFLAGS) -o $@

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -o $@ -c $< -I$(GLOG_INCLUDE_DIR) -I$(PROTOBUF_INCLUDE_DIR) \
			-I$(LEVELDB_INCLUDE_DIR) -I$(LIBCO_INCLUDE_DIR) -I$(PHXRPC_INCLUDE_DIR) \
			-I$(PHXPAXOS_INCLUDE_DIR) -I$(PHXPAXOS_PLUGIN_INCLUDE_DIR) -I$(PHXQUEUE_INCLUDE_DIR)

clean:
	@($(RM) $(TARGETS))
	@($(RM) *.o)

