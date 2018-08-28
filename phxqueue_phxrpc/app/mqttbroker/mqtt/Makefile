include ../../../../makefile.mk

TEST_TARGETS = test_mqtt_packet_id #test_mqtt_msg_handler

all: $(TEST_TARGETS)

#test_mqtt_msg_handler: test_mqtt_msg_handler.o mqtt_msg.o
#	$(LINKER) $^-L$(PHXQUEUE_LIB_DIR) -lphxqueue_phxrpc_logic_mqtt $(LDFLAGS) -o $@

test_mqtt_packet_id: test_mqtt_packet_id.o mqtt_packet_id.o
	$(LINKER) $^ $(LDFLAGS) -o $@

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -o $@ -c $< -I$(GLOG_INCLUDE_DIR) -I$(PROTOBUF_INCLUDE_DIR) \
			-I$(LEVELDB_INCLUDE_DIR) -I$(LIBCO_INCLUDE_DIR) -I$(PHXRPC_INCLUDE_DIR) \
			-I$(PHXPAXOS_INCLUDE_DIR) -I$(PHXPAXOS_PLUGIN_INCLUDE_DIR) -I$(PHXQUEUE_INCLUDE_DIR)

clean:
	@( $(RM) $(TEST_TARGETS) )
	@( $(RM) *.o core.* $(LIB_OBJS) )

