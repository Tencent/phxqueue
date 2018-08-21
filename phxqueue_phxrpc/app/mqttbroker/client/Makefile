include ../../../../makefile.mk

LDFLAGS := -L$(PHXRPC_ROOT)/lib -lphxrpc $(LDFLAGS)

LIB_MQTT_OBJS = ../mqtt/mqtt_msg.o ../mqtt/mqtt_msg_handler.o ../mqtt/mqtt_protocol.o \
		../mqtt/mqtt_msg_handler_factory.o

CLI_OBJS = mqttbroker.pb.o \
		mqttbroker_client.o \
		mqttbroker_client_uthread.o \
		phxrpc_mqttbroker_stub.o

all: libmqttbroker_client.a mqttbroker_tool_main

libmqttbroker_client.a: $(CLI_OBJS)
	$(AR) $@ $^

mqttbroker_tool_main: phxrpc_mqttbroker_tool.o mqttbroker_tool_impl.o mqttbroker_tool_main.o $(LIB_MQTT_OBJS)
	$(LINKER) $^ -L$(PHXQUEUE_LIB_DIR) -L. -lphxqueue_phxrpc_logic_mqtt -lmqttbroker_client -lphxqueue_comm $(LDFLAGS) -o $@

%.pb.o: %.pb.cc
	$(CXX) $(CXXFLAGS) -o $@ -c $< -I$(PROTOBUF_INCLUDE_DIR) -I$(PHXRPC_INCLUDE_DIR) \
			-I$(PHXQUEUE_INCLUDE_DIR) -I.

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -o $@ -c $< -I$(GLOG_INCLUDE_DIR) -I$(PROTOBUF_INCLUDE_DIR) \
			-I$(LEVELDB_INCLUDE_DIR) -I$(LIBCO_INCLUDE_DIR) -I$(PHXRPC_INCLUDE_DIR) \
			-I$(PHXPAXOS_INCLUDE_DIR) -I$(PHXPAXOS_PLUGIN_INCLUDE_DIR) -I$(PHXQUEUE_INCLUDE_DIR)

########## message ##########

pb: mqttbroker.pb.cc mqttbroker.pb.h

mqttbroker.pb.cc: mqttbroker.pb.h

mqttbroker.pb.h: mqttbroker.proto
	$(PROTOBUF_BIN_DIR)/protoc $^ --proto_path=$(PHXRPC_INCLUDE_DIR) --proto_path=$(PHXQUEUE_INCLUDE_DIR) $(PBFLAGS)

########## client ##########

phxrpc_mqttbroker_stub.cpp: phxrpc_mqttbroker_stub.h
phxrpc_mqttbroker_stub.o: phxrpc_mqttbroker_stub.h
mqttbroker_client.cpp: phxrpc_mqttbroker_stub.h
mqttbroker_client.o: phxrpc_mqttbroker_stub.h
mqttbroker_client_uthread.cpp: phxrpc_mqttbroker_stub.h
mqttbroker_client_uthread.o: phxrpc_mqttbroker_stub.h

########## tool ##########

phxrpc_mqttbroker_tool.cpp: phxrpc_mqttbroker_tool.h
phxrpc_mqttbroker_tool.o: phxrpc_mqttbroker_tool.h
mqttbroker_tool_impl.cpp: phxrpc_mqttbroker_tool.h
mqttbroker_tool_impl.o: phxrpc_mqttbroker_tool.h
mqttbroker_tool_main.cpp: phxrpc_mqttbroker_tool.h
mqttbroker_tool_main.o: phxrpc_mqttbroker_tool.h

clean:
	@($(RM) $(TARGETS))
	@($(RM) *.o)

