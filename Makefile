include makefile.mk


LIB_TARGETS := phxqueue_phxrpc/test/libphxqueue_phxrpc_test.a \
			   phxqueue_phxrpc/consumer/libphxqueue_phxrpc_consumer.a \
			   phxqueue_phxrpc/producer/libphxqueue_phxrpc_producer.a \
			   phxqueue_phxrpc/scheduler/libphxqueue_phxrpc_scheduler.a \
			   phxqueue_phxrpc/plugin/libphxqueue_phxrpc_plugin.a \
			   phxqueue_phxrpc/comm/libphxqueue_phxrpc_comm.a \
			   phxqueue/test/libphxqueue_test.a \
			   phxqueue/consumer/libphxqueue_consumer.a \
			   phxqueue/store/libphxqueue_store.a \
			   phxqueue/lock/libphxqueue_lock.a \
			   phxqueue/scheduler/libphxqueue_scheduler.a \
			   phxqueue/producer/libphxqueue_producer.a \
			   phxqueue/config/libphxqueue_config.a \
			   phxqueue/comm/libphxqueue_comm.a \
			   phxqueue/plugin/libphxqueue_plugin.a

BIN_TARGETS := phxqueue/test/test_config_main phxqueue/test/test_consumer_main phxqueue/test/test_lock_main phxqueue/test/test_plugin_main \
			   phxqueue/test/test_producer_main phxqueue/test/test_scheduler_main phxqueue/test/test_store_main phxqueue/test/test_log_main phxqueue/test/test_notifierpool_main \
			   phxqueue/test/test_consistent_hash_main \
			   phxqueue_phxrpc/test/test_load_config_main phxqueue_phxrpc/test/test_rpc_config_main \
			   phxqueue_phxrpc/test/consumer_main phxqueue_phxrpc/test/producer_benchmark_main \
			   phxqueue_phxrpc/test/test_producer_echo_main phxqueue_phxrpc/test/config_check_main \
			   phxqueue_phxrpc/test/test_selector_main phxqueue_phxrpc/test/test_get_main

SUB_MAKE_LIB_TARGETS := phxqueue_phxrpc/app/store/libstore_client.a \
						phxqueue_phxrpc/app/lock/liblock_client.a \
						phxqueue_phxrpc/app/scheduler/libscheduler_client.a \
						phxqueue_phxrpc/app/mqttbroker/libmqttbroker_client.a \
						phxqueue_phxrpc/app/logic/mqtt/libphxqueue_phxrpc_logic_mqtt.a

SUB_MAKE_BIN_TARGETS := phxqueue_phxrpc/app/store/store_main phxqueue_phxrpc/app/store/store_tool_main \
						phxqueue_phxrpc/app/lock/lock_main phxqueue_phxrpc/app/lock/lock_tool_main \
						phxqueue_phxrpc/app/scheduler/scheduler_main phxqueue_phxrpc/app/scheduler/scheduler_tool_main \
						phxqueue_phxrpc/app/mqttconsumer/mqttconsumer_main \
						phxqueue_phxrpc/app/mqttbroker/mqttbroker_main phxqueue_phxrpc/app/mqttbroker/mqttbroker_tool_main


define LIB_GEN
	$(eval $(1)_SRCS := $(shell find $(dir $(1)) -regex '.*\.\(c\|cpp\)' | grep -aiv '_main.cpp'))
	$(eval $(1)_PROTOS := $(shell find $(dir $(1)) -regex '.*\.proto'))
	$(eval PROTO_SRCS += $(addsuffix .pb.cc,$(basename $($(1)_PROTOS))))
	$(eval PROTO_SRCS += $(addsuffix .pb.h,$(basename $($(1)_PROTOS))))
	$(eval $(1)_OBJS := $(addsuffix .pb.o,$(basename $($(1)_PROTOS))))
	$(eval $(1)_OBJS += $(addsuffix .o,$(basename $($(1)_SRCS))))
	$(eval LIB_OBJS += $($(1)_OBJS))
endef

define BIN_GEN
	$(eval BIN_OBJS += $(addsuffix .o,$(1)))
endef

IGNORE := $(foreach lib,$(LIB_TARGETS),$(call LIB_GEN,$(lib)))
IGNORE := $(foreach bin,$(BIN_TARGETS),$(call BIN_GEN,$(bin)))


all: pb $(LIB_TARGETS) $(SUB_MAKE_LIB_TARGETS) install_lib $(BIN_TARGETS) $(SUB_MAKE_BIN_TARGETS)  install_bin build_dir

pb: $(PROTO_SRCS)
	$(foreach sub_make,$(SUB_MAKE_LIB_TARGETS),make -C $(dir $(sub_make)) pb;)
	$(foreach sub_make,$(SUB_MAKE_BIN_TARGETS),make -C $(dir $(sub_make)) pb;)

$(LIB_TARGETS): $$($$@_OBJS)
	$(AR) $@ $^

$(SUB_MAKE_LIB_TARGETS):
	make -C $(dir $@) $(notdir $@)

$(SUB_MAKE_BIN_TARGETS):
	make -C $(dir $@) $(notdir $@)

install_lib:
	mkdir -p lib
	cp $(LIB_TARGETS) lib/
	cp $(SUB_MAKE_LIB_TARGETS) lib/
	$(AR) lib/libphxqueue.a $(LIB_OBJS)

install_bin:
	mkdir -p bin
	cp $(BIN_TARGETS) bin/
	cp $(SUB_MAKE_BIN_TARGETS) bin/

build_dir:
	mkdir -p data/lock.0
	mkdir -p data/lock.1
	mkdir -p data/lock.2
	mkdir -p data/store.0
	mkdir -p data/store.1
	mkdir -p data/store.2
	mkdir -p log/lock.0
	mkdir -p log/lock.1
	mkdir -p log/lock.2
	mkdir -p log/store.0
	mkdir -p log/store.1
	mkdir -p log/store.2
	mkdir -p log/consumer.0
	mkdir -p log/consumer.1
	mkdir -p log/consumer.2
	mkdir -p log/scheduler.0
	mkdir -p log/scheduler.1
	mkdir -p log/scheduler.2
	mkdir -p log/mqttbroker.0


clean:
	rm -rf $(PROTO_SRCS) $(LIB_OBJS) $(BIN_OBJS) $(LIB_TARGETS) $(SUB_MAKE_LIB_TARGETS) $(BIN_TARGETS) $(SUB_MAKE_BIN_TARGETS)
	$(foreach sub_make,$(SUB_MAKE_LIB_TARGETS),make -C $(dir $(sub_make)) clean;)
	$(foreach sub_make,$(SUB_MAKE_BIN_TARGETS),make -C $(dir $(sub_make)) clean;)

.PHONY: all install_lib install_bin clean $(SUB_MAKE_LIB_TARGETS) $(SUB_MAKE_BIN_TARGETS)



%.pb.o: %.pb.cc
	$(CXX) $(CXXFLAGS) $(INC_COMM) -I$(PHXQUEUE_BASE_DIR)/$(dir $^) -c $< -o $@

%.o: %.c
	$(CXX) $(CXXFLAGS) $(INC_COMM) -I$(PHXQUEUE_BASE_DIR)/$(dir $^) -c $< -o $@

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INC_COMM) -I$(PHXQUEUE_BASE_DIR)/$(dir $^) -c $< -o $@

%.pb.h: %.pb.cc

%.pb.cc: %.proto
	$(PROTOBUF_BIN_DIR)/protoc $(PBFLAGS) $^

$(BIN_TARGETS): $$@.o $(LIB_TARGETS) $(SUB_MAKE_LIB_TARGETS)
	$(LINKER) $(CXXFLAGS) $^ $(LDFLAGS) -o $@

