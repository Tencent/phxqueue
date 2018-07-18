include ../../phxrpc.mk

TEST_TARGETS = test_mqtt_msg_handler

all: $(TEST_TARGETS)

test_mqtt_msg_handler: test_mqtt_msg_handler.o
	$(LINKER) $^ -L$(PHXRPC_ROOT)/lib -lphxrpc $(LDFLAGS) -o $@

clean:
	@( $(RM) $(TEST_TARGETS) )
	@( $(RM) *.o core.* $(LIB_OBJS) )

