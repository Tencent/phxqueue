all: test_publish_memory

test_publish_memory: test_publish_memory.cpp
	g++ -Wall -std=c++11 $^ -o $@

clean:
	@($(RM) $(TARGETS))
	@($(RM) *.o)

