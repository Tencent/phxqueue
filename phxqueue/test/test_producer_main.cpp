#include <iostream>

#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"

#include "phxqueue/test/simpleproducer.h"


using namespace phxqueue;
using namespace std;


int main(int argc, char ** argv) {
    comm::LogFunc log_func;
    plugin::LoggerGoogle::GetLogger("test_producer", "/tmp/phxqueue/log", 3, log_func);

    producer::ProducerOption opt;
    opt.log_func = log_func;

    test::SimpleProducer producer(opt);
    producer.Init();
    producer.Enqueue(1000, 123, 1, "buffer", 1);

    return 0;
}


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

