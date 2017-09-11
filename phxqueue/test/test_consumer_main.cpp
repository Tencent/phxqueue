#include <iostream>

#include "phxqueue/test/simpleconsumer.h"
#include "phxqueue/test/simplehandler.h"

#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"


using namespace phxqueue;
using namespace std;


int main(int argc, char **argv) {
    comm::LogFunc log_func;
    plugin::LoggerGoogle::GetLogger("test_consumer", "/tmp/phxqueue/log", 3, log_func);

    consumer::ConsumerOption opt;
    opt.topic = "test";
    opt.ip = "127.0.0.1";
    opt.port = 8001;
    opt.nprocs = 3;
    opt.proc_pid_path = "/tmp/phxqueue/";
    opt.lock_path_base = "./phxqueueconsumer.lock.";
    opt.log_func = log_func;

    test::SimpleConsumer consumer(opt);
    consumer.AddHandlerFactory(1, new comm::DefaultHandlerFactory<test::SimpleHandler>());
    consumer.Run();

    return 0;
}


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

