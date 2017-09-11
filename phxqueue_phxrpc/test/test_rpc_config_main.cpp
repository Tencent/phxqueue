#include <iostream>

#include "phxqueue_phxrpc/test/test_rpc_config.h"

#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"


using namespace std;


int main(int argc, char **argv) {
    phxqueue::comm::LogFunc log_func;
    phxqueue::plugin::LoggerGoogle::GetLogger("test_main", "/tmp/phxqueue/log", 3, log_func);
    phxqueue::comm::Logger::GetInstance()->SetLogFunc(log_func);

    phxqueue_phxrpc::test::TestConfig::Process();

    return 0;
}


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

