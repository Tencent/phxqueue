#include <iostream>
#include "phxqueue/test/test_config.h"

#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"


using namespace phxqueue;
using namespace std;


int main(int argc, char **argv) {
    comm::LogFunc log_func;
    plugin::LoggerGoogle::GetLogger("test_config", "/tmp/phxqueue/log", 3, log_func);
    comm::Logger::GetInstance()->SetLogFunc(log_func);

    config::GlobalConfig global_config;
    assert(comm::RetCode::RET_OK == global_config.Load());
    test::TestConfig::TestGlobalConfig(&global_config);

    return 0;
}


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

