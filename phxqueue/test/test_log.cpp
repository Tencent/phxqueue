#include "phxqueue/test/test_log.h"

#include <iostream>
#include <memory>
#include <unistd.h> 

#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"


namespace phxqueue {

namespace test {


using namespace std;


void TestLog::Process() {
    comm::LogFunc log_func;
    plugin::LoggerGoogle::GetLogger("test_log", "./log/test", 2, log_func); // level: warn
    comm::Logger::GetInstance()->SetLogFunc(log_func);

    NLErr("error");
    NLWarn("warn");
    NLInfo("info");
}

}  // namespace test

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

