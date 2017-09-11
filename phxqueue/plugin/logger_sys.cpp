#include "phxqueue/plugin/logger_sys.h"

#include "phxqueue/comm.h"


#include <cassert>
#include <cstdarg>

#include <glog/logging.h>
#include <syslog.h>


namespace {

inline int LogLevel2SysLogLevel(const phxqueue::comm::LogLevel log_level) {
    switch (log_level) {
    case phxqueue::comm::LogLevel::Error:
        return LOG_ERR;
    case phxqueue::comm::LogLevel::Warning:
        return LOG_WARNING;
    case phxqueue::comm::LogLevel::Info:
        return LOG_INFO;
    case phxqueue::comm::LogLevel::Verbose:
        return LOG_DEBUG;
    default:
        ;
    };
    return LOG_ERR;
}

}

namespace phxqueue {

namespace plugin {


using namespace std;

static int g_sys_log_level_threshold = 0;

int LoggerSys::GetLogger(const std::string &module_name,
                         const int sys_log_level, const bool daemonize, comm::LogFunc &log_func) {

    openlog(module_name.c_str(), (daemonize ? LOG_PERROR : 0) | LOG_CONS | LOG_PID, 0);
    g_sys_log_level_threshold = sys_log_level;
    log_func = LoggerSys::Log;

    return 0;
}

void LoggerSys::Log(const int log_level, const char *format, va_list args) {
    int sys_log_level = LogLevel2SysLogLevel(static_cast<comm::LogLevel>(log_level));
    if (sys_log_level > g_sys_log_level_threshold) return;

    char buf[1024] = {0};
    vsnprintf(buf, sizeof(buf), format, args);
    //vsyslog(sys_log_level, format, args);
}


}  // namespace plugin

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

