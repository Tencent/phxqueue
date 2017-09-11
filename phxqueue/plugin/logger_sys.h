#pragma once

#include <cstdio>
#include <string>

#include "phxqueue/comm.h"


namespace phxqueue {

namespace plugin {


class LoggerSys {
  public:
    static int GetLogger(const std::string &module_name, const int sys_log_level, const bool daemonize, comm::LogFunc &pLogFunc);

    static void Log(const int log_level, const char *format, va_list args);
};


}  // namespace plugin

}  // namespace phxqueue

