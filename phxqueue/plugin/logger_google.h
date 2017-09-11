#pragma once

#include <cstdio>
#include <string>

#include "phxqueue/comm.h"


namespace phxqueue {

namespace plugin {


class LoggerGoogle {
  public:
    static int GetLogger(const std::string &module_name, const std::string &log_path,
                         const int google_log_level, comm::LogFunc &log_func);

    static void Log(const int log_level, const char *format, va_list args);
};


}  // namespace plugin

}  // namespace phxqueue

