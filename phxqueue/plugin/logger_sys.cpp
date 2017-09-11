/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



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

