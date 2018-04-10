/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/plugin/logger_google.h"

#include <cassert>
#include <cstdarg>

#include <glog/logging.h>


namespace {


inline int LogLevel2GoogleLogLevel(const phxqueue::comm::LogLevel log_level) {
    switch (log_level) {
        case phxqueue::comm::LogLevel::Error:
            return google::ERROR;
        case phxqueue::comm::LogLevel::Warning:
            return google::WARNING;
        case phxqueue::comm::LogLevel::Info:
            return google::INFO;
        case phxqueue::comm::LogLevel::Verbose:
        default:
            ;
    }

    return -1;
}


}


namespace phxqueue {

namespace plugin {


using namespace std;


int LoggerGoogle::GetLogger(const string &module_name, const string &log_path,
                            const int google_log_level, comm::LogFunc &log_func) {
    static const string log_name = module_name;
    google::InitGoogleLogging(log_name.c_str());
    FLAGS_log_dir = log_path;
    FLAGS_stderrthreshold = google::FATAL;
    FLAGS_minloglevel = google_log_level;
    log_func = LoggerGoogle::Log;

    return 0;
}


void LoggerGoogle::Log(const int log_level, const char *format, va_list args) {
    char buf[1024] = {0};
    vsnprintf(buf, sizeof(buf), format, args);

    int google_log_level{LogLevel2GoogleLogLevel(static_cast<comm::LogLevel>(log_level))};
    if (google_log_level < 0) return;

    switch (google_log_level) {
        case google::INFO:
            LOG(INFO) << buf;
            break;
        case google::WARNING:
            LOG(WARNING) << buf;
            break;
        case google::ERROR:
            LOG(ERROR) << buf;
            break;
        case google::FATAL:
            LOG(FATAL) << buf;
            break;
        default:
            break;
    }
}


}  // namespace plugin

}  // namespace phxqueue

