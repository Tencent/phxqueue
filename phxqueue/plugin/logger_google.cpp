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
    google::InitGoogleLogging(module_name.c_str());
    FLAGS_log_dir = log_path;
    FLAGS_stderrthreshold = google::FATAL;
    FLAGS_minloglevel = google_log_level;
    log_func = LoggerGoogle::Log;

    return 0;
}


void LoggerGoogle::Log(const int log_level, const char *format, va_list args) {
    char buf[1024]{0};
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


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

