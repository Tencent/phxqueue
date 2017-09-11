#pragma once

#include <cstdio>
#include <cinttypes>
#include <cassert>
#include <memory>

#include "phxqueue/comm/errdef.h"



namespace phxqueue {

namespace comm {


#define NLErr(format, args...) phxqueue::comm::Logger::GetInstance()->LogError("ERR: %s:%d " format, __func__, __LINE__, ## args);
#define NLInfo(format, args...) phxqueue::comm::Logger::GetInstance()->LogInfo("INFO: %s:%d " format, __func__, __LINE__, ## args);
#define NLWarn(format, args...) phxqueue::comm::Logger::GetInstance()->LogWarning("WARN: %s:%d " format, __func__, __LINE__, ## args);
#define NLVerb(format, args...) phxqueue::comm::Logger::GetInstance()->LogVerbose("DEBUG: %s:%d " format, __func__, __LINE__, ## args);

#define QLErr(format, args...) phxqueue::comm::Logger::GetInstance()->LogError("ERR: %s::%s:%d " format, typeid(this).name(), __func__, __LINE__, ## args);
#define QLInfo(format, args...) phxqueue::comm::Logger::GetInstance()->LogInfo("INFO: %s::%s:%d " format, typeid(this).name(), __func__, __LINE__, ## args);
#define QLWarn(format, args...) phxqueue::comm::Logger::GetInstance()->LogWarning("WARN: %s::%s:%d " format, typeid(this).name(), __func__, __LINE__, ## args);
#define QLVerb(format, args...) phxqueue::comm::Logger::GetInstance()->LogVerbose("DEBUG: %s::%s:%d " format, typeid(this).name(), __func__, __LINE__, ## args);


enum class LogLevel {
    None = 0,
    Error = 1,
    Warning = 2,
    Info = 3,
    Verbose = 4,
};

using LogFunc = std::function<void (const int, const char *, va_list)>;

class Logger {
  public:
    Logger();
    virtual ~Logger() = default;

    static Logger *GetInstance();

    void LogError(const char *format, ...);

    void LogStatus(const char *format, ...);

    void LogWarning(const char *format, ...);

    void LogInfo(const char *format, ...);

    void LogVerbose(const char *format, ...);

    void SetLogFunc(LogFunc log_func);

  private:
    class LoggerImpl;
    std::unique_ptr<LoggerImpl> impl_;
};


void LogFuncForPhxPaxos(const int log_level, const char *format, va_list args);

void LogFuncForPhxRpc(const int log_level, const char *format, va_list args);


}  // namespace comm

}  // namespace phxqueue

