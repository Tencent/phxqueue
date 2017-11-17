/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <cassert>
#include <cinttypes>
#include <cstdio>
#include <functional>
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

    void Log(const int log_level, const char *format, va_list args);

  private:
    class LoggerImpl;
    std::unique_ptr<LoggerImpl> impl_;
};


void LogFuncForPhxPaxos(const int log_level, const char *format, va_list args);

void LogFuncForPhxRpc(const int log_level, const char *format, va_list args);


}  // namespace comm

}  // namespace phxqueue

