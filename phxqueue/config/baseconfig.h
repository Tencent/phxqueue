/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <chrono>
#include <memory>

#include "phxqueue/comm.h"


namespace phxqueue {

namespace config {


template <typename T> class BaseConfig {
  public:
    typedef T ProtoType;

    BaseConfig() {}
    virtual ~BaseConfig() {}

    uint64_t GetLastModTime() const {return last_mod_time_;}

    const ProtoType & GetProto() const {
        return proto_;
    }

    void FillConfig(ProtoType &&proto) {
        proto_.CopyFrom(proto);
    }

    comm::RetCode Load() {
        comm::RetCode ret;
        if (!IsModified()) return comm::RetCode::RET_OK;
        QLVerb("ReadConfig");
        if (comm::RetCode::RET_OK != (ret = ReadConfig(proto_))) return ret;
        QLVerb("Rebuild");
        if (comm::RetCode::RET_OK != (ret = Rebuild())) return ret;

        BaseConfig::UpdateModTime();

        QLVerb("OK");
        return comm::RetCode::RET_OK;
    }

    void SetNeedCheck(bool need_check) {
        need_check_ = need_check;
    }

    bool NeedCheck() {
        return need_check_;
    }

  protected:
    ProtoType &GetMutableProto() {
        return proto_;
    }

    virtual bool IsModified() { if (!build_) { build_ = true; return true; } return false; }

    virtual comm::RetCode ReadConfig(ProtoType &config_pb) {return comm::RetCode::RET_OK;}

    virtual comm::RetCode Rebuild() = 0;

    void UpdateModTime() {
        last_mod_time_ = (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())).count();
    }

    void NeedRebuild() {
        auto now_time((std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())).count());
        QLVerb("now_time %llu last_clear_build_time_ %llu", now_time, last_clear_build_time_);
        if (now_time > last_clear_build_time_ + 10000) {  // 10s
            QLVerb("clear build");
            last_clear_build_time_ = now_time;
            build_ = false;
        }
    }

  private:
    bool build_{false};
    uint64_t last_clear_build_time_{0};
    uint64_t last_mod_time_{0};
    ProtoType proto_;
    bool need_check_{false};
};


}  // namespace config

}  // namespace phxqueue

