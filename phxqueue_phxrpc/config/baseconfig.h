/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <map>
#include <set>
#include <google/protobuf/util/json_util.h>

#include "phxqueue/comm.h"
#include "phxqueue_phxrpc/comm.h"


namespace phxqueue_phxrpc {

namespace config {

template <typename ConfigType> class BaseConfig : public comm::FileLoader, public ConfigType {
  public:
    BaseConfig() : comm::FileLoader(), ConfigType() {}
    virtual ~BaseConfig() {}

    virtual bool LoadFile(const std::string &file, bool is_reload) {
        bool need_check = ConfigType::NeedCheck();
        comm::FileConfig config(file);
        if (!config.Read()) {
            QLErr("FileConfig::Read fail. file %s", file.c_str());
            return false;
        }

        std::string content;
        if (!config.GetContent(content)) {
            QLErr("FileConfig::GetContent fail. content %s", content.c_str());
            return false;
        }

        typename ConfigType::ProtoType proto;

        google::protobuf::util::JsonParseOptions opt;
        opt.ignore_unknown_fields = !need_check;
        auto status = JsonStringToMessage(content, &proto, opt);
        if (!status.ok()) {
            QLErr("JsonStringToMessage fail. %s", status.ToString().c_str());
            assert(!need_check);
            return false;
        }

        ConfigType::FillConfig(std::move(proto));

        return true;
    }

    virtual phxqueue::comm::RetCode ReadConfig(typename ConfigType::ProtoType &proto) {
        return phxqueue::comm::RetCode::RET_OK;
    }

    virtual bool IsModified() {
        bool modified = false;
        comm::FileLoader::LoadIfModified(modified);
        modified |= ConfigType::IsModified();
        return modified;
    }

    void SetNeedCheck(bool need_check) {
        ConfigType::SetNeedCheck(need_check);
    }
};


}  // namespace config

}  // namespace phxqueue_phxrpc

