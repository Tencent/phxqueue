/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue_phxrpc/consumer/proto/consumerserverconfig.pb.h"

#include <iostream>

#include <google/protobuf/util/json_util.h>

#include "phxqueue_phxrpc/comm.h"


namespace phxqueue_phxrpc {

namespace consumer {


class ConsumerServerConfig : public comm::FileLoader {
  public:
    ConsumerServerConfig(const std::string &file) : comm::FileLoader(file) {}
    virtual ~ConsumerServerConfig() override {}

    proto::ConsumerServerConfig & GetProto() {
        return proto_;
    }

    virtual bool LoadFile(const std::string &file, bool is_reload) {
        comm::FileConfig config(file);
        if (!config.Read()) {
            return false;
        }

        std::string json;
        if (!config.GetContent(json)) {
            return false;
        }

        google::protobuf::util::JsonParseOptions opt;
        opt.ignore_unknown_fields = true;
        auto status = JsonStringToMessage(json, &proto_, opt);
        if (!status.ok()) {
            return false;
        }

        return true;
    }

  private:
    proto::ConsumerServerConfig proto_;
};


}  // namespace test

}  // namespace phxqueue_phxrpc
