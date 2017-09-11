/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <cstdlib>
#include <functional>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <string>


namespace phxqueue_phxrpc {

namespace comm {


using INIReadFunc = std::function<void (const std::string &section, const std::string &key, std::string &val)>;


class INI2Pb {
  public:
    INI2Pb(INIReadFunc);
    ~INI2Pb();
    bool Parse(google::protobuf::Message *message, int idx = -1);

  private:
    bool ParseField(google::protobuf::Message *message, const std::string &section_name,
                    const google::protobuf::FieldDescriptor *field_descriptor);
    bool ParseMessage(google::protobuf::Message *message, const std::string &section_name);

    INIReadFunc func_ = nullptr;
};


}  // namespace comm

}  // namespace phxqueeu_phxrpc

