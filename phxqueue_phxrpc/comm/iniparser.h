/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>
#include <string>
#include <vector>


namespace phxqueue_phxrpc {

namespace comm {


class INIParser {
  public:
    INIParser();
    virtual ~INIParser();

    void ParseContent(const std::string &content);
    bool GetValue(const std::string &section, const std::string &key, std::string &val);
    void SetDelAfterGet(bool del_after_get);
    void GetAllValue(std::vector<std::tuple<std::string, std::string, std::string>> &vals);

  private:
    class INIParserImpl;
    std::unique_ptr<INIParserImpl> impl_;
};


}  // namespace comm

}  // namespace phxqueue_phxrpc

