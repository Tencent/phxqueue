/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <map>
#include <mutex>
#include <queue>


namespace phxqueue {

namespace comm {


template <typename KeyType, typename ResourceType>
class ResourcePool final {
  public:
    ResourcePool() {}
    ~ResourcePool() {}

    static ResourcePool *GetInstance() {
        static ResourcePool pool;
        return &pool;
    }

    std::unique_ptr<ResourceType> Get(const KeyType &key) {
        std::lock_guard<std::mutex> lg(lock_);

        auto &&it(key2resources_.find(key));
        if (key2resources_.end() == it) return nullptr;
        auto &resources = it->second;
        if (resources.empty()) return nullptr;
        auto resource = std::move(resources.front());
        resources.pop();
        return resource;
    }

    void Put(const KeyType &key, std::unique_ptr<ResourceType> &resource) {
        std::lock_guard<std::mutex> lg(lock_);

        auto &&resources = key2resources_[key];
        resources.push(std::move(resource));
    }

  protected:
    std::map<KeyType, std::queue<std::unique_ptr<ResourceType>>> key2resources_;
    std::mutex lock_;
};


}  // namespace comm

}  // namespace phxqueue

