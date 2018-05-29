/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>
#include <list>
#include <map>

namespace phxqueue {

namespace comm {

namespace utils {

uint64_t MurmurHash64(const void *key, size_t len, uint64_t seed);

template <typename Key, typename Node>
class ConsistenHash {
public:
    ConsistenHash() {}
    ~ConsistenHash() {}

    using NodeScale = std::pair<Node, int>;
    bool Init(const std::list<NodeScale> &node_scale_list, std::function<uint64_t (const Key &)> key_hash_func, std::function<uint64_t (const Node &, int scale)> node_hash_func) {
        if (node_scale_list.empty()) return false;

        key_hash_func_ = key_hash_func;

        hash2node_.clear();
        for (auto &&node_scale : node_scale_list) {
            auto &&node = node_scale.first;
            auto &&scale = node_scale.second;

            for (int s{0}; s < scale; ++s) {
                auto node_hash = node_hash_func(node, s);
                hash2node_.emplace(node_hash, node);
            }
        }

        if (hash2node_.size()) return true;
        return false;
    }

    bool PickNodeByKey(const Key key, Node &node) {
        auto key_hash = key_hash_func_(key);

        auto &&it = hash2node_.lower_bound(key_hash);
        if (hash2node_.end() == it) it = hash2node_.begin();
        if (hash2node_.end() == it) return false;
        node = it->second;
        return true;
    }

private:
    std::map<uint64_t, Node> hash2node_;
    std::function<uint64_t (const Key &)> key_hash_func_;
};

}  // namespace utils

}  // namespace comm

}  // namespace phxqueue


