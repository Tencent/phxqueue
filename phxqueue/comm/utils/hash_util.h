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
#include <list>
#include <map>
#include <vector>

namespace phxqueue {

namespace comm {

namespace utils {


uint64_t MurmurHash64(const void *key, size_t len, uint64_t seed);
int JumpConsistentHash64(uint64_t key, int32_t num_buckets);

template <typename Key, typename Node>
class ConsistenHash {
  public:
    ConsistenHash() {}
    ~ConsistenHash() {}

    using NodeScale = std::pair<Node, int>;
    bool Init(const std::list<NodeScale> &node_scale_list, std::function<uint64_t (const Key &)> key_hash_func, std::function<uint64_t (const Node &, int scale)> node_hash_func) {

        key_hash_func_ = key_hash_func;
		node_hash_func_ = node_hash_func;

        if (node_scale_list.empty()) return false;
        hash2node_.clear();
		node_list_.clear();
        for (auto &&node_scale : node_scale_list) {
            auto &&node = node_scale.first;
            auto &&scale = node_scale.second;
			node_list_.push_back(node);

            for (int s{0}; s < scale; ++s) {
                auto node_hash = node_hash_func_(node, s);
                hash2node_.emplace(node_hash, node);
            }
        }

        if (hash2node_.size()) return true;
        return false;
    }

	void AddNode(const Node & node, int scale)
	{
		if (scale >0) node_list_.push_back(node);
		for (int s{0}; s < scale; ++s) 
		{
			auto node_hash = node_hash_func_(node, s);
			hash2node_.emplace(node_hash, node);
		}
	}

	void GetAllNode(std::list<Node> &node_list)
	{
		node_list.assign(node_list_.begin(), node_list_.end());
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
	std::function<uint64_t (const Node &, int scale)> node_hash_func_;
	std::list<Node> node_list_;
};  

template <typename Key, typename Node>
class JumpConsistenHash {
	public:
		JumpConsistenHash() {}
		~JumpConsistenHash() {}

		void Init(std::function<uint64_t (const Key &)> key_hash_func, std::function<uint64_t (const Node &, int scale)> node_hash_func)
		{
			key_hash_func_ = key_hash_func;
			node_hash_func_ = node_hash_func;
			hash_node_list_.clear();
			bucket_num_ = 0;
		}

		void AddNode(const Node & node, int scale)
		{
			if (scale >0) {
				bucket_num_ += scale;
				auto node_hash = node_hash_func_(node, scale);
				hash_node_list_.push_back(HashNode(node_hash, node, bucket_num_));
			}
		}

		bool PickNodeByKey(const Key key, Node &node) 
		{
			if (bucket_num_ == 0) return false;

			auto key_hash = key_hash_func_(key);
			int idx = JumpConsistentHash64(key_hash, bucket_num_);

			int L = 0, R = hash_node_list_.size() - 1, node_idx = R;
			while (L <= R) {
				int mid = (L + R) >> 1;
				if (hash_node_list_[mid].count >= idx) {
					node_idx = mid;
					R = mid - 1;
				}
				else L = mid + 1;
			}

			node = hash_node_list_[node_idx].node;

			return true;
		}

		void GetAllNode(std::list<Node> &node_list)
		{
			for (int i = 0; i < hash_node_list_.size(); i++) {
				node_list.push_back(hash_node_list_[i].node);
			}
		}

	private:
		struct HashNode {
			uint64_t node_hash;
			Node node;
			int count;
			HashNode();
			HashNode(uint64_t _node_hash, Node _node, int _count) {
				node_hash = _node_hash;
				node = _node;
				count = _count;
			}
			bool operator<(const HashNode& other)const {
				return node_hash < other.node_hash;
			}
		};
		int bucket_num_;
		std::vector<HashNode> hash_node_list_;
		std::function<uint64_t (const Key &)> key_hash_func_;
		std::function<uint64_t (const Node &, int scale)> node_hash_func_;
};


}  // namespace utils

}  // namespace comm

}  // namespace phxqueue

