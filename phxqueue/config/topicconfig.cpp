/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/config/topicconfig.h"

#include <algorithm>
#include <iostream>

#include "phxqueue/comm.h"


namespace phxqueue {

namespace config {


using namespace std;

class TopicConfig::TopicConfigImpl {
	public:
		TopicConfigImpl() {}
		virtual ~TopicConfigImpl() {}

		std::map<int, shared_ptr<proto::Pub>> pub_id2pub;
		std::map<int, shared_ptr<proto::Sub>> sub_id2sub;
		std::map<int, shared_ptr<proto::ConsumerGroup>> consumer_group_id2consumer_group;
		std::map<int, shared_ptr<proto::QueueInfo>> queue_info_id2queue_info;
		std::map<int, vector<pair<int, int>>> queue_info_id2ranges;
		std::map<int, int> queue2queue_info_id;
		std::map<int, int> pub_id2tx_query_consumer_group_id;
		std::map<int, set<int>> pub_id2consumer_group_ids;
		std::map<int, set<int>> pub_id2sub_ids;
		std::map<int, int> handle_id2rank;
		std::vector<shared_ptr<proto::FreqInfo>> freq_infos;
		std::vector<unique_ptr<proto::ReplayInfo>> replay_infos; // read only once
};

TopicConfig::TopicConfig() : impl_(new TopicConfigImpl()){}

TopicConfig::~TopicConfig() {}


comm::RetCode TopicConfig::ReadConfig(proto::TopicConfig &proto) {
    // sample
    proto.Clear();

    // topic
    proto.mutable_topic()->set_topic_id(1000);

    // queue info 1
    {
        auto queue_info = proto.add_queue_infos();
        queue_info->set_queue_info_id(1);
        queue_info->add_ranges("0-9");
    }

    // queue info 2
    {
        auto queue_info = proto.add_queue_infos();
        queue_info->set_queue_info_id(2);
        queue_info->add_ranges("10-19");
    }

    // queue info 3 (retry)
    {
        auto queue_info = proto.add_queue_infos();
        queue_info->set_queue_info_id(3);
        queue_info->add_ranges("1000-1009");
        queue_info->set_count(100);
        queue_info->set_delay(10);

    }

    // queue info 4 (retry)
    {
        auto queue_info = proto.add_queue_infos();
        queue_info->set_queue_info_id(4);
        queue_info->add_ranges("1010-1019");
        queue_info->set_count(-1); //retry forever
        queue_info->set_delay(20);
    }


    // pub 1
    {
        auto pub = proto.add_pubs();
        pub->set_pub_id(1);
        pub->add_consumer_group_ids(1);
        pub->add_consumer_group_ids(2);
        pub->add_queue_info_ids(1);
        pub->add_queue_info_ids(3);
    }

    // pub 2
    {
        auto pub = proto.add_pubs();
        pub->set_pub_id(2);
        pub->add_consumer_group_ids(2);
        pub->add_queue_info_ids(2);
        pub->add_queue_info_ids(4);
    }

    // consumer_group 1
    {
        auto consumer_group = proto.add_consumer_groups();
        consumer_group->set_consumer_group_id(1);
        consumer_group->set_use_dynamic_scale(0);
        consumer_group->set_skip_lock(1);
    }

    // consumer_group 2
    {
        auto consumer_group = proto.add_consumer_groups();
        consumer_group->set_consumer_group_id(2);
        consumer_group->set_use_dynamic_scale(0);
        consumer_group->set_skip_lock(1);
    }

    return comm::RetCode::RET_OK;
}


comm::RetCode TopicConfig::Rebuild() {
    bool need_check = NeedCheck();

    impl_->pub_id2pub.clear();
	impl_->sub_id2sub.clear();
    impl_->consumer_group_id2consumer_group.clear();
    impl_->queue_info_id2queue_info.clear();
    impl_->queue_info_id2ranges.clear();
    impl_->queue2queue_info_id.clear();
	impl_->pub_id2tx_query_consumer_group_id.clear();
    impl_->pub_id2consumer_group_ids.clear();
	impl_->pub_id2sub_ids.clear();
    impl_->handle_id2rank.clear();
    impl_->freq_infos.clear();
    impl_->replay_infos.clear();

    auto &&proto = GetProto();

    for (int i{0}; proto.consumer_groups_size() > i; ++i) {
        const auto &consumer_group(proto.consumer_groups(i));
        if (!consumer_group.consumer_group_id()) continue;
        if (need_check) PHX_ASSERT(impl_->consumer_group_id2consumer_group.end() == impl_->consumer_group_id2consumer_group.find(consumer_group.consumer_group_id()), ==, true);
        impl_->consumer_group_id2consumer_group.emplace(consumer_group.consumer_group_id(), make_shared<proto::ConsumerGroup>(consumer_group));
    }

    for (int i{0}; proto.subs_size() > i; ++i) {
        const auto &sub(proto.subs(i));
        if (!sub.sub_id()) continue;
        if (need_check) {
            PHX_ASSERT(impl_->sub_id2sub.end() == impl_->sub_id2sub.find(sub.sub_id()), ==, true);
            PHX_ASSERT(impl_->consumer_group_id2consumer_group.end() == impl_->consumer_group_id2consumer_group.find(sub.consumer_group_id()), ==, false);
        }
		impl_->sub_id2sub.emplace(sub.sub_id(), make_shared<proto::Sub>(sub));
	}

    for (int i{0}; proto.pubs_size() > i; ++i) {
        auto &pub(proto.pubs(i));
        if (!pub.pub_id()) continue;
        if (need_check) PHX_ASSERT(impl_->pub_id2pub.end() == impl_->pub_id2pub.find(pub.pub_id()), ==, true);
        impl_->pub_id2pub.emplace(pub.pub_id(), make_shared<proto::Pub>(pub));

        auto &&consumer_group_ids = impl_->pub_id2consumer_group_ids[pub.pub_id()];
        for (int j{0}; j < pub.consumer_group_ids_size(); ++j) {
            if (need_check) {
                PHX_ASSERT(consumer_group_ids.end() == consumer_group_ids.find(pub.consumer_group_ids(j)), ==, true);
                PHX_ASSERT(impl_->consumer_group_id2consumer_group.end() == impl_->consumer_group_id2consumer_group.find(pub.consumer_group_ids(j)), ==, false);
            }
            consumer_group_ids.insert(pub.consumer_group_ids(j));
        }

        auto &&sub_ids = impl_->pub_id2sub_ids[pub.pub_id()];
        for (int j{0}; j < pub.sub_ids_size(); ++j) {
            if (need_check) {
                PHX_ASSERT(sub_ids.end() == sub_ids.find(pub.sub_ids(j)), ==, true);
                PHX_ASSERT(impl_->sub_id2sub.end() == impl_->sub_id2sub.find(pub.sub_ids(j)), ==, false);
            }
            sub_ids.insert(pub.sub_ids(j));

			auto sub = impl_->sub_id2sub[pub.sub_ids(j)];
            if (need_check) PHX_ASSERT(impl_->consumer_group_id2consumer_group.end() == impl_->consumer_group_id2consumer_group.find(sub->consumer_group_id()), ==, false);
			if (pub.consumer_group_ids_size() == 0) {
				consumer_group_ids.insert(sub->consumer_group_id());
			}
        }

		if (pub.tx_query_sub_id() > 0) {
            if (need_check) PHX_ASSERT(impl_->sub_id2sub.end() == impl_->sub_id2sub.find(pub.tx_query_sub_id()), ==, false);
			auto sub = impl_->sub_id2sub[pub.tx_query_sub_id()];
            if (need_check) PHX_ASSERT(impl_->consumer_group_id2consumer_group.end() == impl_->consumer_group_id2consumer_group.find(sub->consumer_group_id()), ==, false);
			impl_->pub_id2tx_query_consumer_group_id.emplace(pub.pub_id(), sub->consumer_group_id());
		}
    }


    for (int i{0}; proto.queue_infos_size() > i; ++i) {
        const auto &queue_info(proto.queue_infos(i));


        if (!queue_info.queue_info_id()) continue;
        if (need_check) PHX_ASSERT(impl_->queue_info_id2queue_info.end() == impl_->queue_info_id2queue_info.find(queue_info.queue_info_id()), ==, true);
        impl_->queue_info_id2queue_info.emplace(queue_info.queue_info_id(), make_shared<proto::QueueInfo>(queue_info));

        {
            auto &&ranges = impl_->queue_info_id2ranges[queue_info.queue_info_id()];

            for (int j{0}; j < queue_info.ranges_size(); ++j) {
                vector<string> arr;
                comm::utils::StrSplitList(queue_info.ranges(j), "-", arr);
                if (2 == arr.size() && stoi(arr[0]) <= stoi(arr[1])) {
                    ranges.emplace_back(stoi(arr[0]), stoi(arr[1]));
                } else if (1 == arr.size()) {
                    ranges.emplace_back(stoi(arr[0]), stoi(arr[0]));
                }
            }
            sort(ranges.begin(), ranges.end());

            for (auto &&range : ranges) {
                for (int queue = range.first; queue <= range.second; ++queue) {
                    impl_->queue2queue_info_id[queue] = queue_info.queue_info_id();
                }
            }
        }
    }

    for (int i{0}; proto.freq_infos_size() > i; ++i) {
        auto freq_info = shared_ptr<proto::FreqInfo>(new proto::FreqInfo());
        freq_info->CopyFrom(proto.freq_infos(i));
        impl_->freq_infos.push_back(freq_info);
    }

    for (int i{0}; proto.replay_infos_size() > i; ++i) {
        auto replay_info = unique_ptr<proto::ReplayInfo>(new proto::ReplayInfo());
        replay_info->CopyFrom(proto.replay_infos(i));
        impl_->replay_infos.push_back(move(replay_info));
    }

    for (int i{0}; proto.topic().handle_ids_size() > i; ++i) {
        if (need_check) PHX_ASSERT(impl_->handle_id2rank.end() == impl_->handle_id2rank.find(proto.topic().handle_ids(i)), ==, true);
        impl_->handle_id2rank.emplace(proto.topic().handle_ids(i), i);
    }

    return comm::RetCode::RET_OK;
}

/******************* pub ********************/
comm::RetCode TopicConfig::GetAllPub(vector<shared_ptr<const proto::Pub>> &pubs) const {
    pubs.clear();

    for (auto &&it : impl_->pub_id2pub) {
        pubs.push_back(it.second);
    }
    return comm::RetCode::RET_OK;
}

comm::RetCode TopicConfig::GetAllPubID(set<int> &pub_ids) const {
    pub_ids.clear();

    for (auto &&it : impl_->pub_id2pub) {
        pub_ids.insert(it.first);
    }
    return comm::RetCode::RET_OK;
}

bool TopicConfig::IsValidPubID(const int pub_id) const {
    auto &&it = impl_->pub_id2pub.find(pub_id);
    return impl_->pub_id2pub.end() != it;
}

comm::RetCode TopicConfig::GetPubByPubID(const int pub_id, shared_ptr<const proto::Pub> &pub) const {
    pub = nullptr;

    auto &&it = impl_->pub_id2pub.find(pub_id);
    if (impl_->pub_id2pub.end() == it) {
        return comm::RetCode::RET_ERR_RANGE_PUB;
    }
    pub = it->second;

    return comm::RetCode::RET_OK;
}

comm::RetCode TopicConfig::GetPubNameByPubID(const int pub_id, std::string &pub_name) const {
    comm::RetCode ret;
    shared_ptr<const proto::Pub> pub;
    if (comm::RetCode::RET_OK != (ret = GetPubByPubID(pub_id, pub))) return ret;
    if (!pub) return comm::RetCode::RET_ERR_RANGE_PUB;
	pub_name = pub->pub_name();
    return comm::RetCode::RET_OK;
}

comm::RetCode TopicConfig::GetConsumerGroupIDsByPubID(const int pub_id, set<int> &consumer_group_ids) const {
    consumer_group_ids.clear();

	auto &&it = impl_->pub_id2consumer_group_ids.find(pub_id);
    if (impl_->pub_id2consumer_group_ids.end() == it) {
        return comm::RetCode::RET_ERR_RANGE_PUB;
    }
    consumer_group_ids = it->second;

	auto &&iter = impl_->pub_id2tx_query_consumer_group_id.find(pub_id); 
	if (iter != impl_->pub_id2tx_query_consumer_group_id.end()) {
		consumer_group_ids.insert(iter->second);
	}

    return comm::RetCode::RET_OK;
}

comm::RetCode TopicConfig::GetConsumerGroupIDsByHandleID(const int handle_id, const int pub_id, std::set<int> &consumer_group_ids) const {
	consumer_group_ids.clear();

    comm::RetCode ret;
	shared_ptr<const config::proto::Pub> pub;
	if (comm::RetCode::RET_OK != (ret = GetPubByPubID(pub_id, pub))) return ret;

	if (pub->consumer_group_ids_size() > 0) {
		for (int i{0}; i < pub->consumer_group_ids_size(); ++i) {
			consumer_group_ids.insert(pub->consumer_group_ids(i));
		}
	}
	else {
		if (handle_id == comm::proto::HANDLER_TX_QUERY) {
			if (pub->tx_query_sub_id() > 0) {
				shared_ptr<const config::proto::Sub> sub;
				if (comm::RetCode::RET_OK != (ret = GetSubBySubID(pub->tx_query_sub_id(), sub))) return ret;
				consumer_group_ids.insert(sub->consumer_group_id());
			}
		}
		else {
			for (int i{0}; i < pub->sub_ids_size(); i++) {
				shared_ptr<const config::proto::Sub> sub;
				if (comm::RetCode::RET_OK != (ret = GetSubBySubID(pub->sub_ids(i), sub))) return ret;
				consumer_group_ids.insert(sub->consumer_group_id());
			}
		}
	}
	return ret;
}

bool TopicConfig::IsTransaction(const int pub_id) const {
    shared_ptr<const proto::Pub> pub;
    if (comm::RetCode::RET_OK != GetPubByPubID(pub_id, pub)) return false;
	if (!pub) return false;
	return pub->is_transaction() ? true : false;
}

bool TopicConfig::IsAllowClientIDDuplicate(const int pub_id) const {
    shared_ptr<const proto::Pub> pub;
    if (comm::RetCode::RET_OK != GetPubByPubID(pub_id, pub)) return false;
	if (!pub) return false;
	return pub->allow_clientid_duplicate() ? true : false;
}

bool TopicConfig::IsNeedAlarm(const int pub_id, const int cnt) const {
    shared_ptr<const proto::Pub> pub;
    if (comm::RetCode::RET_OK != GetPubByPubID(pub_id, pub)) return false;
	if (!pub) return false;
	return cnt >= pub->alarm_count_limit();
}

/******************* sub ********************/
comm::RetCode TopicConfig::GetAllSub(std::vector<std::shared_ptr<const proto::Sub>> &subs) const {
	subs.clear();

    for (auto &&it : impl_->sub_id2sub) {
        subs.push_back(it.second);
    }
    return comm::RetCode::RET_OK;
}

comm::RetCode TopicConfig::GetAllSubID(set<int> &sub_ids) const {
    sub_ids.clear();

    for (auto &&it : impl_->sub_id2sub) {
        sub_ids.insert(it.first);
    }
    return comm::RetCode::RET_OK;
}

bool TopicConfig::IsValidSubID(const int sub_id) const {
    auto &&it = impl_->sub_id2sub.find(sub_id);
    return impl_->sub_id2sub.end() != it;
}

comm::RetCode TopicConfig::GetSubBySubID(const int sub_id, shared_ptr<const proto::Sub> &sub) const {
    sub = nullptr;

    auto &&it = impl_->sub_id2sub.find(sub_id);
    if (impl_->sub_id2sub.end() == it) {
        return comm::RetCode::RET_ERR_RANGE_SUB;
    }
    sub = it->second;

    return comm::RetCode::RET_OK;
}

comm::RetCode TopicConfig::GetSubNameBySubID(const int sub_id, std::string &sub_name) const {
    comm::RetCode ret;
    shared_ptr<const proto::Sub> sub;
    if (comm::RetCode::RET_OK != (ret = GetSubBySubID(sub_id, sub))) return ret;
    if (!sub) return comm::RetCode::RET_ERR_RANGE_SUB;
	sub_name = sub->sub_name();
    return comm::RetCode::RET_OK;
}

comm::RetCode TopicConfig::GetSubIDsByPubID(const int pub_id, std::set<int> &sub_ids) const {
	sub_ids.clear();

    comm::RetCode ret;
    shared_ptr<const proto::Pub> pub;
    if (comm::RetCode::RET_OK != (ret = GetPubByPubID(pub_id, pub))) return ret;
    if (!pub) return comm::RetCode::RET_ERR_RANGE_PUB;
    for (int i{0}; i < pub->sub_ids_size(); ++i) {
		sub_ids.insert(pub->sub_ids(i));
    }
    return comm::RetCode::RET_OK;
}

comm::RetCode TopicConfig::GetTxQuerySubIDByPubID(const int pub_id, int &sub_id) const {
    comm::RetCode ret;
    shared_ptr<const proto::Pub> pub;
    if (comm::RetCode::RET_OK != (ret = GetPubByPubID(pub_id, pub))) return ret;
    if (!pub) return comm::RetCode::RET_ERR_RANGE_PUB;
	sub_id = pub->tx_query_sub_id();
    return comm::RetCode::RET_OK;
}

/******************* consumer_group ********************/
comm::RetCode TopicConfig::GetAllConsumerGroup(vector<shared_ptr<const proto::ConsumerGroup>> &consumer_groups) const {
    consumer_groups.clear();

    for (auto &&it : impl_->consumer_group_id2consumer_group) {
        consumer_groups.push_back(it.second);
    }
    return comm::RetCode::RET_OK;
}

comm::RetCode TopicConfig::GetAllConsumerGroupID(set<int> &consumer_group_ids) const {
    consumer_group_ids.clear();

    for (auto &&it : impl_->consumer_group_id2consumer_group) {
        consumer_group_ids.insert(it.first);
    }
    return comm::RetCode::RET_OK;
}

bool TopicConfig::IsValidConsumerGroupID(const int consumer_group_id) const {
    auto &&it = impl_->consumer_group_id2consumer_group.find(consumer_group_id);
    return impl_->consumer_group_id2consumer_group.end() != it;
}

comm::RetCode TopicConfig::GetConsumerGroupByConsumerGroupID(const int consumer_group_id, shared_ptr<const proto::ConsumerGroup> &consumer_group) const {
    consumer_group = nullptr;

    auto &&it = impl_->consumer_group_id2consumer_group.find(consumer_group_id);
    if (impl_->consumer_group_id2consumer_group.end() == it) {
        return comm::RetCode::RET_ERR_RANGE_CONSUMER_GROUP;
    }
    consumer_group = it->second;

    return comm::RetCode::RET_OK;
}


/******************* queue_info ********************/

comm::RetCode TopicConfig::GetQueueInfoIDRankByPubID(const int queue_info_id, const int pub_id, uint64_t &rank, const comm::proto::QueueType queue_type) const {
    rank = 0;
	comm::RetCode ret;
	std::vector<int> queue_info_ids;
	if (comm::RetCode::RET_OK != (ret = GetQueueInfoIDsByQueueType(pub_id, queue_info_ids, queue_type))) {
		return ret;
	}

	for (int i{0}; i < queue_info_ids.size(); ++i) {
        if (queue_info_id == queue_info_ids.at(i)) {
            rank = i;
            return comm::RetCode::RET_OK;
        }
    }
    return comm::RetCode::RET_ERR_RANGE_QUEUE_INFO;
}

bool TopicConfig::IsValidQueue(const int queue_id, const int pub_id, const int consumer_group_id) const {
    auto &&g = [&](const int queue_info_id)->bool {
		auto &&it = impl_->queue_info_id2ranges.find(queue_info_id);
		if (impl_->queue_info_id2ranges.end() == it) return false;
		auto &&ranges = it->second;
		for (auto &&range : ranges) {
			if (range.first <= queue_id && queue_id <= range.second) {
				return true;
			}
		}
		return false;
    };

    auto &&f = [&](const int pub_id)->bool {
		shared_ptr<const proto::Pub> pub;
		if (comm::RetCode::RET_OK != GetPubByPubID(pub_id, pub) || !pub) {
			return false;
		}
		if (pub->is_transaction()) {
			bool valid_consumer_group = true;
			if (consumer_group_id != -1) {
				auto &&it = impl_->pub_id2tx_query_consumer_group_id.find(pub_id);
				if (it == impl_->pub_id2tx_query_consumer_group_id.end() || it->second != consumer_group_id) {
                    valid_consumer_group = false;
                }
			}
			if (valid_consumer_group) {
				for (int i{0}; i < pub->tx_query_queue_info_ids_size(); i++) {
					if (g(pub->tx_query_queue_info_ids(i))) return true;
				}
			}
		}

		if (consumer_group_id != -1) {
			auto &&it = impl_->pub_id2consumer_group_ids.find(pub_id);
			if (it != impl_->pub_id2consumer_group_ids.end()) {
				auto &&consumer_group_ids = it->second;
				if (consumer_group_ids.find(consumer_group_id) == consumer_group_ids.end()) {
					return false;
				}
			}
			else return false;
		}
		for (int i{0}; i < pub->queue_info_ids_size(); i++) {
			if (g(pub->queue_info_ids(i))) return true;
		}
		return false;
	};

	if (-1 != pub_id) {
		return f(pub_id);
	} else {
		for (auto &&kv : impl_->pub_id2pub) {
			if (f(kv.first)) return true;
		}
	}

	return false;
}

comm::RetCode TopicConfig::GetQueuesByQueueInfoID(const int queue_info_id, set<int> &queues) const {
	queues.clear();

	auto &&it = impl_->queue_info_id2ranges.find(queue_info_id);
	if (it == impl_->queue_info_id2ranges.end()) return comm::RetCode::RET_ERR_RANGE_QUEUE_INFO;
	auto &&ranges = it->second;

	for (auto &&range : ranges) {
		for (int queue{range.first}; queue <= range.second; ++queue) {
			queues.insert(queue);
		}
	}

	return comm::RetCode::RET_OK;
}


comm::RetCode TopicConfig::GetQueueDelay(const int queue, int &delay) const {
	comm::RetCode ret;
	shared_ptr<const proto::QueueInfo> queue_info;
	if (comm::RetCode::RET_OK != (ret = GetQueueInfoByQueue(queue, queue_info))) {
		return ret;
	}
	delay = queue_info->delay();

	return comm::RetCode::RET_OK;
}

comm::RetCode TopicConfig::GetNQueue(const int queue_info_id, int &nqueue) const {
	auto &&it(impl_->queue_info_id2ranges.find(queue_info_id));
	if (it == impl_->queue_info_id2ranges.end()) return comm::RetCode::RET_ERR_RANGE_QUEUE_INFO;
	auto &&ranges = it->second;

	nqueue = 0;
	for (auto &&range : ranges) {
		nqueue += range.second - range.first + 1;
	}
	return comm::RetCode::RET_OK;
}

comm::RetCode TopicConfig::GetQueueByRank(const int queue_info_id, const uint64_t rank, int &queue) const {
	auto &&it = impl_->queue_info_id2ranges.find(queue_info_id);
	if (it == impl_->queue_info_id2ranges.end()) return comm::RetCode::RET_ERR_RANGE_QUEUE_INFO;
	auto &&ranges = it->second;

	if (0 == ranges.size()) return comm::RetCode::RET_ERR_RANGE_RANK;

	uint64_t r{rank};
	for (auto &&range : ranges) {
		uint64_t w{range.second - range.first + 1};
		if (r >= w) r -= w;
		else {
			queue = (int)(range.first + r);
			return comm::RetCode::RET_OK;
		}
	}
	return comm::RetCode::RET_ERR_RANGE_RANK;
}

comm::RetCode TopicConfig::GetQueueByLoopRank(const int queue_info_id, const uint64_t rank, int &queue) const {
	comm::RetCode ret;
	int nqueue{0};
	if (comm::RetCode::RET_OK != (ret = GetNQueue(queue_info_id, nqueue))) {
		return ret;
	}
	if (0 == nqueue) {
		return comm::RetCode::RET_ERR_NQUEUE_INVALID;
	}
	return GetQueueByRank(queue_info_id, rank % nqueue, queue);
}

comm::RetCode TopicConfig::GetQueueInfoIDByQueue(const int queue, int &queue_info_id) const {
	auto &&it = impl_->queue2queue_info_id.find(queue);
	if (it == impl_->queue2queue_info_id.end()) return comm::RetCode::RET_ERR_RANGE_QUEUE;
    queue_info_id = it->second;
    return comm::RetCode::RET_OK;
}

comm::RetCode TopicConfig::GetQueueInfoByQueue(const int queue, shared_ptr<const proto::QueueInfo> &queue_info) const {
	queue_info = nullptr;
	auto &&it = impl_->queue2queue_info_id.find(queue);
	if (it == impl_->queue2queue_info_id.end()) return comm::RetCode::RET_ERR_RANGE_QUEUE;

	return GetQueueInfoByQueueInfoID(it->second, queue_info);
}

comm::RetCode TopicConfig::GetQueueInfoByQueueInfoID(const int queue_info_id, shared_ptr<const proto::QueueInfo> &queue_info) const {
	queue_info = nullptr;

    auto &&it = impl_->queue_info_id2queue_info.find(queue_info_id);
    if (impl_->queue_info_id2queue_info.end() != it) {
        queue_info = it->second;
        return comm::RetCode::RET_OK;
    }
    return comm::RetCode::RET_ERR_RANGE_QUEUE_INFO;
}

bool TopicConfig::QueueShouldSkip(const int queue, const int consumer_group_id) const {
    comm::RetCode ret;
    int queue_info_id;
    if (comm::RetCode::RET_OK != GetQueueInfoIDByQueue(queue, queue_info_id)) return true;

    auto &&proto = GetProto();
    for (int i{0}; i < proto.skip_infos_size(); ++i) {
        auto &&skip_info = proto.skip_infos(i);
        if (skip_info.uin() == 0 &&
            skip_info.pub_id() == -1 &&
            skip_info.handle_id() == -1 &&
            (skip_info.consumer_group_id() == -1 || consumer_group_id == -1 || skip_info.consumer_group_id() == consumer_group_id) &&
            (skip_info.queue_info_id() == -1 || skip_info.queue_info_id() == queue_info_id) &&
			skip_info.has_client_id() == false) {
            return true;
        }
    }
    return false;
}

bool TopicConfig::ItemShouldSkip(const comm::proto::QItem &item, const int consumer_group_id, const int queue_info_id) const {
    auto &&proto = GetProto();
    for (int i{0}; i < proto.skip_infos_size(); ++i) {
        auto &&skip_info = proto.skip_infos(i);
        if ((skip_info.uin() == 0 || skip_info.uin() == item.meta().uin()) &&
            (skip_info.pub_id() == -1 || skip_info.pub_id() == item.pub_id()) &&
            (skip_info.handle_id() == -1 || skip_info.handle_id() == item.handle_id()) &&
            (skip_info.consumer_group_id() == -1 || skip_info.consumer_group_id() == consumer_group_id) &&
            (skip_info.queue_info_id() == -1 || skip_info.queue_info_id() == queue_info_id) &&
			(skip_info.has_client_id() == false || skip_info.client_id() == item.meta().client_id()) ) {
            return true;
        }
    }
    return false;
}

comm::RetCode TopicConfig::GetQueueInfoIDsByQueueType(const int pub_id, std::vector<int> &queue_info_ids, const comm::proto::QueueType queue_type) const {
    comm::RetCode ret;
    shared_ptr<const proto::Pub> pub;
    if (comm::RetCode::RET_OK != (ret = GetPubByPubID(pub_id, pub)) || !pub) {
        return comm::RetCode::RET_ERR_RANGE_PUB;
    }

	if (queue_type == comm::proto::QueueType::NORMAL_QUEUE) {
		for (int i{0}; i < pub->queue_info_ids_size(); i++) {
			queue_info_ids.push_back(pub->queue_info_ids(i));
		}
	}
	else if (queue_type == comm::proto::QueueType::TX_QUERY_QUEUE) {
		for (int i{0}; i < pub->tx_query_queue_info_ids_size(); i++) {
			queue_info_ids.push_back(pub->tx_query_queue_info_ids(i));
		}
	}
	else {
		return comm::RetCode::RET_ERR_ARG;
	}
	return comm::RetCode::RET_OK;
}

comm::RetCode TopicConfig::GetQueueInfoIDByCount(const int pub_id, const int cnt, int &queue_info_id, const comm::proto::QueueType queue_type) const {
    comm::RetCode ret;
	std::vector<int> queue_info_ids;
	if (comm::RetCode::RET_OK != (ret = GetQueueInfoIDsByQueueType(pub_id, queue_info_ids, queue_type))) {
		return ret;
	}

    int c{cnt};
    for (int i{0}; i < queue_info_ids.size(); ++i) {
        const int queue_info_id_tmp = queue_info_ids.at(i);
        shared_ptr<const proto::QueueInfo> queue_info;
        if (comm::RetCode::RET_OK != (ret = GetQueueInfoByQueueInfoID(queue_info_id_tmp, queue_info)) || !queue_info) {
            return comm::RetCode::RET_ERR_RANGE_QUEUE_INFO;
        }
        if (-1 != queue_info->count() && c >= queue_info->count()) {
            c -= queue_info->count();
        } else {
            queue_info_id = queue_info_id_tmp;
            return comm::RetCode::RET_OK;
        }
    }
    return comm::RetCode::RET_ERR_RANGE_CNT;
}

comm::RetCode TopicConfig::GetHandleIDRank(const int handle_id, int &rank) const {
    auto &&it = impl_->handle_id2rank.find(handle_id);
    if (impl_->handle_id2rank.end() == it) return comm::RetCode::RET_ERR_RANGE_HANDLE;
    rank = it->second;
    return comm::RetCode::RET_OK;
}

/******************* freq ********************/
comm::RetCode TopicConfig::GetAllFreqInfo(std::vector<shared_ptr<proto::FreqInfo> > &freq_infos) const {
    freq_infos = impl_->freq_infos;
    return comm::RetCode::RET_OK;
}

/******************* replay ********************/
comm::RetCode TopicConfig::GetAllReplayInfo(std::vector<unique_ptr<proto::ReplayInfo> > &replay_infos) const {
    for (auto &&replay_info : impl_->replay_infos) {
        replay_infos.push_back(move(replay_info));
    }
    impl_->replay_infos.clear();
    return comm::RetCode::RET_OK;
}


}  // namespace config

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

