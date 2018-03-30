/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <algorithm>

#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"

#include "phxqueue/config/globalconfig.h"


namespace phxqueue {

namespace config {


using namespace std;

struct GlobalConfig::GlobalConfigImpl_t {
		std::map<string, int> topic_name2topic_id;
		std::map<int, int> handle_id2topic_id;
		std::map<int, shared_ptr<TopicConfig> > topic_id2topic_config;
		std::map<int, shared_ptr<ConsumerConfig> > topic_id2consumer_config;
		std::map<int, shared_ptr<StoreConfig> > topic_id2store_config;
		std::map<int, shared_ptr<SchedulerConfig> > topic_id2scheduler_config;
		std::map<int, shared_ptr<LockConfig> > topic_id2lock_config;
};


GlobalConfig::GlobalConfig() : impl_(new GlobalConfigImpl_t()) {
    assert(impl_);
}

GlobalConfig::~GlobalConfig() {}

GlobalConfig *GlobalConfig::GetThreadInstance() {
    static thread_local GlobalConfig *global_config = nullptr;
    if (!global_config) {
        global_config = plugin::ConfigFactory::GetInstance()->NewGlobalConfig().release();
    }
    assert(global_config);
    global_config->Load();

    return global_config;
}

comm::RetCode GlobalConfig::ReadConfig(proto::GlobalConfig &proto) {
    // sample
    proto.Clear();

    auto topic_info = proto.add_topic_infos();
    topic_info->set_topic_id(1000);
    topic_info->set_topic_name("test");
    topic_info->set_topic_config_path("");
    topic_info->set_consumer_config_path("");
    topic_info->set_store_config_path("");
    topic_info->set_scheduler_config_path("");
    topic_info->set_lock_config_path("");

    return comm::RetCode::RET_OK;
}

comm::RetCode GlobalConfig::Rebuild() {
    impl_->topic_name2topic_id.clear();
    impl_->topic_id2topic_config.clear();
    impl_->topic_id2consumer_config.clear();
    impl_->topic_id2store_config.clear();
    impl_->topic_id2scheduler_config.clear();
    impl_->topic_id2lock_config.clear();
    impl_->handle_id2topic_id.clear();

    comm::RetCode ret;

    auto &&proto = GetProto();
    for (int i{0}; i < proto.topic_infos_size(); ++i) {
        auto &&topic_info = proto.topic_infos(i);

        impl_->topic_name2topic_id.emplace(topic_info.topic_name(), topic_info.topic_id());

        shared_ptr<TopicConfig> topic_config = plugin::ConfigFactory::GetInstance()->NewTopicConfig(topic_info.topic_id(), topic_info.topic_config_path());
        if (topic_config) {
            topic_config->Load();
            impl_->topic_id2topic_config.emplace(topic_info.topic_id(), topic_config);
        }

        for (int j{0}; j < topic_config->GetProto().topic().handle_ids_size(); ++j) {
            impl_->handle_id2topic_id[topic_config->GetProto().topic().handle_ids(j)] = topic_info.topic_id();
        }

        auto &&consumer_config = plugin::ConfigFactory::GetInstance()->NewConsumerConfig(topic_info.topic_id(), topic_info.consumer_config_path());
        if (consumer_config) {
            consumer_config->Load();
            impl_->topic_id2consumer_config.emplace(topic_info.topic_id(), move(consumer_config));
        }

        auto &&store_config = plugin::ConfigFactory::GetInstance()->NewStoreConfig(topic_info.topic_id(), topic_info.store_config_path());
        if (store_config) {
            store_config->Load();
            impl_->topic_id2store_config.emplace(topic_info.topic_id(), move(store_config));
        }

        auto &&scheduler_config = plugin::ConfigFactory::GetInstance()->NewSchedulerConfig(topic_info.topic_id(), topic_info.scheduler_config_path());
        if (scheduler_config) {
            scheduler_config->Load();
            impl_->topic_id2scheduler_config.emplace(topic_info.topic_id(), move(scheduler_config));
        }

        auto &&lock_config = plugin::ConfigFactory::GetInstance()->NewLockConfig(topic_info.topic_id(), topic_info.lock_config_path());
        if (lock_config) {
            lock_config->Load();
            impl_->topic_id2lock_config.emplace(topic_info.topic_id(), move(lock_config));
        }
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode GlobalConfig::GetTopicIDByTopicName(const string &topic_name, int &topic_id) const {
    auto &&it = impl_->topic_name2topic_id.find(topic_name);
    if (it == impl_->topic_name2topic_id.end()) {
        return comm::RetCode::RET_ERR_RANGE_TOPIC;
    }
    topic_id = it->second;
    return comm::RetCode::RET_OK;
}

comm::RetCode GlobalConfig::GetTopicNameByTopicID(const int topic_id, string &topic_name) const {
	for (auto &&it = impl_->topic_name2topic_id.begin(); it != impl_->topic_name2topic_id.end(); it++) {
		if (it->second == topic_id) {
			topic_name = it->first;
			return comm::RetCode::RET_OK;
		}
	}
	return comm::RetCode::RET_ERR_RANGE_TOPIC;
}

comm::RetCode GlobalConfig::GetTopicConfigByTopicID(const int topic_id, shared_ptr<const TopicConfig> &topic_config) {
    topic_config = nullptr;

    auto it = impl_->topic_id2topic_config.find(topic_id);
    if (it == impl_->topic_id2topic_config.end()) {
        NeedRebuild();
        return comm::RetCode::RET_ERR_RANGE_TOPIC;
    }
    if (!it->second) return comm::RetCode::RET_ERR_RANGE_TOPIC;
    it->second->Load();
    topic_config = it->second;
    return comm::RetCode::RET_OK;
}


comm::RetCode GlobalConfig::GetAllTopicConfig(vector<shared_ptr<const TopicConfig> > &topic_configs) const {
    topic_configs.clear();
    for (auto &&it : impl_->topic_id2topic_config) {
        auto &&topic_config = it.second;
        if (topic_config) {
            topic_config->Load();
            topic_configs.push_back(topic_config);
        }
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode GlobalConfig::GetAllTopicID(set<int> &topic_ids) const {
    topic_ids.clear();
    for (auto &&it : impl_->topic_id2topic_config) {
        topic_ids.insert(it.first);
    }
    return comm::RetCode::RET_OK;
}

comm::RetCode GlobalConfig::GetTopicIDByHandleID(const int handle_id, int &topic_id) const {
    topic_id = 0;
    auto it = impl_->handle_id2topic_id.find(handle_id);
    if (impl_->handle_id2topic_id.end() == it) return comm::RetCode::RET_ERR_RANGE_HANDLE;
    topic_id = it->second;
    return comm::RetCode::RET_OK;
}

comm::RetCode GlobalConfig::GetConsumerConfig(const int topic_id, shared_ptr<const ConsumerConfig> &consumer_config) {
    consumer_config = nullptr;

    auto &&it = impl_->topic_id2consumer_config.find(topic_id);
    if (it == impl_->topic_id2consumer_config.end()) {
        NeedRebuild();
        return comm::RetCode::RET_ERR_RANGE_TOPIC;
    }
    if (!it->second) return comm::RetCode::RET_ERR_RANGE_TOPIC;
    it->second->Load();
    consumer_config = it->second;
    return comm::RetCode::RET_OK;
}

comm::RetCode GlobalConfig::GetStoreConfig(const int topic_id, shared_ptr<const StoreConfig> &store_config) {
    store_config = nullptr;

    auto &&it = impl_->topic_id2store_config.find(topic_id);
    if (it == impl_->topic_id2store_config.end()) {
        NeedRebuild();
        return comm::RetCode::RET_ERR_RANGE_TOPIC;
    }
    if (!it->second) return comm::RetCode::RET_ERR_RANGE_TOPIC;
    it->second->Load();
    store_config = it->second;
    return comm::RetCode::RET_OK;
}

comm::RetCode GlobalConfig::GetSchedulerConfig(const int topic_id, shared_ptr<const SchedulerConfig> &scheduler_config) {
    scheduler_config = nullptr;

    auto &&it = impl_->topic_id2scheduler_config.find(topic_id);
    if (it == impl_->topic_id2scheduler_config.end()) {
        NeedRebuild();
        return comm::RetCode::RET_ERR_RANGE_TOPIC;
    }
    if (!it->second) return comm::RetCode::RET_ERR_RANGE_TOPIC;
    it->second->Load();
    scheduler_config = it->second;
    return comm::RetCode::RET_OK;
}

comm::RetCode GlobalConfig::GetLockConfig(const int topic_id, shared_ptr<const LockConfig> &lock_config) {
    lock_config = nullptr;

    auto &&it = impl_->topic_id2lock_config.find(topic_id);
    if (it == impl_->topic_id2lock_config.end()) {
        NeedRebuild();
        return comm::RetCode::RET_ERR_RANGE_TOPIC;
    }
    if (!it->second) return comm::RetCode::RET_ERR_RANGE_TOPIC;
    it->second->Load();
    lock_config = it->second;
    return comm::RetCode::RET_OK;
}

uint64_t GlobalConfig::GetLastModTime(const int topic_id) {
    comm::RetCode ret;
    uint64_t res = 0, tmp;

    shared_ptr<const TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = GetTopicConfigByTopicID(topic_id, topic_config))) return 0;
    res = topic_config->GetLastModTime();

    shared_ptr<const ConsumerConfig> consumer_config;
    if (comm::RetCode::RET_OK != (ret = GetConsumerConfig(topic_id, consumer_config))) return 0;
    tmp = consumer_config->GetLastModTime();
    if (res < tmp) res = tmp;

    shared_ptr<const StoreConfig> store_config;
    if (comm::RetCode::RET_OK != (ret = GetStoreConfig(topic_id, store_config))) return 0;
    tmp = store_config->GetLastModTime();
    if (res < tmp) res = tmp;

    shared_ptr<const SchedulerConfig> scheduler_config;
    if (comm::RetCode::RET_OK != (ret = GetSchedulerConfig(topic_id, scheduler_config))) return 0;
    tmp = scheduler_config->GetLastModTime();
    if (res < tmp) res = tmp;

    shared_ptr<const LockConfig> lock_config;
    if (comm::RetCode::RET_OK != (ret = GetLockConfig(topic_id, lock_config))) return 0;
    tmp = lock_config->GetLastModTime();
    if (res < tmp) res = tmp;


    return res;
}


}  // namesapce config

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

