/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/comm.h"
#include "phxqueue/test/check_config.h"

#include <iostream>
#include <memory>
#include <vector>
#include <string>


namespace phxqueue {

namespace test {


using namespace std;




void CheckConfig::Process() {
    auto global_config = config::GlobalConfig::GetThreadInstance();

    CheckGlobalConfig(global_config);

    std::set<int> topic_ids;
    PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(global_config->GetAllTopicID(topic_ids)));

    for (auto &&topic_id : topic_ids) {
        QLInfo("topic_id %d checking...", topic_id);

        shared_ptr<const config::TopicConfig> topic_config;
        PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(global_config->GetTopicConfigByTopicID(topic_id, topic_config)));
        CheckTopicConfig(topic_id, topic_config.get());

        shared_ptr<const config::ConsumerConfig> consumer_config;
        PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(global_config->GetConsumerConfig(topic_id, consumer_config)));
        CheckConsumerConfig(topic_id, consumer_config.get());

        shared_ptr<const config::StoreConfig> store_config;
        PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(global_config->GetStoreConfig(topic_id, store_config)));
        CheckStoreConfig(topic_id, store_config.get());

        shared_ptr<const config::SchedulerConfig> scheduler_config;
        PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(global_config->GetSchedulerConfig(topic_id, scheduler_config)));
        CheckSchedulerConfig(topic_id, scheduler_config.get());

        shared_ptr<const config::LockConfig> lock_config;
        PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(global_config->GetLockConfig(topic_id, lock_config)));
        CheckLockConfig(topic_id, lock_config.get());

        QLInfo("topic_id %d done", topic_id);
    }
}

void CheckConfig::CheckGlobalConfig(config::GlobalConfig *global_config) {
}

void CheckConfig::CheckTopicConfig(const int topic_id, const config::TopicConfig *topic_config) {

    auto &&topic = topic_config->GetProto().topic();
    PHX_ASSERT(topic_id, ==, topic.topic_id());
    for (int i{0}; i < topic.handle_ids_size(); ++i) {
        auto handle_id = topic.handle_ids(i);
        int rank;
        PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetHandleIDRank(handle_id, rank)));
        PHX_ASSERT(rank, ==, i);
    }

	// sub
    std::vector<std::shared_ptr<const config::proto::Sub>> subs;
    PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetAllSub(subs)));
    std::set<int> sub_ids;
    PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetAllSubID(sub_ids)));
    PHX_ASSERT(subs.size(), ==, sub_ids.size());

    // pub
    std::vector<std::shared_ptr<const config::proto::Pub>> pubs;
    PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetAllPub(pubs)));

    std::set<int> pub_ids;
    PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetAllPubID(pub_ids)));
    PHX_ASSERT(pubs.size(), ==, pub_ids.size());
    for (auto &&pub : pubs) {
        auto pub_id = pub->pub_id();
        PHX_ASSERT(pub_ids.end() != pub_ids.find(pub_id), ==, true);
        PHX_ASSERT(topic_config->IsValidPubID(pub_id), ==, true);

        {
            std::shared_ptr<const config::proto::Pub> tmp_pub;
            PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetPubByPubID(pub_id, tmp_pub)));
            PHX_ASSERT(tmp_pub->pub_id(), ==, pub_id);
        }

        {
            std::set<int> consumer_group_ids;
            PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetConsumerGroupIDsByPubID(pub_id, consumer_group_ids)));
            if (pub->consumer_group_ids_size()) PHX_ASSERT(pub->consumer_group_ids_size(), ==, consumer_group_ids.size());
            for (int i{0}; i < pub->consumer_group_ids_size(); ++i) {
                auto consumer_group_id = pub->consumer_group_ids(i);
                PHX_ASSERT(consumer_group_ids.end() != consumer_group_ids.find(consumer_group_id), ==, true);
            }
        }

		{
            std::set<int> pub_sub_ids;
            PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetSubIDsByPubID(pub_id, pub_sub_ids)));
            PHX_ASSERT(pub->sub_ids_size(), ==, pub_sub_ids.size());
            for (int i{0}; i < pub->sub_ids_size(); ++i) {
                auto sub_id = pub->sub_ids(i);
                PHX_ASSERT(pub_sub_ids.end() != pub_sub_ids.find(sub_id), ==, true);
                PHX_ASSERT(sub_ids.end() != sub_ids.find(sub_id), ==, true);
            }
        }

		{
			if (pub->is_transaction()) {
                PHX_ASSERT(pub->tx_query_sub_id() != 0, ==, true);
                PHX_ASSERT(sub_ids.end() != sub_ids.find(pub->tx_query_sub_id()), ==, true);
			}
		}
    }

    // consumer_group
    std::vector<std::shared_ptr<const config::proto::ConsumerGroup>> consumer_groups;
    PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetAllConsumerGroup(consumer_groups)));

    std::set<int> consumer_group_ids;
    PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetAllConsumerGroupID(consumer_group_ids)));
    PHX_ASSERT(consumer_groups.size(), ==, consumer_group_ids.size());
    for (auto &&consumer_group : consumer_groups) {
        auto consumer_group_id = consumer_group->consumer_group_id();
        PHX_ASSERT(consumer_group_ids.end() != consumer_group_ids.find(consumer_group_id), ==, true);
        PHX_ASSERT(topic_config->IsValidConsumerGroupID(consumer_group_id), ==, true);

        {
            std::shared_ptr<const config::proto::ConsumerGroup> tmp_consumer_group;
            PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetConsumerGroupByConsumerGroupID(consumer_group_id, tmp_consumer_group)));
            PHX_ASSERT(tmp_consumer_group->consumer_group_id(), ==, consumer_group_id);
        }
    }

    // queue_info
    for (int i{0}; i < pubs.size(); ++i) {
        auto &&pub = pubs[i];
        auto pub_id = pub->pub_id();

		// normal queue
        int cnt = 0;
        for (int j{0}; j < pub->queue_info_ids_size(); ++j) {
            auto queue_info_id = pub->queue_info_ids(j);

            std::shared_ptr<const config::proto::QueueInfo> queue_info;
            PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetQueueInfoByQueueInfoID(queue_info_id, queue_info)));
            PHX_ASSERT(queue_info_id, ==, queue_info->queue_info_id());

            PHX_ASSERT(queue_info->freq_limit(), >=, 0);
            PHX_ASSERT(queue_info->freq_interval(), >=, 0);
            PHX_ASSERT(queue_info->sleep_us_per_get(), >=, 0);
            PHX_ASSERT(queue_info->sleep_us_on_get_fail(), >=, 0);
            PHX_ASSERT(queue_info->sleep_us_on_get_no_item(), >=, 0);
            PHX_ASSERT(queue_info->sleep_us_on_get_size_too_small(), >=, 0);
            PHX_ASSERT(queue_info->get_size_too_small_threshold(), >=, 0);
            PHX_ASSERT(queue_info->delay(), >=, 0);
            PHX_ASSERT(queue_info->count(), >=, -1);

            {
                int tmp_queue_info_id;
                PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetQueueInfoIDByCount(pub_id, cnt, tmp_queue_info_id, comm::proto::QueueType::NORMAL_QUEUE)));
                PHX_ASSERT(tmp_queue_info_id, ==, queue_info_id);
                cnt += queue_info->count();
            }

            {
                uint64_t rank;
                PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetQueueInfoIDRankByPubID(queue_info_id, pub_id, rank, comm::proto::QueueType::NORMAL_QUEUE)));
                PHX_ASSERT(j, ==, rank);
            }

            std::set<int> queues;
            PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetQueuesByQueueInfoID(queue_info_id, queues)));
            int nqueue;
            PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetNQueue(queue_info_id, nqueue)));
            PHX_ASSERT(nqueue, ==, queues.size());
            for (auto &&queue : queues) {
                PHX_ASSERT(queue, >=, 0);
                PHX_ASSERT(topic_config->IsValidQueue(queue), ==, true);
                PHX_ASSERT(topic_config->IsValidQueue(queue, pub_id), ==, true);
                for (int l{0}; l < pub->consumer_group_ids_size(); ++l) {
                    auto consumer_group_id = pub->consumer_group_ids(l);
                    PHX_ASSERT(topic_config->IsValidQueue(queue, pub_id, consumer_group_id), ==, true);
                }

                {
                    int delay;
                    PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetQueueDelay(queue, delay)));
                }

                {
                    std::shared_ptr<const config::proto::QueueInfo> tmp_queue_info;
                    PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetQueueInfoByQueue(queue, tmp_queue_info)));
                    PHX_ASSERT(queue_info_id, ==, tmp_queue_info->queue_info_id());
                }
            }
            if (nqueue > 0) {
                int queue;
                PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetQueueByRank(queue_info_id, 0, queue)));
                PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetQueueByLoopRank(queue_info_id, 0, queue)));
            }
        }

		// tx query queue
		cnt = 0;
		for (int j{0}; j < pub->tx_query_queue_info_ids_size(); ++j) {
            auto queue_info_id = pub->tx_query_queue_info_ids(j);

            std::shared_ptr<const config::proto::QueueInfo> queue_info;
            PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetQueueInfoByQueueInfoID(queue_info_id, queue_info)));
            PHX_ASSERT(queue_info_id, ==, queue_info->queue_info_id());

            PHX_ASSERT(queue_info->freq_limit(), >=, 0);
            PHX_ASSERT(queue_info->freq_interval(), >=, 0);
            PHX_ASSERT(queue_info->sleep_us_per_get(), >=, 0);
            PHX_ASSERT(queue_info->sleep_us_on_get_fail(), >=, 0);
            PHX_ASSERT(queue_info->sleep_us_on_get_no_item(), >=, 0);
            PHX_ASSERT(queue_info->sleep_us_on_get_size_too_small(), >=, 0);
            PHX_ASSERT(queue_info->get_size_too_small_threshold(), >=, 0);
            PHX_ASSERT(queue_info->delay(), >=, 0);
            PHX_ASSERT(queue_info->count(), >=, -1);

            {
                int tmp_queue_info_id;
                PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetQueueInfoIDByCount(pub_id, cnt, tmp_queue_info_id, comm::proto::QueueType::TX_QUERY_QUEUE)));
                PHX_ASSERT(tmp_queue_info_id, ==, queue_info_id);
                cnt += queue_info->count();
            }

            {
                uint64_t rank;
                PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetQueueInfoIDRankByPubID(queue_info_id, pub_id, rank, comm::proto::QueueType::TX_QUERY_QUEUE)));
                PHX_ASSERT(j, ==, rank);
            }

            std::set<int> queues;
            PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetQueuesByQueueInfoID(queue_info_id, queues)));
            int nqueue;
            PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetNQueue(queue_info_id, nqueue)));
            PHX_ASSERT(nqueue, ==, queues.size());
            for (auto &&queue : queues) {
                PHX_ASSERT(queue, >=, 0);
                PHX_ASSERT(topic_config->IsValidQueue(queue), ==, true);
                PHX_ASSERT(topic_config->IsValidQueue(queue, pub_id), ==, true);
                for (int l{0}; l < pub->consumer_group_ids_size(); ++l) {
                    auto consumer_group_id = pub->consumer_group_ids(l);
                    PHX_ASSERT(topic_config->IsValidQueue(queue, pub_id, consumer_group_id), ==, true);
                }

                {
                    int delay;
                    PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetQueueDelay(queue, delay)));
                }

                {
                    std::shared_ptr<const config::proto::QueueInfo> tmp_queue_info;
                    PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetQueueInfoByQueue(queue, tmp_queue_info)));
                    PHX_ASSERT(queue_info_id, ==, tmp_queue_info->queue_info_id());
                }
            }
            if (nqueue > 0) {
                int queue;
                PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetQueueByRank(queue_info_id, 0, queue)));
                PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetQueueByLoopRank(queue_info_id, 0, queue)));
            }
        }

    }

    // freq_info
    {
        std::vector<std::shared_ptr<config::proto::FreqInfo> > freq_infos;
        PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetAllFreqInfo(freq_infos)));
    }

    // replay_info
    {
        std::vector<std::unique_ptr<config::proto::ReplayInfo> > replay_infos;
        PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(topic_config->GetAllReplayInfo(replay_infos)));
    }
}


void CheckConfig::CheckConsumerConfig(const int topic_id, const config::ConsumerConfig *consumer_config) {

    std::vector<std::shared_ptr<const config::proto::Consumer>> consumers;

    PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(consumer_config->GetAllConsumer(consumers)));

    for (auto &&consumer : consumers) {
        std::shared_ptr<const config::proto::Consumer> tmp_consumer;
        PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(consumer_config->GetConsumerByAddr(consumer->addr(), tmp_consumer)));
    }
}

void CheckConfig::CheckStoreConfig(const int topic_id, const config::StoreConfig *store_config) {

    std::vector<std::shared_ptr<const config::proto::Store>> stores;
    PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(store_config->GetAllStore(stores)));

    std::set<int> store_ids;
    PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(store_config->GetAllStoreID(store_ids)));
    PHX_ASSERT(store_ids.size(), ==, stores.size());

    for (auto &&store : stores) {
        auto store_id = store->store_id();

        PHX_ASSERT(store_ids.end() != store_ids.find(store_id), ==, true);

        {
            std::shared_ptr<const config::proto::Store> tmp_store;
            PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(store_config->GetStoreByStoreID(store_id, tmp_store)));
            PHX_ASSERT(store_id, ==, tmp_store->store_id());
        }

        for (int i{0}; i < store->addrs_size(); ++i) {
            auto &&addr = store->addrs(i);

            {
                int tmp_store_id;
                PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(store_config->GetStoreIDByAddr(addr, tmp_store_id)));
                PHX_ASSERT(tmp_store_id, ==, store_id);
            }
            {
                std::shared_ptr<const config::proto::Store> tmp_store;
                PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(store_config->GetStoreByAddr(addr, tmp_store)));
                PHX_ASSERT(tmp_store->store_id(), ==, store_id);
            }
        }
    }
}


void CheckConfig::CheckSchedulerConfig(const int topic_id, const config::SchedulerConfig *scheduler_config) {
}


void CheckConfig::CheckLockConfig(const int topic_id, const config::LockConfig *lock_config) {

    std::vector<std::shared_ptr<const config::proto::Lock> > locks;
    PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(lock_config->GetAllLock(locks)));

    std::set<int> lock_ids;
    PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(lock_config->GetAllLockID(lock_ids)));
    PHX_ASSERT(locks.size(), ==, lock_ids.size());

    for (auto &&lock : locks) {
        auto lock_id = lock->lock_id();

        PHX_ASSERT(lock_ids.end() != lock_ids.find(lock_id), ==, true);

        {
            std::shared_ptr<const config::proto::Lock> tmp_lock;
            PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(lock_config->GetLockByLockID(lock_id, tmp_lock)));
            PHX_ASSERT(lock_id, ==, tmp_lock->lock_id());
        }

        for (int i{0}; i < lock->addrs_size(); ++i) {
            auto &&addr = lock->addrs(i);

            {
                int tmp_lock_id;
                PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(lock_config->GetLockIDByAddr(addr, tmp_lock_id)));
                PHX_ASSERT(tmp_lock_id, ==, lock_id);
            }
            {
                std::shared_ptr<const config::proto::Lock> tmp_lock;
                PHX_ASSERT(phxqueue::comm::as_integer(comm::RetCode::RET_OK), ==, phxqueue::comm::as_integer(lock_config->GetLockByAddr(addr, tmp_lock)));
                PHX_ASSERT(tmp_lock->lock_id(), ==, lock_id);
            }
        }
    }
}


}  // namespace test

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

