/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/test/test_config.h"

#include <iostream>
#include <memory>


namespace phxqueue {

namespace test {


using namespace std;

void TestConfig::TestGlobalConfig(config::GlobalConfig *global_config) {
    const int topic_id{1000};

    shared_ptr<const config::TopicConfig> topic_config;
    assert(comm::RetCode::RET_OK == global_config->GetTopicConfigByTopicID(topic_id, topic_config));
    TestTopicConfig(topic_config.get());

    shared_ptr<const config::ConsumerConfig> consumer_config;
    assert(comm::RetCode::RET_OK == global_config->GetConsumerConfig(topic_id, consumer_config));
    TestConsumerConfig(consumer_config.get());

    shared_ptr<const config::StoreConfig> store_config;
    assert(comm::RetCode::RET_OK == global_config->GetStoreConfig(topic_id, store_config));
    TestStoreConfig(store_config.get());

    shared_ptr<const config::SchedulerConfig> scheduler_config;
    assert(comm::RetCode::RET_OK == global_config->GetSchedulerConfig(topic_id, scheduler_config));
    TestSchedulerConfig(scheduler_config.get());

    shared_ptr<const config::LockConfig> lock_config;
    assert(comm::RetCode::RET_OK == global_config->GetLockConfig(topic_id, lock_config));
    TestLockConfig(lock_config.get());
}

void TestConfig::TestTopicConfig(const config::TopicConfig *topic_config) {
    // 1->1, 2
    // 2->2
    const vector<int> expected_pub_ids = {1, 2};
    const vector<int> expected_consumer_group_ids = {1, 2};

    /*************************** pub ****************************/
    // GetAllPub
    {
        vector<shared_ptr<const config::proto::Pub> > pubs;
        assert(comm::RetCode::RET_OK == topic_config->GetAllPub(pubs));
        assert(expected_pub_ids.size() == pubs.size());
    }

    // GetAllPubID
    {
        set<int> pub_ids;
        assert(comm::RetCode::RET_OK == topic_config->GetAllPubID(pub_ids));
        assert(expected_pub_ids.size() == pub_ids.size());
    }


    // IsValidPubID
    {
        for (auto &&pub_id : expected_pub_ids) {
            assert(topic_config->IsValidPubID(pub_id));
        }
    }

    // GetPubByPubID
    {
        shared_ptr<const config::proto::Pub> pub;
        assert(comm::RetCode::RET_OK == topic_config->GetPubByPubID(expected_pub_ids[0], pub));
        assert(pub);
        assert(expected_pub_ids[0] == pub->pub_id());
    }

    // GetConsumerGroupIDsByPubID
    {
        set<int> consumer_group_ids;
        assert(comm::RetCode::RET_OK ==
               topic_config->GetConsumerGroupIDsByPubID(expected_pub_ids[0], consumer_group_ids)); // 1->1,2
        assert(expected_consumer_group_ids.size() == consumer_group_ids.size());
    }

    /*************************** consumer_group ****************************/

    // GetAllConsumerGroup
    {
        vector<shared_ptr<const config::proto::ConsumerGroup> > consumer_groups;
        assert(comm::RetCode::RET_OK == topic_config->GetAllConsumerGroup(consumer_groups));
        assert(expected_consumer_group_ids.size() == consumer_groups.size());
    }

    // GetAllConsumerGroupID
    {
        set<int> consumer_group_ids;
        assert(comm::RetCode::RET_OK == topic_config->GetAllConsumerGroupID(consumer_group_ids));
        assert(expected_consumer_group_ids.size() == consumer_group_ids.size());
    }


    // IsValidConsumerGroupID
    {
        for (auto &&consumer_group_id : expected_consumer_group_ids) {
            assert(topic_config->IsValidConsumerGroupID(consumer_group_id));
        }
    }

    // GetConsumerGroupByConsumerGroupID
    {
        shared_ptr<const config::proto::ConsumerGroup> consumer_group;
        assert(comm::RetCode::RET_OK == topic_config->GetConsumerGroupByConsumerGroupID(expected_consumer_group_ids[0], consumer_group));
        assert(consumer_group);
        assert(expected_consumer_group_ids[0] == consumer_group->consumer_group_id());
    }

    /*************************** queue_info ****************************/


	/*
    // GetQueueInfoIDRankByPub
    {
        const int queue_info_id{1};
        const uint64_t expected_rank{0};

        shared_ptr<const config::proto::Pub> pub;

        assert(comm::RetCode::RET_OK == topic_config->GetPubByPubID(expected_pub_ids[0], pub));
        assert(pub);
        assert(expected_pub_ids[0] == pub->pub_id());

        uint64_t rank;

        assert(comm::RetCode::RET_OK ==
               topic_config->GetQueueInfoIDRankByPubID(queue_info_id, pub->pub_id(), rank));
        assert(rank == expected_rank);
    }
	*/

    // GetQueueInfoIDRankByPubID
    {
        const int queue_info_id{1};
        const uint64_t expected_rank{0};

        uint64_t rank;

        assert(comm::RetCode::RET_OK ==
               topic_config->GetQueueInfoIDRankByPubID(queue_info_id, expected_pub_ids[0], rank));
        assert(rank == expected_rank);
    }


    // IsValidQueue
    {
        const int queue{3};
        const int pub_id{1};
        assert(topic_config->IsValidQueue(queue, pub_id));
    }

    // GetQueuesByQueueInfoID
    {
        const int queue_info_id{1};
        const int expected_queues_size{10};
        std::set<int> queues;

        assert(comm::RetCode::RET_OK ==
               topic_config->GetQueuesByQueueInfoID(queue_info_id, queues));
        assert(expected_queues_size == queues.size());
    }

    // GetQueueDelay
    {
        const int queue{1000};
        const int expected_delay{10};

        int delay;

        assert(comm::RetCode::RET_OK == topic_config->GetQueueDelay(queue, delay));
        assert(delay == expected_delay);
    }

    // GetNQueue
    {
        const int queue_info_id{1};
        const int expected_nqueue{10};

        int nr_queue;

        assert(comm::RetCode::RET_OK == topic_config->GetNQueue(queue_info_id, nr_queue));
        assert(nr_queue == expected_nqueue);
    }

    // GetQueueByRank
    {
        const int queue_info_id{1};
        const int rank{2};
        const int expected_queue{2};

        int queue;

        assert(comm::RetCode::RET_OK == topic_config->GetQueueByRank(queue_info_id, rank, queue));
        assert(expected_queue == queue);
    }

    // GetQueueInfoByQueue
    {
        const int queue{3};

        shared_ptr<const config::proto::QueueInfo> queue_info;
        assert(comm::RetCode::RET_OK == topic_config->GetQueueInfoByQueue(queue, queue_info));
        assert(queue_info);
    }

    // GetQueueInfoByQueueInfoID
    {
        const int queue_info_id{1};

        shared_ptr<const config::proto::QueueInfo> queue_info;
        assert(comm::RetCode::RET_OK ==
               topic_config->GetQueueInfoByQueueInfoID(queue_info_id, queue_info));
        assert(queue_info);
    }

    // ShouldSkip
    {
        comm::proto::QItem item;
        assert(!topic_config->ItemShouldSkip(item, 1, 1));
    }

    // GetQueueInfoIDByCount
    {
        const int retry_pub_id{1};
        const int expected_queue_info_id{1};
        const int expected_retry_queue_info_id{3};

        int queue_info_id;

        assert(comm::RetCode::RET_OK ==
               topic_config->GetQueueInfoIDByCount(retry_pub_id, 0, queue_info_id));
        assert(expected_queue_info_id == queue_info_id);
        assert(comm::RetCode::RET_OK ==
               topic_config->GetQueueInfoIDByCount(retry_pub_id, 1, queue_info_id));
        assert(expected_retry_queue_info_id == queue_info_id);
    }
}

void TestConfig::TestStoreConfig(const config::StoreConfig *store_config) {
    const int expected_store_id{1};
    comm::proto::Addr expected_addr;
    expected_addr.set_ip("127.0.0.1");
    expected_addr.set_port(5100);
    expected_addr.set_paxos_port(5101);

    // GetAllStore
    {
        vector<shared_ptr<const config::proto::Store> > stores;
        const int expected_nr_store{1};

        assert(comm::RetCode::RET_OK == store_config->GetAllStore(stores));
        assert(expected_nr_store == stores.size());
    }

    // GetAllStoreID
    {
        set<int> store_ids;
        const int expected_nr_store{1};

        assert(comm::RetCode::RET_OK == store_config->GetAllStoreID(store_ids));
        assert(expected_nr_store == store_ids.size());
    }

    // GetStoreByStoreID
    {

        shared_ptr<const config::proto::Store> store;
        assert(comm::RetCode::RET_OK == store_config->GetStoreByStoreID(expected_store_id, store));
        assert(store);
    }

    // GetStoreIDByAddr
    {
        int store_id;
        assert(comm::RetCode::RET_OK == store_config->GetStoreIDByAddr(expected_addr, store_id));
        assert(expected_store_id == store_id);
    }

    // GetStoreByAddr
    {
        shared_ptr<const config::proto::Store> store;
        assert(comm::RetCode::RET_OK == store_config->GetStoreByAddr(expected_addr, store));
        assert(store);
    }
}


void TestConfig::TestConsumerConfig(const config::ConsumerConfig *consumer_config) {
    comm::proto::Addr expected_addr;
    expected_addr.set_ip("127.0.0.1");
    expected_addr.set_port(8001);
    expected_addr.set_paxos_port(0);

    // GetAllConsumer
    {
        vector<shared_ptr<const config::proto::Consumer> > consumers;
        const int expected_nr_consumer{3};

        assert(comm::RetCode::RET_OK == consumer_config->GetAllConsumer(consumers));
        assert(expected_nr_consumer == consumers.size());
    }

    // GetConsumerByAddr
    {
        shared_ptr<const config::proto::Consumer> consumer;
        assert(comm::RetCode::RET_OK ==
               consumer_config->GetConsumerByAddr(expected_addr, consumer));
        assert(consumer);
    }
}

void TestConfig::TestSchedulerConfig(const config::SchedulerConfig *scheduler_config) {
    comm::proto::Addr expected_addr;
    expected_addr.set_ip("127.0.0.1");
    expected_addr.set_port(6100);
    expected_addr.set_paxos_port(0);

    // GetAllScheduler
    {
        shared_ptr<const config::proto::Scheduler> scheduler;
        assert(comm::RetCode::RET_OK == scheduler_config->GetScheduler(scheduler));
    }
}


void TestConfig::TestLockConfig(const config::LockConfig *lock_config) {
    const int expected_lock_id{1};
    comm::proto::Addr expected_addr;
    expected_addr.set_ip("127.0.0.1");
    expected_addr.set_port(7100);
    expected_addr.set_paxos_port(7101);

    // GetAllLock
    {
        vector<shared_ptr<const config::proto::Lock> > locks;
        const int expected_nr_lock{1};

        assert(comm::RetCode::RET_OK == lock_config->GetAllLock(locks));
        assert(expected_nr_lock == locks.size());
    }

    // GetAllLockID
    {
        set<int> lock_ids;
        const int expected_nr_lock{1};

        assert(comm::RetCode::RET_OK == lock_config->GetAllLockID(lock_ids));
        assert(expected_nr_lock == lock_ids.size());
    }

    // GetLockByLockID
    {

        shared_ptr<const config::proto::Lock> lock;
        assert(comm::RetCode::RET_OK == lock_config->GetLockByLockID(expected_lock_id, lock));
        assert(lock);
    }

    // GetLockIDByAddr
    {
        int lock_id;
        assert(comm::RetCode::RET_OK == lock_config->GetLockIDByAddr(expected_addr, lock_id));
        assert(expected_lock_id == lock_id);
    }

    // GetLockByAddr
    {
        shared_ptr<const config::proto::Lock> lock;
        assert(comm::RetCode::RET_OK == lock_config->GetLockByAddr(expected_addr, lock));
        assert(lock);
    }
}


}  // namespace test

}  // namespace phxqueue

