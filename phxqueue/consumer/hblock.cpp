/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/consumer/hblock.h"

#include <algorithm>
#include <cinttypes>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <pthread.h>
#include <set>
#include <signal.h>
#include <sstream>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <zlib.h>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"
#include "phxqueue/scheduler.h"
#include "phxqueue/lock.h"
#include "phxqueue/plugin.h"

#include "phxqueue/consumer/consumer.h"


#define LOCK_BUF_MAGIC 890725
#define LOCK_ITEM_MAGIC 889735


namespace phxqueue {

namespace consumer {


using namespace std;


class HeartBeatLock::HeartBeatLockImpl {
  public:
    HeartBeatLockImpl() {}
    virtual ~HeartBeatLockImpl() {}

    int nproc{0};
    QueueBuf_t *buf{nullptr};
    pthread_rwlock_t *rwlock{nullptr};

    // 以下两者决定是否重新更新权重
    uint64_t conf_last_mod_time{0};  // consumer client配置上次更新时间
    map<int, uint64_t> scale_hashs;  // sched 返回的scale的hash值

    // 分配队列数占进程数
    int proc_used{0};

    Consumer *consumer{nullptr};
};

HeartBeatLock::HeartBeatLock() : impl_(new HeartBeatLockImpl()){
    assert(impl_);
}


HeartBeatLock::~HeartBeatLock() {
    if (impl_->buf)
    {
        shmdt((void *)impl_->buf);
    }
}

comm::RetCode HeartBeatLock::Init(Consumer *consumer, const int shmkey, const string &lockpath, const int nproc) {
    comm::RetCode ret;

    impl_->consumer = consumer;
    impl_->nproc = nproc;

    const int topic_id = impl_->consumer->GetTopicID();

    // init QueueInfo
    {
        void *shm_addr{nullptr};
        size_t shm_size = sizeof(QueueBuf_t) + sizeof(Queue_t) * nproc;
        int shm_id = shmget(shmkey, shm_size, 0666);
        if (shm_id < 0) {
            /* create */
            shm_id = shmget(shmkey, shm_size, 0666 | IPC_CREAT);

            if (shm_id < 0) {

                /* remove old */

                if ((shm_id = shmget(shmkey, 0, 0666)) < 0) {
                    QLErr("ERR: shmget ret %d %s", shm_id, strerror(errno));
                    return comm::RetCode::RET_ERR_SYS;
                }
                if (shmctl(shm_id, IPC_RMID, NULL) < 0) {
                    QLErr("ERR: shmctl %s", strerror(errno));
                    return comm::RetCode::RET_ERR_SYS;
                }

                /* recreate */
                if ((shm_id = shmget(shmkey, shm_size, 0666 | IPC_CREAT)) < 0) {
                    QLErr("ERR: shmget ret %d %s", shm_id, strerror(errno));
                    return comm::RetCode::RET_ERR_SYS;
                }
            }

            shm_addr = shmat(shm_id, nullptr, 0);
            if (shm_addr == (void *)-1) {
                QLErr("ERR: shmat %s", strerror(errno));
                return comm::RetCode::RET_ERR_SYS;
            }

            memset(shm_addr, 0, shm_size);
        } else {
            /* exists */
            shm_addr = shmat(shm_id, NULL, 0);
            if (shm_addr == (void *)-1) {
                QLErr("ERR: shmat %s", strerror(errno));
                return comm::RetCode::RET_ERR_SYS;
            }
        }
        impl_->buf = (QueueBuf_t *)shm_addr;
    }

    // init multi-process rwlock
    {
        pthread_rwlockattr_t attr;

        if (0 != pthread_rwlockattr_init(&attr)) {
            QLErr("ERR: pthread_rwlockattr_init %s", strerror(errno));
            return comm::RetCode::RET_ERR_SYS;
        }
        if (0 != pthread_rwlockattr_setpshared(&attr, PTHREAD_PROCESS_SHARED)) {
            QLErr("ERR: pthread_rwlockattr_setpshared %s", strerror(errno));
            return comm::RetCode::RET_ERR_SYS;
        }

        int shm_id = shm_open(lockpath.c_str(), O_RDWR|O_CREAT, 0666);
        if (shm_id < 0) {
            QLErr("ERR: shm_open(%s) %s", lockpath.c_str(), strerror(errno));
            return comm::RetCode::RET_ERR_SYS;
        }
        ftruncate(shm_id, sizeof(pthread_rwlock_t));

        impl_->rwlock = (pthread_rwlock_t *)mmap(NULL, sizeof(pthread_rwlock_t),
                                                 PROT_READ|PROT_WRITE, MAP_SHARED, shm_id, 0);
        if (MAP_FAILED == impl_->rwlock) {
            QLErr("ERR: mmap failed %s", strerror(errno));
            return comm::RetCode::RET_ERR_SYS;
        }

        if (0 != pthread_rwlock_init(impl_->rwlock, &attr)) {
            QLErr("ERR: pthread_rwlock_init %s", strerror(errno));
            return comm::RetCode::RET_ERR_SYS;
        }
    }

    // init oss
    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("ERR: GetTopicConfigByTopicID ret %d topic_id %u", ret, topic_id);
        return ret;
    }

    return comm::RetCode::RET_OK;
}

void HeartBeatLock::ClearInvalidConsumerGroupIDs(const set<int> &valid_consumer_group_ids) {
    comm::utils::RWLock l(impl_->rwlock, comm::utils::RWLock::LockMode::WRITE);

    for (int vpid{0}; vpid < impl_->nproc; ++vpid) {
        Queue_t *queue{&impl_->buf->queues[vpid]};
        if (LOCK_ITEM_MAGIC == queue->magic) {
            if (valid_consumer_group_ids.end() == valid_consumer_group_ids.find(queue->consumer_group_id)) {
                QLInfo("QUEUEINFO: vpid %d clear consumer_group %u store %u queue %u", vpid, queue->consumer_group_id, queue->store_id, queue->queue_id);
                memset(queue, 0, sizeof(Queue_t));
            }
        }
    }
}

void HeartBeatLock::DistubePendingQueues(const map<int, vector<Queue_t>> &consumer_group_id2pending_queues) {
    //if (impl_->ossid) OssAttrInc(impl_->ossid, 20, 1);

    comm::utils::RWLock l(impl_->rwlock, comm::utils::RWLock::LockMode::WRITE);

    for (auto &&kv : consumer_group_id2pending_queues) {
        auto &&consumer_group_id = kv.first;
        auto &&pending_queues = kv.second;

        vector<bool> done;
        done.resize(pending_queues.size(), false);

        size_t idx;

        // if there is a proc handling a queue in pending_queues, keep doing that. prevent from lock switching.
        for (int vpid{0}; vpid < impl_->nproc; ++vpid) {
            Queue_t *queue{&impl_->buf->queues[vpid]};

            if (LOCK_ITEM_MAGIC == queue->magic && consumer_group_id == queue->consumer_group_id) {
                for (idx = 0; idx < pending_queues.size(); ++idx) {
                    auto &&pending_queue = pending_queues[idx];
                    if (queue->consumer_group_id == pending_queue.consumer_group_id &&
                        queue->store_id == pending_queue.store_id &&
                        queue->queue_id == pending_queue.queue_id) {

                        if (!done[idx]) {
                            done[idx] = true;
                            QLInfo("QUEUEINFO: vpid %d keep consumer_group %u store %u queue %u", vpid, queue->consumer_group_id, queue->store_id, queue->queue_id);
                        } else {
                            memset(queue, 0, sizeof(Queue_t));
                        }

                        break;
                    }
                }
                if (pending_queues.size() == idx) {
                    memset(queue, 0, sizeof(Queue_t));
                }
            }
        }

        // the left nproc handle the left queues in pending_queues
        for (int vpid{0}; vpid < impl_->nproc; ++vpid) {
            Queue_t *queue{&impl_->buf->queues[vpid]};

            if (LOCK_ITEM_MAGIC != queue->magic) {
                idx = 0;
                for (; idx < pending_queues.size(); ++idx) {
                    if (!done[idx]) break;
                }
                if (idx == pending_queues.size()) {
                    queue->magic = 0;
                } else {
                    auto &&pending_queue = pending_queues[idx];

                    queue->magic = LOCK_ITEM_MAGIC;
                    queue->consumer_group_id = pending_queue.consumer_group_id;
                    queue->store_id = pending_queue.store_id;
                    queue->queue_id = pending_queue.queue_id;

                    done[idx] = true;
                    QLInfo("QUEUEINFO: vpid %d new consumer_group %u store %u queue %u", vpid, queue->consumer_group_id, queue->store_id, queue->queue_id);
                }
            }
        }
    }

    for (int vpid{0}; vpid < impl_->nproc; ++vpid) {
	    Queue_t *queue{&impl_->buf->queues[vpid]};
	    if (LOCK_ITEM_MAGIC == queue->magic) {
		    QLInfo("QUEUEINFO: vpid %d match consumer_group %u store %u queue %u", vpid, queue->consumer_group_id, queue->store_id, queue->queue_id);
	    }
    }


    impl_->buf->magic = LOCK_BUF_MAGIC;


}

void HeartBeatLock::RunSync() {
    comm::RetCode ret;
    pid_t pid = fork();
    if (pid == 0) {

        prctl(PR_SET_PDEATHSIG, SIGHUP);

        impl_->consumer->OnRunSync();

        //oss_call_stat_attach(vpid);
        //mmlb_stat_attach(vpid);

        const int topic_id = impl_->consumer->GetTopicID();

        while (true) {
            if (0 > comm::as_integer(ret = Sync())) {
                QLErr("ERR: Sync fail. ret %d", comm::as_integer(ret));
                comm::ConsumerHeartBeatLockBP::GetThreadInstance()->OnSyncFail(topic_id);
            }

            shared_ptr<const config::TopicConfig> topic_config;
            if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
                QLErr("ERR: GetTopicConfigByTopicID ret %d topic_id %u", ret, topic_id);
                continue;
            }

            sleep(topic_config->GetProto().topic().scheduler_get_scale_interval_s());
        }
        exit(0);
    }
}

static size_t CalHash(const vector<comm::proto::AddrScale> &addr_scales) {
    size_t h = crc32(0, Z_NULL, 0);
    for (auto &&addr_scale : addr_scales) {
        int port = addr_scale.addr().port();
        int paxos_port = addr_scale.addr().paxos_port();
        int scale = addr_scale.scale();
        h = crc32(h, (const unsigned char *)addr_scale.addr().ip().c_str(), addr_scale.addr().ip().length());
        h = crc32(h, (const unsigned char *)&port, sizeof(int));
        h = crc32(h, (const unsigned char *)&paxos_port, sizeof(int));
        h = crc32(h, (const unsigned char *)&scale, sizeof(int));
    }
    return h;
}

comm::RetCode HeartBeatLock::GetAddrScale(ConsumerGroupID2AddrScales &consumer_group_id2addr_scales) {
    QLVerb("start");

    consumer_group_id2addr_scales.clear();

    auto &&opt = impl_->consumer->GetConsumerOption();

    AddrScales config_addr_scales, dynamic_addr_scales;

    comm::RetCode ret;

    auto topic_id = impl_->consumer->GetTopicID();

    shared_ptr<const config::ConsumerConfig> consumer_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetConsumerConfig(topic_id, consumer_config))) {
        QLErr("GetConsumerConfig ret %d", comm::as_integer(ret));
        return ret;
    }

    std::set<int> consumer_group_ids;
    {
        comm::proto::Addr addr;
        addr.set_ip(opt->ip);
        addr.set_port(opt->port);

        if (comm::RetCode::RET_OK != (ret = config::utils::GetConsumerGroupIDsByConsumerAddr(topic_id, addr, consumer_group_ids))) {
            QLErr("GetConsumerGroupIDs ret %d", comm::as_integer(ret));
            return ret;
        }
    }

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("ERR: GetTopicConfigByTopicID ret %d", comm::as_integer(ret));
        return ret;
    }

    std::set<int> dynamic_consumer_group_ids;
    for (auto &&consumer_group_id : consumer_group_ids) {
        shared_ptr<const config::proto::ConsumerGroup> consumer_group;
        if (comm::RetCode::RET_OK != (ret = topic_config->GetConsumerGroupByConsumerGroupID(consumer_group_id, consumer_group))) {
            QLErr("GetConsumerGroupByConsumerGroupID ret %d", comm::as_integer(ret));
            continue;
        }
        if (consumer_group->use_dynamic_scale()) {
            dynamic_consumer_group_ids.insert(consumer_group_id);
        }
    }
    QLVerb("dynamic consumer_group_ids size %d", dynamic_consumer_group_ids.size());


    if (dynamic_consumer_group_ids.size()) {
        comm::proto::GetAddrScaleRequest req;
        comm::proto::GetAddrScaleResponse resp;
        req.set_topic_id(topic_id);
        auto &&addr(req.mutable_addr());
        addr->set_ip(opt->ip);
        addr->set_port(opt->port);
        auto &&load_info = req.mutable_load_info();
        load_info->set_cpu(comm::utils::GetCpu());
        if (impl_->nproc) load_info->set_proc_used_ratio(impl_->proc_used * 100 / impl_->nproc);

        scheduler::SchedulerMasterClient<comm::proto::GetAddrScaleRequest, comm::proto::GetAddrScaleResponse> scheduler_master_client;
        ret = scheduler_master_client.ClientCall(req, resp, bind(&Consumer::GetAddrScale, impl_->consumer, placeholders::_1, placeholders::_2));

        if (comm::RetCode::RET_OK != ret) {
            QLErr("ERR: GetAddrScale ret %d", comm::as_integer(ret));
            if (impl_->conf_last_mod_time) return ret;
            else dynamic_consumer_group_ids.clear();
        } else {
            for (size_t i{0}; i < resp.addr_scales_size(); ++i) {
                dynamic_addr_scales.push_back(resp.addr_scales(i));
                QLInfo("dynamic addr(%s:%d) scale %d", resp.addr_scales(i).addr().ip().c_str(), resp.addr_scales(i).addr().port(), resp.addr_scales(i).scale());
            }
        }
    }

    if (dynamic_consumer_group_ids.size() < consumer_group_ids.size()) {
        vector<shared_ptr<const config::proto::Consumer> > consumers;
        if (comm::RetCode::RET_OK != (ret = consumer_config->GetAllConsumer(consumers))) {
            QLErr("ERR: GetAllConsumer ret %d", comm::as_integer(ret));
            return ret;
        }
        QLVerb("consumers size %d", consumers.size());

        for (auto &&consumer : consumers) {
            comm::proto::AddrScale addr_scale;
            addr_scale.mutable_addr()->CopyFrom(consumer->addr());
            addr_scale.set_scale(consumer->scale());
            config_addr_scales.push_back(addr_scale);
            QLInfo("config addr(%s:%d) scale %d", addr_scale.addr().ip().c_str(), addr_scale.addr().port(), addr_scale.scale());
        }
    }

    // get all addr_scales done

    auto &&f = [&](const int consumer_group_id, const comm::proto::AddrScale &addr_scale)->void {
        std::set<int> consumer_group_ids;
        if (comm::RetCode::RET_OK != (ret = config::utils::GetConsumerGroupIDsByConsumerAddr(topic_id, addr_scale.addr(), consumer_group_ids))) {
            QLErr("GetConsumerGroupIDs ret %d", comm::as_integer(ret));
            return;
        }
        if (consumer_group_ids.end() != consumer_group_ids.find(consumer_group_id)) {
            consumer_group_id2addr_scales[consumer_group_id].push_back(addr_scale);
            QLVerb("consumer_group_id2addr_scales consumer_group_id %d addr(%s:%d:%d) scale %d", consumer_group_id,
                   addr_scale.addr().ip().c_str(), addr_scale.addr().port(), addr_scale.addr().paxos_port(),
                   addr_scale.scale());
        }
    };

    for (auto &&consumer_group_id : consumer_group_ids) {
        if (dynamic_consumer_group_ids.end() == dynamic_consumer_group_ids.find(consumer_group_id)) {
            for (auto &&addr_scale : config_addr_scales) {
                f(consumer_group_id, addr_scale);
            }
        } else {
            for (auto &&addr_scale : dynamic_addr_scales) {
                f(consumer_group_id, addr_scale);
            }
        }
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode HeartBeatLock::GetAllQueues(const int consumer_group_id, vector<Queue_t> &all_queues) {
    all_queues.clear();

    comm::RetCode ret = comm::RetCode::RET_OK;

    auto topic_id = impl_->consumer->GetTopicID();

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("ERR: GetTopicConfigByTopicID err. topic_id %u ret %d", topic_id, comm::as_integer(ret));
        return ret;
    }

    shared_ptr<const config::StoreConfig> store_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetStoreConfig(topic_id, store_config))) {
        QLErr("ERR: GetStoreConfigByPubID err. topic_id %d ret %d", topic_id, comm::as_integer(ret));
        return ret;
    }

    set<int> store_ids;
    if (comm::RetCode::RET_OK != (ret = store_config->GetAllStoreID(store_ids))) {
        QLErr("ERR: GetAllStoreID ret %d", comm::as_integer(ret));
        return ret;
    }

    set<pair<int, int> > filter;
    for (auto &&store_id : store_ids) {
        set<int> pub_ids;
        if (comm::RetCode::RET_OK != (ret = config::utils::GetPubIDsByStoreID(topic_id, store_id, pub_ids))) {
            QLErr("GetPubIDsByStoreID ret %d topic_id %d store_id %d", as_integer(ret), topic_id, store_id);
            continue;
        }

        for (auto &&pub_id : pub_ids) {
            shared_ptr<const config::proto::Pub> pub;
            if (comm::RetCode::RET_OK != (ret = topic_config->GetPubByPubID(pub_id, pub))) {
                QLErr("ERR: GetPubByPubID err. ret %d", comm::as_integer(ret));
                continue;
            }

            // check pub1
			if (pub->is_transaction()) {
				if (pub->tx_query_sub_id() > 0) {
					shared_ptr<const config::proto::Sub> sub;
					if (comm::RetCode::RET_OK != (ret = topic_config->GetSubBySubID(pub->tx_query_sub_id(), sub))) {
						QLErr("ERR: GetSubBySubID err. ret %d", comm::as_integer(ret));
						continue;
					}
					if (sub->consumer_group_id() == consumer_group_id) {
						GetQueuesWithFilter(pub_id, store_id, consumer_group_id, all_queues, &(pub->tx_query_queue_info_ids()), topic_config, filter);
					}
				}

				for (int i{0}; i < pub->sub_ids_size(); ++i) {
					shared_ptr<const config::proto::Sub> sub;
					if (comm::RetCode::RET_OK != (ret = topic_config->GetSubBySubID(pub->sub_ids(i), sub))) {
						QLErr("ERR: GetSubBySubID err. ret %d", comm::as_integer(ret));
						continue;
					}
					if (sub->consumer_group_id() == consumer_group_id) {
						GetQueuesWithFilter(pub_id, store_id, consumer_group_id, all_queues, &(pub->queue_info_ids()), topic_config, filter);
						break;
					}
				}
			}
			else {
				std::set<int> consumer_group_ids;
				if (comm::RetCode::RET_OK != (ret = topic_config->GetConsumerGroupIDsByPubID(pub_id, consumer_group_ids))) {
					QLErr("ERR: GetConsumerGroupIDsByPubID err. ret %d", comm::as_integer(ret));
					continue;
				}
                if (consumer_group_ids.find(consumer_group_id) == consumer_group_ids.end()) continue;
				GetQueuesWithFilter(pub_id, store_id, consumer_group_id, all_queues, &(pub->queue_info_ids()), topic_config, filter);
            }
        }
    }
    return comm::RetCode::RET_OK;
}

void HeartBeatLock::GetQueuesWithFilter(const int pub_id, const int store_id, const int consumer_group_id, std::vector<Queue_t> &all_queues, const ::google::protobuf::RepeatedField< ::google::protobuf::int32 >* queue_info_ids, shared_ptr<const config::TopicConfig> topic_config, set<pair<int, int> > &filter)
{
	if (queue_info_ids == nullptr) return ;
	
	comm::RetCode ret;
	for (int i{0}; i < queue_info_ids->size(); ++i) {
		auto &&queue_info_id = queue_info_ids->Get(i);

		set<int> queue_ids;
		if (comm::RetCode::RET_OK != (ret = topic_config->GetQueuesByQueueInfoID(queue_info_id, queue_ids))) {
			QLErr("GetQueuesByQueueInfoID ret %d queue_info_id %d", comm::as_integer(ret), queue_info_id);
			continue;
		}

		for (auto &&queue_id : queue_ids) {
            if (topic_config->QueueShouldSkip(queue_id, consumer_group_id)) continue;

			auto &&tmp = make_pair(store_id, queue_id);
			if (filter.end() != filter.find(tmp)) continue;
			filter.insert(tmp);

			Queue_t queue;
			queue.magic = 0;
			queue.pub_id = pub_id;
			queue.consumer_group_id = consumer_group_id;
			queue.store_id = store_id;
			queue.queue_id = queue_id;

			QLVerb("add into all_queues. consumer_group_id %d store_id %d queue_id %d", consumer_group_id, store_id, queue_id);

			all_queues.emplace_back(queue);
		}
	}
}

comm::RetCode HeartBeatLock::GetPendingQueues(const vector<Queue_t> &all_queues, const AddrScales &addr_scales, vector<Queue_t> &pending_queues) {
    comm::RetCode ret;
    set<size_t> queue_idxs;
    if (comm::RetCode::RET_OK != (ret = impl_->consumer->GetQueueByAddrScale(all_queues, addr_scales, queue_idxs))) {
        QLErr("ERR: GetQueueByAddrScale ret %d", comm::as_integer(ret));
        return ret;
    }
    for (auto &&idx : queue_idxs) {
        pending_queues.push_back(all_queues[idx]);
        QLVerb("add into pending_queues. idx %d", idx);
    }
    return comm::RetCode::RET_OK;
}

void HeartBeatLock::UpdateProcUsed() {
    int proc_used = 0;
    for (int vpid{0}; vpid < impl_->nproc; ++vpid) {
        Queue_t *queue{&impl_->buf->queues[vpid]};
        if (LOCK_ITEM_MAGIC == queue->magic) {
            ++proc_used;
        }
    }
    impl_->proc_used = proc_used;
}


comm::RetCode HeartBeatLock::Sync() {
    QLInfo("Sync begin");

    comm::RetCode ret;

    const int topic_id = impl_->consumer->GetTopicID();

    UpdateProcUsed();

    comm::ConsumerHeartBeatLockBP::GetThreadInstance()->OnSync(topic_id);
    comm::ConsumerHeartBeatLockBP::GetThreadInstance()->OnProcUsed(topic_id, impl_->nproc, impl_->proc_used);
    if (impl_->proc_used >= impl_->nproc) {
        comm::ConsumerHeartBeatLockBP::GetThreadInstance()->OnProcUsedExceed(topic_id, impl_->nproc, impl_->proc_used);
        QLErr("ERR: impl_->proc_used(%d) >= nproc(%d)", impl_->proc_used, impl_->nproc);
    }

    QLInfo("nproc %d proc_used %d", impl_->nproc, impl_->proc_used);

    ConsumerGroupID2AddrScales consumer_group_id2addr_scales;
    ret = GetAddrScale(consumer_group_id2addr_scales);
    if (comm::RetCode::RET_OK != ret &&
        comm::RetCode::RET_ERR_RANGE_ADDR != ret /*clear queues while consumer's addr not found in consumer_config*/) {
        QLErr("ERR: GetAddrScale ret %d", comm::as_integer(ret));
        return ret;
    }

    std::set<int> valid_consumer_group_ids;
    for (auto &&kv : consumer_group_id2addr_scales) {
        valid_consumer_group_ids.insert(kv.first);
    }
    ClearInvalidConsumerGroupIDs(valid_consumer_group_ids);

    uint64_t conf_last_mod_time = config::GlobalConfig::GetThreadInstance()->GetLastModTime(topic_id);

    map<int, vector<Queue_t> > consumer_group_id2pending_queues;
    for (auto &&it : consumer_group_id2addr_scales) {
        auto &&consumer_group_id = it.first;
        auto &&addr_scale = it.second;

        QLVerb("consumer_group_id %d addr_scale.size %zu", consumer_group_id, addr_scale.size());

        uint64_t scale_hash = CalHash(addr_scale);

        comm::ConsumerHeartBeatLockBP::GetThreadInstance()->OnScaleHash(topic_id, consumer_group_id, scale_hash);

        if (LOCK_BUF_MAGIC == impl_->buf->magic &&
            conf_last_mod_time == impl_->conf_last_mod_time &&
            scale_hash == impl_->scale_hashs[consumer_group_id]) {
            QLInfo("no need to adjust scale. consumer_group_id %d scale_hash %" PRIu64, consumer_group_id, scale_hash);
            continue;
        }

        comm::ConsumerHeartBeatLockBP::GetThreadInstance()->OnAdjustScale(topic_id, consumer_group_id);

        std::vector<Queue_t> all_queues;
        if (comm::RetCode::RET_OK != (ret = GetAllQueues(consumer_group_id, all_queues))) {
            QLErr("ERR: GetAllQueues ret %d", comm::as_integer(ret));
            return ret;
        }

        auto &&pending_queues = consumer_group_id2pending_queues[consumer_group_id];
        if (comm::RetCode::RET_OK != (ret = GetPendingQueues(all_queues, addr_scale, pending_queues))) {
            QLErr("ERR: GetPendingQueues ret %d", comm::as_integer(ret));
            continue;
        }

        impl_->scale_hashs[consumer_group_id] = scale_hash;
    }

    DistubePendingQueues(consumer_group_id2pending_queues);

    impl_->conf_last_mod_time = conf_last_mod_time;

    comm::ConsumerHeartBeatLockBP::GetThreadInstance()->OnSyncSucc(topic_id);

    return comm::RetCode::RET_OK;
}

comm::RetCode HeartBeatLock::DoLock(const int vpid, Queue_t *const queue) {

    comm::RetCode ret;

    auto &&opt = impl_->consumer->GetConsumerOption();

    auto topic_id = impl_->consumer->GetTopicID();

    if (LOCK_ITEM_MAGIC != queue->magic) {
        comm::ConsumerHeartBeatLockBP::GetThreadInstance()->OnNoLockTarget(topic_id);
        return comm::RetCode::RET_NO_LOCK_TARGET;
    }

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("ERR: GetTopicConfigByTopicID ret %d topic_id %u", comm::as_integer(ret), topic_id);
        return ret;
    }

    shared_ptr<const config::proto::ConsumerGroup> consumer_group;
    if (comm::RetCode::RET_OK != (ret = topic_config->GetConsumerGroupByConsumerGroupID(queue->consumer_group_id, consumer_group))) {
        QLErr("ERR: GetConsumerGroupByConsumerGroupID ret %d consumer_group_id %u", comm::as_integer(ret), queue->consumer_group_id);
        return ret;
    }

    if (consumer_group->skip_lock()) {
        comm::ConsumerHeartBeatLockBP::GetThreadInstance()->OnSkipLock(topic_id, consumer_group->consumer_group_id());
        return comm::RetCode::RET_OK;
    }

    shared_ptr<const config::LockConfig> lock_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetLockConfig(topic_id, lock_config))) {
        QLErr("ERR: GetLockConfig ret %d topic_id %u", comm::as_integer(ret), topic_id);
        return ret;
    }

    set<int> lock_ids;
    if (comm::RetCode::RET_OK != (ret = lock_config->GetAllLockID(lock_ids))) {
        QLErr("ERR: GetAllLockID ret %d", comm::as_integer(ret));
        return ret;
    }

    if (lock_ids.empty()) {
        QLErr("ERR: lock_ids empty");
        return comm::RetCode::RET_ERR_RANGE_LOCK;
    }

    const int lock_id = *lock_ids.begin();

    string lock_key;
    {
        ostringstream oss;
        oss << topic_id << "-" << queue->consumer_group_id << "-" << queue->store_id << "-" << queue->queue_id;
        lock_key = oss.str();
    }

    string client_id;
    {
        ostringstream oss;
        oss << opt->ip << "-" << vpid;
        client_id = oss.str();
    }

    static __thread string last_lock_key = "";
    static __thread uint64_t version = 0;
    static __thread uint64_t overdue_time_ms = 0;

    if (!last_lock_key.empty() && last_lock_key != lock_key) { // try release old lock
        comm::proto::AcquireLockRequest req;
        comm::proto::AcquireLockResponse resp;

        req.set_topic_id(topic_id);
        req.set_lock_id(lock_id);
        auto &&lock_info = req.mutable_lock_info();
        lock_info->set_lock_key(last_lock_key);
        lock_info->set_version(version);
        lock_info->set_client_id(client_id);
        lock_info->set_lease_time_ms(0);

        lock::LockMasterClient<comm::proto::AcquireLockRequest, comm::proto::AcquireLockResponse> lock_master_client;
        ret = lock_master_client.ClientCall(req, resp, bind(&Consumer::AcquireLock, impl_->consumer, placeholders::_1, placeholders::_2));

        if (comm::RetCode::RET_OK == ret || comm::RetCode::RET_ACQUIRE_LOCK_FAIL == ret) {
            ;
        } else {
            QLErr("ERR: AcquireLock ret %d", comm::as_integer(ret));
        }

        last_lock_key = lock_key;
        version = 0;
        overdue_time_ms = 0;
    }

    uint64_t cur_time_ms = comm::utils::Time::GetSteadyClockMS();
    bool need_acquire_lock = false;
    {
        comm::proto::GetLockInfoRequest req;
        comm::proto::GetLockInfoResponse resp;

        req.set_topic_id(topic_id);
        req.set_lock_id(lock_id);
        req.set_lock_key(lock_key);

        lock::LockMasterClient<comm::proto::GetLockInfoRequest, comm::proto::GetLockInfoResponse> lock_master_client;
        ret = lock_master_client.ClientCall(req, resp, bind(&Consumer::GetLockInfo, impl_->consumer, placeholders::_1, placeholders::_2));

        if (0 > comm::as_integer(ret)) {
            QLErr("ERR: GetLockInfo ret %d", comm::as_integer(ret));
            return ret;
        }

        if (version != resp.lock_info().version()) {
            version = resp.lock_info().version();
            overdue_time_ms = cur_time_ms + resp.lock_info().lease_time_ms();
        }
        if (client_id == resp.lock_info().client_id() || cur_time_ms > overdue_time_ms)
            need_acquire_lock = true;
    }

    if (need_acquire_lock) {
        comm::proto::AcquireLockRequest req;
        comm::proto::AcquireLockResponse resp;

        req.set_topic_id(topic_id);
        req.set_lock_id(lock_id);
        auto &&lock_info = req.mutable_lock_info();
        lock_info->set_lock_key(lock_key);
        lock_info->set_version(version);
        lock_info->set_client_id(client_id);
        lock_info->set_lease_time_ms(topic_config->GetProto().topic().consumer_lock_lease_time_s() * 1000ULL);

        lock::LockMasterClient<comm::proto::AcquireLockRequest, comm::proto::AcquireLockResponse> lock_master_client;
        ret = lock_master_client.ClientCall(req, resp, bind(&Consumer::AcquireLock, impl_->consumer, placeholders::_1, placeholders::_2));

        if (comm::RetCode::RET_OK == ret || comm::RetCode::RET_ACQUIRE_LOCK_FAIL == ret) {
            return ret;
        } else {
            QLErr("ERR: AcquireLock ret %d", comm::as_integer(ret));
            return ret;
        }
    }

    return comm::RetCode::RET_NO_NEED_LOCK;
}

comm::RetCode HeartBeatLock::Lock(const int vpid, int &consumer_group_id, int &store_id, int &queue_id) {
    auto &&opt(impl_->consumer->GetConsumerOption());

    auto topic_id = impl_->consumer->GetTopicID();

    comm::ConsumerHeartBeatLockBP::GetThreadInstance()->OnLock(topic_id, consumer_group_id, store_id, queue_id);

    if (vpid >= impl_->nproc) {
        QLErr("ERR: vpid err. vpid %u impl_->nproc %u", vpid, impl_->nproc);
        comm::ConsumerHeartBeatLockBP::GetThreadInstance()->OnProcLack(topic_id);
        exit(-1);
    }

    Queue_t queue;
    {
        comm::utils::RWLock l(impl_->rwlock, comm::utils::RWLock::LockMode::READ);
        memcpy(&queue, &impl_->buf->queues[vpid], sizeof(Queue_t));
    }
    QLVerb("vpid %u queue consumer_group_id %u store_id %u queue_id %u", vpid, queue.consumer_group_id, queue.store_id, queue.queue_id);


    comm::RetCode ret;
    if (comm::RetCode::RET_OK != (ret = DoLock(vpid, &queue))) {
        if (comm::as_integer(ret) < 0) {
            QLErr("ERR: DoLock ret %d vpid %u", comm::as_integer(ret), vpid);
            comm::ConsumerHeartBeatLockBP::GetThreadInstance()->OnLockFail(topic_id, consumer_group_id, store_id, queue_id);
        }
        return ret;
    }
    consumer_group_id = queue.consumer_group_id;
    store_id = queue.store_id;
    queue_id = queue.queue_id;

    comm::ConsumerHeartBeatLockBP::GetThreadInstance()->OnLockSucc(topic_id, consumer_group_id, store_id, queue_id);
    return comm::RetCode::RET_OK;
}

comm::RetCode HeartBeatLock::GetQueuesDistribute(vector<Queue_t> &queues) {
    queues.clear();

    comm::utils::RWLock l(impl_->rwlock, comm::utils::RWLock::LockMode::READ);

    for (int vpid{0}; vpid < impl_->nproc; ++vpid) {
        queues.push_back(impl_->buf->queues[vpid]);
    }
    return comm::RetCode::RET_OK;
}


}  // namespace consumer

}  // namespace phxqueue

