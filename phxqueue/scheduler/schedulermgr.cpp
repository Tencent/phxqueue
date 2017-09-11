/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/scheduler/schedulermgr.h"

#include "phxqueue/comm.h"
#include "phxqueue/config.h"


namespace {


inline int LoadInfo2Load(const phxqueue::comm::proto::LoadInfo &loadinfo) {
    return loadinfo.cpu();
}


}


namespace phxqueue {

namespace scheduler {

using namespace std;


class SchedulerMgr::SchedulerMgrImpl {
  public:
    SchedulerMgrImpl() {}
    virtual ~SchedulerMgrImpl() {}

    Scheduler *scheduler{nullptr};
    uint64_t master_expire_time_ms{0};
    pthread_rwlock_t *rwlock{nullptr};
    map<int, TopicData> topic_id2data_map;
    map<int, TopicLock> topic_id2lock_map;
};


SchedulerMgr::SchedulerMgr(Scheduler *const scheduler) : impl_(new SchedulerMgrImpl()) {
    impl_->scheduler = scheduler;
}

SchedulerMgr::~SchedulerMgr() {}

comm::RetCode SchedulerMgr::Init() {
    comm::SchedulerMgrBP::GetThreadInstance()->OnInit();

    impl_->rwlock = new pthread_rwlock_t;
    pthread_rwlock_init(impl_->rwlock, nullptr);
    QLVerb("init rwlock");

    // hrw
    //hrwhash_ = new HRWHash(store_cli_conf->GetEndpointConfig());

    // consistent hashing
    //conhash_ = new ConHash();
    //conhash_->InitHash(store_cli_conf->GetEndpointConfig()->GetList());

    const uint64_t now{comm::utils::Time::GetSteadyClockMS()};
    comm::RetCode ret{LoadBalance(now)};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("LoadBalance err %d", ret);

        return ret;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode SchedulerMgr::Dispose() {
    comm::SchedulerMgrBP::GetThreadInstance()->OnDispose();

    {
        comm::utils::RWLock rwlock_write(impl_->rwlock, comm::utils::RWLock::LockMode::WRITE);
        impl_->topic_id2data_map.clear();
        //if (conhash_) {
        //    delete conhash_;
        //    conhash_ = nullptr;
        //}
        //if (hrwhash_) {
        //    delete hrwhash_;
        //    hrwhash_ = nullptr;
        //}
    }

    if (nullptr != impl_->rwlock) {
        QLVerb("destroy rwlock");
        pthread_rwlock_destroy(impl_->rwlock);
        delete impl_->rwlock;
        impl_->rwlock = nullptr;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode SchedulerMgr::GetAddrScale(const comm::proto::GetAddrScaleRequest &req,
                                         comm::proto::GetAddrScaleResponse &resp) {
    comm::RetCode ret;
    comm::SchedulerMgrBP::GetThreadInstance()->OnGetAddrScale(req);

    // TODO:
    //OssAttrInc(oss_attr_id(), 11, 1);
    uint64_t now{comm::utils::Time::GetSteadyClockMS()};
    if (!IsMaster(now)) {
        comm::SchedulerMgrBP::GetThreadInstance()->OnIMNotMaster(req);
        return comm::RetCode::RET_ERR_NOT_MASTER;
    }
    comm::SchedulerMgrBP::GetThreadInstance()->OnIMMaster(req);

    // prepare
    int topic_id{req.topic_id()};
    comm::utils::RWLock rwlock_read(impl_->rwlock, comm::utils::RWLock::LockMode::READ);
    auto topic_id2data_it(impl_->topic_id2data_map.find(topic_id));
    if (impl_->topic_id2data_map.end() == topic_id2data_it) {
        QLErr("topic %d not found", topic_id);
        // TODO:
        //OssAttrInc(oss_attr_id(), 14u, 1u);

        return comm::RetCode::RET_ERR_RANGE_TOPIC;
    }

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));

        return ret;
    }

    auto &topic_data(topic_id2data_it->second);
    auto topic_id2lock_it(impl_->topic_id2lock_map.find(topic_id));
    if (impl_->topic_id2lock_map.end() == topic_id2lock_it) {
        QLErr("topic %d lock lost", topic_id);

        return comm::RetCode::RET_ERR_LOGIC;
    }
    auto &topic_lock(topic_id2lock_it->second);

    uint64_t addr{comm::utils::EncodeAddr(req.addr())};
    comm::utils::RWLock topic_rwlock_read(topic_lock.rwlock, comm::utils::RWLock::LockMode::READ);

    TraceMap(topic_id, topic_data);
    auto it(topic_data.consumer_addr2info_map.find(addr));
    if (topic_data.consumer_addr2info_map.end() == it) {
        QLErr("topic %d consumer %s not found", topic_id, comm::utils::AddrToString(req.addr()).c_str());
        comm::SchedulerMgrBP::GetThreadInstance()->OnConsumerNotFound(req);

        return comm::RetCode::RET_ERR_RANGE_CONSUMER;
    }

    // found
    if (impl_->scheduler->NeedSkipUpdateLoad(req)) {
        comm::SchedulerMgrBP::GetThreadInstance()->OnSkipUpdateLoad(req.addr());
        QLInfo("topic %d consumer %s skip",
               topic_id, comm::utils::AddrToString(req.addr()).c_str());
    } else {
        auto &consumer_info(it->second);
        consumer_info.set_last_active_time(now);
        consumer_info.loads.Update(LoadInfo2Load(req.load_info()));
        const int new_load{consumer_info.loads.Mean()};
        if (topic_config->GetProto().topic().scheduler_load_sticky() <
            abs(new_load - consumer_info.sticky_load) || 0 >= consumer_info.sticky_load) {
            consumer_info.sticky_load = new_load;
            comm::SchedulerMgrBP::GetThreadInstance()->OnUpdateStickyLoad(req);
        }
        QLInfo("topic %d consumer %s last_active %" PRIu64 " sticky_load %d load_cursor %u loads {%s}",
               topic_id, comm::utils::AddrToString(req.addr()).c_str(),
               consumer_info.last_active_time(), consumer_info.sticky_load,
               consumer_info.loads.cursor(), consumer_info.loads.ToString().c_str());
    }

    // resp
    ret = BuildTopicScaleResponse(topic_data, resp);

    if (comm::RetCode::RET_OK != ret) {
        comm::SchedulerMgrBP::GetThreadInstance()->OnBuildTopicScaleResponseFail(req);
        QLErr("topic %d BuildTopicScaleResponse err %d", topic_id, ret);
    } else {
        comm::SchedulerMgrBP::GetThreadInstance()->OnBuildTopicScaleResponseSucc(req, resp);
        QLVerb("topic %d BuildTopicScaleResponse ok", topic_id);
    }

    return ret;
}

comm::RetCode SchedulerMgr::RenewMaster(const uint64_t expire_time_ms) {
    impl_->master_expire_time_ms = expire_time_ms;

    return comm::RetCode::RET_OK;
}

bool SchedulerMgr::IsMaster() {
    bool force{false};
    bool master{false};
    IsForceMaster(force, master);
    if (force) return master;

    return comm::utils::Time::GetSteadyClockMS() < impl_->master_expire_time_ms;
}

bool SchedulerMgr::IsMaster(uint64_t now) {
    bool force{false};
    bool master{false};
    IsForceMaster(force, master);
    if (force) return master;

    return now < impl_->master_expire_time_ms;
}

comm::RetCode SchedulerMgr::LoadBalance(const uint64_t now) {
    comm::RetCode ret{comm::RetCode::RET_ERR_UNINIT};

    ret = ReloadSchedConfig();
    if (comm::RetCode::RET_OK != ret) {
        QLErr("ReloadSchedConfig err %d", ret);

        return ret;
    }

    ret = ReloadTopicConfig();
    if (comm::RetCode::RET_OK != ret) {
        QLErr("ReloadTopicConfig err %d", ret);

        return ret;
    }

    comm::utils::RWLock rwlock_read(impl_->rwlock, comm::utils::RWLock::LockMode::READ);

    for (auto &&topic_id2data_kv : impl_->topic_id2data_map) {
        const int topic_id{topic_id2data_kv.first};
        QLInfo("topic_id %d", topic_id);

        comm::SchedulerLoadBalanceBP::GetThreadInstance()->OnLoadBalance(topic_id);

        auto &topic_data(topic_id2data_kv.second);
        auto topic_id2lock_it(impl_->topic_id2lock_map.find(topic_id));
        if (impl_->topic_id2lock_map.end() == topic_id2lock_it) {
            QLErr("topic %d lock lost", topic_id);

            continue;
        }
        auto &topic_lock(topic_id2lock_it->second);

        // update topic data
        bool consumer_conf_modified{false};
        comm::RetCode topic_ret{ReloadConsumerConfig(topic_id, topic_data, topic_lock,
                                               now, consumer_conf_modified)};
        if (comm::RetCode::RET_OK != topic_ret) {
            QLErr("topic %d ReloadConsumerConfig %d", topic_id, topic_ret);
            comm::SchedulerLoadBalanceBP::GetThreadInstance()->OnReloadConsumerConfigFail(topic_id);

            continue;
        }
        comm::SchedulerLoadBalanceBP::GetThreadInstance()->OnReloadConsumerConfigSucc(topic_id, consumer_conf_modified);

        Mode mode{GetMode(topic_id, topic_data, now)};
        if (Mode::Dynamic == mode) {
            comm::SchedulerLoadBalanceBP::GetThreadInstance()->OnDynamicMode(topic_id);

            // 1. live modified
            bool live_modified{false};
            topic_ret = UpdateLive(topic_id, topic_data, topic_lock, now, live_modified);
            if (comm::RetCode::RET_OK != topic_ret) {
                QLErr("topic %d UpdateLive err %d", topic_id, topic_ret);
                comm::SchedulerLoadBalanceBP::GetThreadInstance()->OnUpdateLiveFail(topic_id);

                continue;
            }
            comm::SchedulerLoadBalanceBP::GetThreadInstance()->OnUpdateLiveSucc(topic_id, live_modified);

            // 2. mean load
            float mean_load{0};
            topic_ret = GetMeanLoad(topic_id, topic_data, mean_load);
            if (0 > as_integer(topic_ret)) {
                QLErr("topic %d GetMeanLoad err %d", topic_id, topic_ret);
                comm::SchedulerLoadBalanceBP::GetThreadInstance()->OnGetMeanLoadFail(topic_id);
                continue;
            } else if (0 < as_integer(topic_ret)) {
                QLInfo("GetMeanLoad ret %d topic_id %d", as_integer(ret), topic_id);
                continue;
            }
            comm::SchedulerLoadBalanceBP::GetThreadInstance()->OnGetMeanLoadSucc(topic_id);

            // 3. balanced
            bool balanced{false};
            topic_ret = CheckImbalance(topic_id, topic_data, topic_lock, mean_load, balanced);
            if (comm::RetCode::RET_OK != topic_ret) {
                QLErr("topic %d CheckImbalance err %d", topic_id, topic_ret);
                comm::SchedulerLoadBalanceBP::GetThreadInstance()->OnCheckImbalanceFail(topic_id);

                continue;
            }
            comm::SchedulerLoadBalanceBP::GetThreadInstance()->
                    OnCheckImbalanceSucc(topic_id, mean_load, balanced);

            QLInfo("topic %d consumer_config_modified %d live_modified %d balance %d", topic_id,
                   consumer_conf_modified, live_modified, balanced);

            // 4. adjust weight
            if (consumer_conf_modified || live_modified || !balanced) {
                topic_ret = AdjustScale(topic_id, topic_data, topic_lock, mean_load, now,
                                        consumer_conf_modified | live_modified);
                if (comm::RetCode::RET_OK != topic_ret) {
                    QLErr("topic %d AdjustScale err %d", topic_id, topic_ret);
                    comm::SchedulerLoadBalanceBP::GetThreadInstance()->OnAdjustScaleFail(topic_id);

                    continue;
                }
                comm::SchedulerLoadBalanceBP::GetThreadInstance()->OnAdjustScaleSucc(topic_id);

            }

        }  // if (Mode::Dynamic == mode)

        TraceMap(topic_id, topic_data);
    }

    return comm::RetCode::RET_OK;
}

// TODO:
//comm::RetCode SchedulerMgr::GetStatus(const phxqueue::GetStatusReq &req, phxqueue::GetStatusResp &resp) {
//    uint64_t now{Time::GetSteadyClockMS()};
//    if (!IsMaster(now))
//        return comm::RetCode::RET_ERR_NOT_MASTER;
//
//    resp.clear_consumer_status();
//    RWLock rwlock_read(impl_->rwlock, comm::utils::RWLock::LockMode::READ);
//    auto it(impl_->topic_id2data_map.find(req.topic_id()));
//    if (impl_->topic_id2data_map.end() == it) {
//        QLErr("topic %d not found", req.topic_id());
//
//        return comm::RetCode::RET_ERR_RANGE_TOPIC;
//    }
//    TopicData &topic_data(it->second);
//
//    for (auto &&cosumer_ip2info_kv : *(topic_data.cosumer_ip2info_map_)) {
//        phxqueue::ConsumerStatus *status{resp.add_consumer_status()};
//        status->mutable_ip_weight()->set_ip(cosumer_ip2info_kv.first);
//        status->mutable_ip_weight()->set_weight(cosumer_ip2info_kv.second.weight);
//        status->set_init_weight(cosumer_ip2info_kv.second.init_weight);
//        status->set_sync_expire_time(cosumer_ip2info_kv.second.sync_expire_time);
//    }
//
//    return comm::RetCode::RET_OK;
//}

comm::RetCode SchedulerMgr::ReloadSchedConfig() {
    // TODO: conf_->LoadIfModified();

    return comm::RetCode::RET_OK;
}

comm::RetCode SchedulerMgr::ReloadTopicConfig() {
    set<int> topic_ids;
    comm::RetCode ret{config::GlobalConfig::GetThreadInstance()->GetAllTopicID(topic_ids)};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetAllTopic err %d", ret);

        return ret;
    }

    QLVerb("nr_topic %zu", topic_ids.size());

    // check map update
    bool modified{false};
    {
        comm::utils::RWLock rwlock_read(impl_->rwlock, comm::utils::RWLock::LockMode::READ);
        if (impl_->topic_id2data_map.size() != topic_ids.size()) {
            modified = true;
        } else {
            for (const int topic_id : topic_ids) {
                if (impl_->topic_id2data_map.end() == impl_->topic_id2data_map.find(topic_id)) {
                    modified = true;

                    break;
                }
            }
        }
    }

    // map updated
    if (modified) {
        comm::utils::RWLock rwlock_write(impl_->rwlock, comm::utils::RWLock::LockMode::WRITE);

        // add
        for (const int topic_id : topic_ids) {
            if (impl_->topic_id2data_map.end() == impl_->topic_id2data_map.find(topic_id)) {
                impl_->topic_id2lock_map.emplace(topic_id, move(TopicLock()));
                impl_->topic_id2data_map.emplace(topic_id, move(TopicData()));
            }
        }

        // remove
        if (topic_ids.size() != impl_->topic_id2data_map.size()) {
            set<int> topic_id_set;
            for (const auto &topic_id : topic_ids) {
                topic_id_set.emplace(topic_id);
            }
            for (auto it(impl_->topic_id2data_map.begin());
                 impl_->topic_id2data_map.end() != it; ++it) {
                if (topic_id_set.end() == topic_id_set.find(it->first)) {
                    const int topic_id{it->first};
                    auto topic_id2lock_it(impl_->topic_id2lock_map.find(topic_id));
                    if (impl_->topic_id2lock_map.end() == topic_id2lock_it) {
                        QLErr("topic %d lock lost", it->first);

                        continue;
                    }
                    // TODO: check if deadlock
                    comm::utils::RWLock rwlock_write(topic_id2lock_it->second.rwlock,
                                                     comm::utils::RWLock::LockMode::WRITE);
                    it = impl_->topic_id2data_map.erase(it);
                }
            }
        }
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode SchedulerMgr::ReloadConsumerConfig(const int topic_id, TopicData &topic_data,
                                                 TopicLock &topic_lock, const uint64_t now,
                                                 bool &modified) {
    modified = false;

    uint64_t conf_last_mod_time{config::GlobalConfig::GetThreadInstance()->GetLastModTime(topic_id)};
    if (conf_last_mod_time == topic_data.conf_last_mod_time) {
        QLVerb("topic %d not outdated skip new_mod_time %lu old_mod_time %lu",
               topic_id, conf_last_mod_time, topic_data.conf_last_mod_time);

        return comm::RetCode::RET_OK;
    } else {
        QLVerb("topic %d outdated new_mod_time %lu old_mod_time %lu",
               topic_id, conf_last_mod_time, topic_data.conf_last_mod_time);
    }

    shared_ptr<const config::ConsumerConfig> consumer_config;
    comm::RetCode ret{config::GlobalConfig::GetThreadInstance()->
            GetConsumerConfig(topic_id, consumer_config)};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetConsumerConfig err %d", ret);

        return ret;
    }

    map<uint64_t, int> consumer_addr2scale_map;
    ret = GetConsumerConfigDetail(topic_id, consumer_addr2scale_map, consumer_config);
    if (comm::RetCode::RET_OK != ret) {
        QLErr("topic %d GetConsumerConfigDetail err %d", topic_id, ret);
    }

    auto &consumer_addr2info_map(topic_data.consumer_addr2info_map);

    // check map update
    bool modify{false};
    {
        comm::utils::RWLock rwlock_read(topic_lock.rwlock, comm::utils::RWLock::LockMode::READ);
        if (consumer_addr2info_map.size() != consumer_addr2scale_map.size()) {
            modify = true;
        } else {
            for (auto &kv : consumer_addr2scale_map) {
                auto it(consumer_addr2info_map.find(kv.first));
                if (consumer_addr2info_map.end() == it) {
                    modify = true;

                    break;
                }
            }
        }
    }

    // map updated
    if (modify) {
        comm::utils::RWLock rwlock_write(topic_lock.rwlock, comm::utils::RWLock::LockMode::WRITE);

        // add
        for (auto &&kv : consumer_addr2scale_map) {
            if (consumer_addr2info_map.end() == consumer_addr2info_map.find(kv.first)) {
                // set both init weight and weight
                auto &consumer_info(consumer_addr2info_map[kv.first]);
                consumer_info.weight = consumer_info.init_weight = kv.second;

                comm::proto::Addr addr;
                comm::utils::DecodeAddr(kv.first, addr);
                comm::SchedulerLoadBalanceBP::GetThreadInstance()->OnConsumerAdd(topic_id, addr);
            }
        }

        // remove
        if (consumer_addr2scale_map.size() != consumer_addr2info_map.size()) {
            for (auto it(consumer_addr2info_map.begin()); consumer_addr2info_map.end() != it;) {
                if (consumer_addr2scale_map.end() == consumer_addr2scale_map.find(it->first)) {
                    comm::proto::Addr addr;
                    comm::utils::DecodeAddr(it->first, addr);
                    comm::SchedulerLoadBalanceBP::GetThreadInstance()->
                            OnConsumerRemove(topic_id, addr);

                    it = consumer_addr2info_map.erase(it);
                } else {
                    ++it;
                }
            }
        }

        comm::SchedulerLoadBalanceBP::GetThreadInstance()->
                OnConsumerChange(topic_id, consumer_addr2info_map.size());
    }

    // update consumer info
    {
        comm::utils::RWLock rwlock_read(topic_lock.rwlock, comm::utils::RWLock::LockMode::READ);

        for (auto &&kv : consumer_addr2info_map) {
            // update weight only
            kv.second.init_weight = consumer_addr2scale_map[kv.first];
        }
    }

    modified = true;
    topic_data.conf_last_mod_time = conf_last_mod_time;

    return comm::RetCode::RET_OK;
}

comm::RetCode SchedulerMgr::GetConsumerConfigDetail(const int topic_id,
                                                    map<uint64_t, int> &cosumer_addr2scale_map,
                                                    shared_ptr<const config::ConsumerConfig> consumer_config) {
    vector<shared_ptr<const config::proto::Consumer>> consumers;
    comm::RetCode ret{consumer_config->GetAllConsumer(consumers)};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetAllConsumer err %d", ret);

        return ret;
    }

    for (auto &&consumer : consumers) {
        uint64_t addr{comm::utils::EncodeAddr(consumer->addr())};
        cosumer_addr2scale_map.emplace(addr, consumer->scale());
        QLVerb("topic %d consumer %s init_weight %d",
               topic_id, comm::utils::AddrToString(consumer->addr()).c_str(), consumer->scale());
    }

    return ret;
}

comm::RetCode SchedulerMgr::BuildTopicScaleResponse(const TopicData &topic_data,
                                                    comm::proto::GetAddrScaleResponse &resp) {
    resp.mutable_addr_scales()->Clear();
    resp.mutable_addr_scales()->Reserve(topic_data.consumer_addr2info_map.size());
    for (auto &&consumer_addr2info_kv : topic_data.consumer_addr2info_map) {
        comm::proto::AddrScale *addr_scale{resp.add_addr_scales()};
        comm::utils::DecodeAddr(consumer_addr2info_kv.first, *(addr_scale->mutable_addr()));
        addr_scale->set_scale(consumer_addr2info_kv.second.weight);
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode SchedulerMgr::BuildTopicScaleString(const TopicData &topic_data, string &s) {
    for (auto &&consumer_addr2info_kv : topic_data.consumer_addr2info_map) {
        if (1000 < s.size()) {
            s += "...";

            return comm::RetCode::RET_OK;
        }
        s += comm::utils::EncodedAddrToIPString(consumer_addr2info_kv.first) + ":";
        s += to_string(consumer_addr2info_kv.second.weight) + ";";
    }

    return comm::RetCode::RET_OK;
}

Mode SchedulerMgr::GetMode(const int topic_id, TopicData &topic_data, const uint64_t now) {
    Mode mode{Mode::Dynamic};
    std::shared_ptr<const config::TopicConfig> topic_config;
    comm::RetCode ret{config::GlobalConfig::GetThreadInstance()->
            GetTopicConfigByTopicID(topic_id, topic_config)};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetTopicConfigByTopicID err %d", ret);

        return Mode::Static;
    }

    if (topic_data.init_time +
        3 * topic_config->GetProto().topic().scheduler_get_scale_interval_s() * 1000 >= now) {
        // stay a long time for most consumer GetAddrScale()
        QLInfo("Mode::Dynamic not ready");
        mode = Mode::Static;
    }

    return mode;
}

comm::RetCode SchedulerMgr::GetMeanLoad(const int topic_id, const TopicData &topic_data,
                                        float &mean_load) {
    mean_load = 0;
    int nr_live{0};
    int sum_load{0};

    for (const auto &cosumer_addr2info_kv : topic_data.consumer_addr2info_map) {
        const auto &consumer_info(cosumer_addr2info_kv.second);
        if (!consumer_info.live)
            continue;

        sum_load += consumer_info.sticky_load;
        ++nr_live;
    }

    if (0 >= nr_live) {
        QLInfo("topic_id %d no live consumer", topic_id);
        return comm::RetCode::RET_NO_LIVE_CONSUMER;
    }

    if (0 >= sum_load) {
        QLErr("topic %d sum_load", topic_id, sum_load, sum_load);

        return comm::RetCode::RET_ERR_LOGIC;
    }

    mean_load = sum_load / nr_live;
    QLInfo("topic %d mean_load %.1f sum_load %d nr_live %d", topic_id, mean_load, sum_load, nr_live);

    return comm::RetCode::RET_OK;
}

comm::RetCode SchedulerMgr::UpdateLive(const int topic_id, TopicData &topic_data,
                                       TopicLock &topic_lock, const uint64_t now, bool &modified) {
    comm::utils::RWLock rwlock_read(topic_lock.rwlock, comm::utils::RWLock::LockMode::READ);

    comm::RetCode ret;

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->
                                  GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));

        return ret;
    }

    modified = false;
    for (auto &&consumer_addr2info_kv : topic_data.consumer_addr2info_map) {
        ConsumerInfo &consumer_info(consumer_addr2info_kv.second);

        // new die
        if (consumer_info.live && consumer_info.last_active_time() +
            topic_config->GetProto().topic().scheduler_hash_keep_time_s() * 1000 <= now) {
            consumer_info.Reset();
            consumer_info.live = false;
            // TODO:
            //OssAttrInc(oss_attr_id(), 36u, 1u);
            modified = true;
            QLInfo("topic %d consumer %s new_die last_active %" PRIu64 " now %" PRIu64, topic_id,
                   comm::utils::EncodedAddrToIPString(consumer_addr2info_kv.first).c_str(),
                   consumer_info.last_active_time(), now);

            comm::proto::Addr addr;
            comm::utils::DecodeAddr(consumer_addr2info_kv.first, addr);
            comm::SchedulerLoadBalanceBP::GetThreadInstance()->OnConsumerNewDie(topic_id, addr);

            continue;
        }

        // new live
        if (!consumer_info.live && consumer_info.last_active_time() +
            topic_config->GetProto().topic().scheduler_hash_keep_time_s() * 1000 > now) {
            consumer_info.live = true;
            // TODO:
            //OssAttrInc(oss_attr_id(), 35u, 1u);
            modified = true;
            QLInfo("topic %d consumer %s new_live last_active %" PRIu64 " now %" PRIu64,
                   topic_id, comm::utils::EncodedAddrToIPString(consumer_addr2info_kv.first).c_str(),
                   consumer_info.last_active_time(), now);

            comm::proto::Addr addr;
            comm::utils::DecodeAddr(consumer_addr2info_kv.first, addr);
            comm::SchedulerLoadBalanceBP::GetThreadInstance()->OnConsumerNewLive(topic_id, addr);

            continue;
        }
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode SchedulerMgr::CheckImbalance(const int topic_id, const TopicData &topic_data,
                                           TopicLock &topic_lock, const float mean_load,
                                           bool &balance) {
    balance = false;
    int nr_live{0};
    int nr_balance{0};

    comm::RetCode ret;

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->
                                  GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));

        return ret;
    }

    comm::utils::RWLock rwlock_read(topic_lock.rwlock, comm::utils::RWLock::LockMode::READ);

    for (const auto &cosumer_addr2info_kv : topic_data.consumer_addr2info_map) {
        const string consumer_addr_string{comm::utils::EncodedAddrToIPString(cosumer_addr2info_kv.first)};
        const auto &consumer_info(cosumer_addr2info_kv.second);

        if (!consumer_info.live)
            continue;

        if (!IsConsumerImbalance(topic_config, consumer_addr_string,
                                 consumer_info.sticky_load, mean_load)) {
            ++nr_balance;
        }
    }

    balance = (nr_live == nr_balance);

    QLInfo("topic %d nr_consumer %zu nr_live %d nr_balance %d mean_load %.1f",
           topic_id, topic_data.consumer_addr2info_map.size(),
           nr_live, nr_balance, mean_load);

    return comm::RetCode::RET_OK;
}

comm::RetCode SchedulerMgr::AdjustScale(const int topic_id, TopicData &topic_data,
                                        TopicLock &topic_lock, const float mean_load,
                                        const uint64_t now, const bool force_apply) {
    comm::utils::RWLock rwlock_read(topic_lock.rwlock, comm::utils::RWLock::LockMode::READ);

    comm::RetCode ret;

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->
                                  GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));

        return ret;
    }


    // 1. preview reduce weight
    int nr_preview_adjust{0};
    map<uint64_t, int> consumer_addr2preview_map;
    for (auto &&consumer_addr2info_kv : topic_data.consumer_addr2info_map) {
        const uint64_t consumer_addr{consumer_addr2info_kv.first};
        const string consumer_addr_string{comm::utils::EncodedAddrToIPString(consumer_addr)};
        auto &&consumer_info(consumer_addr2info_kv.second);

        int new_weight{consumer_info.weight};
        if (consumer_info.live) {
            new_weight = NextConsumerWeight(topic_config, consumer_info.sticky_load,
                                            mean_load, consumer_info.init_weight);
            consumer_addr2preview_map.emplace(consumer_addr, new_weight);
        } else {
            new_weight = 0;
            consumer_addr2preview_map.emplace(consumer_addr, new_weight);
        }
        if (new_weight != consumer_info.weight) {
            ++nr_preview_adjust;
            QLInfo("topic %d consumer %s adjust change init_weight %d preview weight %d -> %d "
                   "sticky_load %d mean_load %.1f now %" PRIu64,
                   topic_id, consumer_addr_string.c_str(), consumer_info.init_weight,
                   consumer_info.weight, new_weight,
                   consumer_info.sticky_load, mean_load, now);


            comm::proto::Addr addr;
            comm::utils::DecodeAddr(consumer_addr, addr);
            comm::SchedulerLoadBalanceBP::GetThreadInstance()->
                    OnPreviewAdjustChange(topic_id, addr, consumer_info.init_weight,
                                          consumer_info.weight, new_weight);

        } else {
            QLInfo("topic %d consumer %s adjust unchange init_weight %d "
                   "weight %d sticky_load %d mean_load %.1f now %" PRIu64,
                   topic_id, consumer_addr_string.c_str(), consumer_info.init_weight,
                   consumer_info.weight, consumer_info.sticky_load, mean_load, now);
            // TODO:
            //OssAttrInc(oss_attr_id(), 52u, 1u);

            comm::proto::Addr addr;
            comm::utils::DecodeAddr(consumer_addr, addr);
            comm::SchedulerLoadBalanceBP::GetThreadInstance()->
                    OnPreviewAdjustUnchange(topic_id, addr, consumer_info.init_weight,
                                            consumer_info.weight);
        }
    }

    // 2. apply or cancel
    bool apply{force_apply};
    int nr_consumer{0};
    int sum_diff{0};

    for (auto &&consumer_addr2info_kv : topic_data.consumer_addr2info_map) {
        const uint64_t consumer_addr{consumer_addr2info_kv.first};
        const ConsumerInfo &consumer_info(consumer_addr2info_kv.second);
        auto consumer_addr2preview_it(consumer_addr2preview_map.find(consumer_addr));
        if (consumer_addr2preview_map.end() != consumer_addr2preview_it) {
            ++nr_consumer;
            sum_diff += abs(consumer_info.weight - consumer_addr2preview_it->second);
        }
    }

    if (!apply) {
        if (static_cast<double>(nr_consumer) *
            topic_config->GetProto().topic().scheduler_weight_sticky() < sum_diff) {
            apply = true;
        }
    }

    // 3. apply
    if (apply) {
        comm::SchedulerLoadBalanceBP::GetThreadInstance()->OnAdjustApply(topic_id);

        for (auto &consumer_addr2info_kv : topic_data.consumer_addr2info_map) {
            const uint64_t consumer_addr{consumer_addr2info_kv.first};
            ConsumerInfo &consumer_info(consumer_addr2info_kv.second);
            auto consumer_addr2preview_it(consumer_addr2preview_map.find(consumer_addr));
            if (consumer_addr2preview_map.end() != consumer_addr2preview_it) {
                consumer_info.weight = consumer_addr2preview_it->second;
            }
        }
    } else {
        comm::SchedulerLoadBalanceBP::GetThreadInstance()->OnAdjustNotApply(topic_id);
    }

    // 4. result
    string s;
    BuildTopicScaleString(topic_data, s);

    QLInfo("topic %d mean_load %.1f nr_preview_adjust %d sum_diff %d "
           "nr_consumer %d apply %d force_apply %d consumers %zu {%s}",
           topic_id, mean_load, nr_preview_adjust, sum_diff, nr_consumer, apply,
           force_apply, topic_data.consumer_addr2info_map.size(), s.c_str());

    return comm::RetCode::RET_OK;
}

bool SchedulerMgr::IsConsumerImbalance(const shared_ptr<const config::TopicConfig> &topic_config,
                                       const string &consumer_addr_string,
                                       const int sticky_load, const float &mean_load) {

    const int topic_id = topic_config->GetProto().topic().topic_id();

    bool load_imbalance{topic_config->GetProto().topic().scheduler_max_load() < sticky_load &&
            mean_load * topic_config->GetProto().topic().scheduler_max_mean_load_percent() / 100 < sticky_load};

    if (load_imbalance) {
        // TODO:
        //OssAttrInc(oss_attr_id(), 31u, 1u);
    }
    QLVerb("topic %d consumer %s imbalance %d sticky_load %d mean_load %.1f",
           topic_id, consumer_addr_string.c_str(), load_imbalance,
           sticky_load, mean_load);

    return load_imbalance;
}

int SchedulerMgr::NextConsumerWeight(const shared_ptr<const config::TopicConfig> &topic_config,
                                     const int dynamic_load, const float mean_load,
                                     const int init_weight) {
    if (0 >= dynamic_load)
        return 0;

    int new_weight{static_cast<int>(init_weight * mean_load /
                                    static_cast<float>(dynamic_load))};
    if (init_weight * topic_config->GetProto().topic().scheduler_max_init_weight_percent() / 100 < new_weight) {
        new_weight = init_weight * topic_config->GetProto().topic().scheduler_max_init_weight_percent() / 100;
    }

    return new_weight;
}

void SchedulerMgr::IsForceMaster(bool &force, bool &master) {
    force = false;

    comm::RetCode ret;

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->
                                  GetTopicConfigByTopicID(impl_->scheduler->GetTopicID(), topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));

        return;
    }

    QLInfo("local %s:%d force master %s:%d",
           impl_->scheduler->GetSchedulerOption()->ip.c_str(),
           impl_->scheduler->GetSchedulerOption()->port,
           topic_config->GetProto().topic().scheduler_master_ip().c_str(),
           topic_config->GetProto().topic().scheduler_master_port());
    if (0 != topic_config->GetProto().topic().scheduler_master_port() &&
        !topic_config->GetProto().topic().scheduler_master_ip().empty() &&
        "0.0.0.0" != topic_config->GetProto().topic().scheduler_master_ip()) {
        force = true;
        if (impl_->scheduler->GetSchedulerOption()->ip ==
            topic_config->GetProto().topic().scheduler_master_ip() &&
            impl_->scheduler->GetSchedulerOption()->port ==
            topic_config->GetProto().topic().scheduler_master_port()) {
            master = true;

            return;
        }
        master = false;

        return;
    }

    return;
}

void SchedulerMgr::TraceMap(const int topic_id, const TopicData &topic_data) {
    string s;
    const auto &consumer_addr2info_map(topic_data.consumer_addr2info_map);
    for (auto &&consumer_addr2info_kv : consumer_addr2info_map) {
        s += comm::utils::EncodedAddrToIPString(consumer_addr2info_kv.first) + ":" +
                to_string(consumer_addr2info_kv.first) + ":" +
                to_string(consumer_addr2info_kv.second.weight) + ":" +
                to_string(consumer_addr2info_kv.second.init_weight) + ";";
    }
    QLVerb("map {%s}", s.c_str());
}


}  // namespace scheduler

}  // namespace phxqueue

