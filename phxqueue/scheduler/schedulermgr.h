/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <atomic>
#include <cinttypes>
#include <list>
#include <map>
#include <memory>
#include <vector>
#include <mutex>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"

#include "phxqueue/scheduler/scheduler.h"


namespace phxqueue {

namespace scheduler {


enum class Mode {Static, Dynamic};


class MeanVector {
  public:
    MeanVector(const uint32_t size) {
        vector_.resize(size);
    }
    MeanVector(const MeanVector &) = delete;
    MeanVector &operator=(const MeanVector &) = delete;

    virtual ~MeanVector() = default;

    void Update(const int value) {
        std::lock_guard<std::mutex> guard(mutex_);

        const uint32_t size{vector_.size()};
        if (0 >= size)
            return;

        vector_[cursor_ % size] = value;
        ++cursor_;
    }

    float Mean() {
        std::lock_guard<std::mutex> guard(mutex_);

        const uint32_t size{vector_.size()};
        if (0 >= size || 0 >= cursor_)
            return 0;

        int sum{0};
        uint32_t all{cursor_ < size ? cursor_ : size};
        for (int i{0}; all > i; ++i) {
            sum += vector_.at(i);
        }

        return static_cast<float>(sum) / all;
    }

    void Reset() {
        std::lock_guard<std::mutex> guard(mutex_);

        cursor_ = 0;
    }

    std::string ToString(const std::string &separator = ",") {
        std::string s;
        for (int i{0}; vector_.size() > i; ++i) {
            s += std::to_string(vector_.at(i)) + separator;
        }

        return s;
    }

    uint32_t cursor() const {
        return cursor_;
    }

  private:
    std::mutex mutex_;
    uint32_t cursor_{0};
    std::vector<int> vector_;
};


class ConsumerInfo {
  public:
    ConsumerInfo() : loads(6) {}
    ConsumerInfo(const ConsumerInfo &) = delete;
    ConsumerInfo(ConsumerInfo &&) = default;

    virtual ~ConsumerInfo() = default;

    ConsumerInfo &operator=(const ConsumerInfo &) = delete;
    ConsumerInfo &operator=(ConsumerInfo &&) = default;

    void Reset() {
        weight = 0;
        loads.Reset();
        sticky_load = 0;
    }

    void set_last_active_time(const uint64_t time) {
        return last_active_time_.store(time);
    }

    uint64_t last_active_time() {
        return last_active_time_.load();
    }

    int weight{0};
    MeanVector loads;
    int sticky_load{0};

    int init_weight{0};
    // just a cache used by LoadBalance(), update by UpdateLive()
    bool live{false};

  private:
    // update by GetAddrScale()
    std::atomic<uint64_t> last_active_time_{0};
};


enum class OperationCode {
    UNKNOWN = 0,
    GET_ADDR_SCALE = 1
};


struct Operation {
    Operation(const OperationCode op2, const uint64_t time2, const int &cpu2)
            : op(op2), time(time2), cpu(cpu2) {}

    Operation(const Operation &) = default;

    Operation(Operation &&) = default;

    virtual ~Operation() = default;

    Operation &operator=(const Operation &) = default;

    Operation &operator=(Operation &&) = default;

    OperationCode op{OperationCode::UNKNOWN};
    uint64_t time{0};
    int cpu{0};
};

typedef std::list<Operation> OperationList;


template <typename T>
T Mean(const std::vector<T> &v) {
    if (0 >= v.size())
        return 0;

    T sum(0);
    int total{0};
    for (auto &&i : v) {
        if (0 < i) {
            sum += i;
            ++total;
        }
    }

    if (0 >= sum || 0 >= total)
        return 0;

    return sum / total;
}


class TopicData {
  public:
    TopicData() {}

    // noncopyable
    TopicData(const TopicData &) = delete;

    TopicData(TopicData &&rhs) {
        consumer_addr2info_map = std::move(rhs.consumer_addr2info_map);
        conf_last_mod_time = rhs.conf_last_mod_time;
        init_time = rhs.init_time;
        cosumer_ip2history_map = std::move(rhs.cosumer_ip2history_map);
    }

    virtual ~TopicData() = default;

    // noncopyable
    TopicData &operator=(const TopicData &) = delete;

    TopicData &operator=(TopicData &&rhs) {
        consumer_addr2info_map = std::move(rhs.consumer_addr2info_map);
        conf_last_mod_time = rhs.conf_last_mod_time;
        init_time = rhs.init_time;
        cosumer_ip2history_map = std::move(rhs.cosumer_ip2history_map);

        return *this;
    }

    std::map<uint64_t, ConsumerInfo> consumer_addr2info_map;
    time_t conf_last_mod_time{0};
    uint64_t init_time{comm::utils::Time::GetSteadyClockMS()};
    std::map<uint32_t, OperationList> cosumer_ip2history_map;
    // must modify move constructor & move assignment at the same time
};


class TopicLock {
  public:
    TopicLock() {
        rwlock = new pthread_rwlock_t;
        pthread_rwlock_init(rwlock, nullptr);
        QLVerb("init rwlock");
    }

    // noncopyable
    TopicLock(const TopicLock &) = delete;

    TopicLock(TopicLock &&rhs) {
        rwlock = rhs.rwlock;
        rhs.rwlock = nullptr;
    }

    virtual ~TopicLock() {
        if (nullptr != rwlock) {
            QLVerb("destroy rwlock");
            pthread_rwlock_destroy(rwlock);
            delete rwlock;
            rwlock = nullptr;
        }
    }

    // noncopyable
    TopicLock &operator=(const TopicLock &) = delete;

    TopicLock &operator=(TopicLock &&rhs) {
        rwlock = rhs.rwlock;
        rhs.rwlock = nullptr;

        return *this;
    }

    pthread_rwlock_t *rwlock{nullptr};
    // must modify move constructor & move assignment at the same time
};


class SchedulerMgr {
  public:
    SchedulerMgr(Scheduler *const scheduler);
    virtual ~SchedulerMgr();

    comm::RetCode Init();
    comm::RetCode Dispose();

    comm::RetCode GetAddrScale(const comm::proto::GetAddrScaleRequest &req,
                               comm::proto::GetAddrScaleResponse &resp);

    comm::RetCode RenewMaster(const uint64_t expire_time_ms);
    bool IsMaster();
    bool IsMaster(uint64_t now);

    comm::RetCode LoadBalance(const uint64_t now);
    // TODO:
    //comm::RetCode GetStatus(const phxqueue::GetStatusReq &req, phxqueue::GetStatusResp &resp);

    //uint64_t master_expire_time() const { return master_expire_time_; }
    //void set_master_expire_time(const uint64_t master_expire_time) {
    //    master_expire_time_ = master_expire_time;
    //}

  private:
    // reload conf
    comm::RetCode ReloadSchedConfig();
    comm::RetCode ReloadTopicConfig();
    comm::RetCode ReloadConsumerConfig(const int topic_id, TopicData &topic_data,
                                       TopicLock &topic_lock, const uint64_t now, bool &modified);
    comm::RetCode GetConsumerConfigDetail(const int topic_id,
                                          std::map<uint64_t, int> &cosumer_addr2scale_map,
                                          std::shared_ptr<const config::ConsumerConfig> consumer_config);

    // topic
    comm::RetCode BuildTopicScaleResponse(const TopicData &topic_data,
                                          comm::proto::GetAddrScaleResponse &resp);
    comm::RetCode BuildTopicScaleString(const TopicData &topic_data, std::string &s);
    Mode GetMode(const int topic_id, TopicData &topic_data, const uint64_t now);
    comm::RetCode GetMeanLoad(const int topic_id, const TopicData &topic_data, float &mean_load);
    comm::RetCode UpdateLive(const int topic_id, TopicData &topic_data, TopicLock &topic_lock,
                             const uint64_t now, bool &modified);
    comm::RetCode CheckImbalance(const int topic_id, const TopicData &topic_data, TopicLock &topic_lock,
                                 const float mean_load, bool &balance);
    comm::RetCode AdjustScale(const int topic_id, TopicData &topic_data, TopicLock &topic_lock,
                              const float mean_load, const uint64_t now, const bool force_apply);

    // consumer
    bool IsConsumerImbalance(const std::shared_ptr<const config::TopicConfig> &topic_config, const std::string &consumer_addr_string,
                             const int sticky_load, const float &mean_load);
    int NextConsumerWeight(const std::shared_ptr<const config::TopicConfig> &topic_config, const int dynamic_load, const float mean_load, const int init_weight);

    // misc
    void IsForceMaster(bool &force, bool &master);

    // debug
    void TraceMap(const int topic_id, const TopicData &topic_data);

    class SchedulerMgrImpl;
    std::unique_ptr<SchedulerMgrImpl> impl_;
};


}  // namespace scheduler

}  // namespace phxqueue

