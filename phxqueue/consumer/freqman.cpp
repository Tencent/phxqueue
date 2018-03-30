/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/consumer/freqman.h"

#include <algorithm>
#include <cinttypes>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <map>
#include <memory>
#include <pthread.h>
#include <set>
#include <signal.h>
#include <sstream>
#include <sys/ipc.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <vector>
#include <zlib.h>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"


namespace phxqueue {

namespace consumer {


using namespace std;


#define MAX_VPID 10000
#define MAX_HANDLE_ID_NUM 1000


struct LimitInfo_t {
    int nhandle_limit;
    int nrefill;
    int refill_interval_ms;
};

struct ConsumeStat_t {
    int nget = 0;
    int nhandle_tot = 0;
    int handle_id_rank2nhandle[MAX_HANDLE_ID_NUM + 1];
};

struct FreqManShm_t {
    LimitInfo_t limit_infos[MAX_VPID + 1];
    ConsumeStat_t consume_stats[MAX_VPID + 1];
};

class FreqMan::FreqManImpl {
  public:
    int topic_id = 0;
    Consumer *consumer = nullptr;
    uint64_t last_update_time = 0;

    FreqManShm_t *shm = nullptr;
};

FreqMan::FreqMan() : impl_(new FreqManImpl()){}

FreqMan::~FreqMan() {}

comm::RetCode FreqMan::Init(const int topic_id, Consumer *consumer) {
    impl_->topic_id = topic_id;
    impl_->consumer = consumer;

    impl_->last_update_time = comm::utils::Time::GetSteadyClockMS();

    impl_->shm = (FreqManShm_t *)mmap(nullptr, sizeof(FreqManShm_t), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (MAP_FAILED == impl_->shm) {
        QLErr("mmap failed %s", strerror(errno));
        return comm::RetCode::RET_ERR_SYS;
    }
    memset(impl_->shm, 0, sizeof(FreqManShm_t));

    return comm::RetCode::RET_OK;
}

static void ClearConsumeStat(ConsumeStat_t &consume_stat, const int nhandle_id) {
    consume_stat.nget = 0;
    consume_stat.nhandle_tot = 0;
    for (int i{0}; i < nhandle_id; ++i) consume_stat.handle_id_rank2nhandle[i] = 0;
}

void FreqMan::UpdateConsumeStat(const int vpid, const comm::proto::ConsumerContext &cc,
                                const vector<shared_ptr<comm::proto::QItem>> &items) {
    if (vpid > MAX_VPID) {
        QLErr("vpid(%d) > MAX_VPID(%d)", vpid, MAX_VPID);
        return;
    }

    comm::RetCode ret;

    auto &&consume_stat(impl_->shm->consume_stats[vpid]);

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(impl_->topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));
        return;
    }
    static uint64_t topic_config_last_mod_time{0};
    auto tmp_last_mod_time(topic_config->GetLastModTime());
    if (topic_config_last_mod_time != tmp_last_mod_time) {
        topic_config_last_mod_time = tmp_last_mod_time;
        ClearConsumeStat(consume_stat, topic_config->GetProto().topic().handle_ids_size());
    }

    ++consume_stat.nget;

    for (auto &&item : items) {
        if (impl_->consumer->SkipHandle(cc, *item)) continue;

        int rank;
        if (comm::RetCode::RET_OK != (ret = topic_config->GetHandleIDRank(item->handle_id(), rank))) {
            QLErr("GetHandleIDRank ret %d handle_id %d", as_integer(ret), item->handle_id());
            continue;
        }
        if (rank > MAX_HANDLE_ID_NUM) {
            QLErr("handle_id %d rank(%d) > MAX_HANDLE_ID_NUM(%d)", item->handle_id(), rank, MAX_HANDLE_ID_NUM);
            continue;
        }
        ++consume_stat.nhandle_tot;
        ++consume_stat.handle_id_rank2nhandle[rank];
    }
    QLVerb("consume_stat nget %d nhandle_tot %d", consume_stat.nget, consume_stat.nhandle_tot);
}

void FreqMan::Run() {
    comm::RetCode ret;
    pid_t pid = fork();
    if (pid == 0) {
        prctl(PR_SET_PDEATHSIG, SIGHUP);

        while (true) {
            try {
                if (comm::RetCode::RET_OK != (ret =  UpdateLimitInfo())) {
                    QLErr("UpdateLimitInfo ret %d", ret);
                }
                ClearAllConsumeStat();
            } catch (const std::exception& e) {
                QLErr("exception %s", e.what());
            }
            sleep(5);
        }
        exit(0);
    }
}

void FreqMan::ClearAllConsumeStat() {
    comm::RetCode ret;
    auto &&opt(impl_->consumer->GetConsumerOption());

    int nr_procs{opt->nprocs};

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(impl_->topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));
        return;
    }

    for (int i{0}; i < nr_procs; ++i) {
        ClearConsumeStat(impl_->shm->consume_stats[i], topic_config->GetProto().topic().handle_ids_size());
    }
}


comm::RetCode FreqMan::UpdateLimitInfo() {
    QLVerb("start");

    comm::RetCode ret;

    uint64_t now = comm::utils::Time::GetSteadyClockMS();
    uint64_t diff_time_ms = now - impl_->last_update_time;
    impl_->last_update_time = now;

    auto &&opt(impl_->consumer->GetConsumerOption());

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(impl_->topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));
        return ret;
    }
    const int batch_limit = topic_config->GetProto().topic().batch_limit();

    shared_ptr<const config::StoreConfig> store_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetStoreConfig(impl_->topic_id, store_config))) {
        QLErr("GetStoreConfig ret %d", as_integer(ret));
        return ret;
    }

    set<int> store_ids;
    if (comm::RetCode::RET_OK != (ret = store_config->GetAllStoreID(store_ids))) {
        QLErr("GetAllStoreID ret %d", as_integer(ret));
        return ret;
    }

    vector<shared_ptr<config::proto::FreqInfo> > freq_infos;
    if (comm::RetCode::RET_OK != (ret = topic_config->GetAllFreqInfo(freq_infos))) {
        QLErr("GetAllFreqInfo ret %d", as_integer(ret));
        return ret;
    }

    vector<Queue_t> queues;
    if (comm::RetCode::RET_OK != (ret = impl_->consumer->GetQueuesDistribute(queues))) {
        QLErr("GetQueuesDistribute ret %d", as_integer(ret));
        return ret;
    }

    if (queues.size() != opt->nprocs) {
        QLErr("queues.size(%d) != opt->nprocs(%d)", queues.size(), opt->nprocs);
        return comm::RetCode::RET_ERR_UNEXPECTED;
    }

    unique_ptr<bool[]> updated(new bool[opt->nprocs]);
    memset(updated.get(), 0, sizeof(bool) * opt->nprocs);

    map<tuple<int, int, int>, double> pub_consumer_group_queue_info_id2max_handle_pct;
    for (auto &&freq_info : freq_infos) {
        auto pub_id = freq_info->pub_id();
        auto consumer_group_id = freq_info->consumer_group_id();
        auto handle_id = freq_info->handle_id();
        auto limit_per_min = freq_info->limit_per_min();
        auto queue_info_id = freq_info->queue_info_id();

        QLVerb("pub_id %d consumer_group_id %d handle_id %d limit_per_min %d", pub_id, consumer_group_id, handle_id, limit_per_min);

        if (!limit_per_min) continue;

        int nrefill = 0;
        int refill_interval_ms = 0;

        int handle_id_rank = -1;
        if (-1 != handle_id) {
            if (comm::RetCode::RET_OK != (ret = topic_config->GetHandleIDRank(handle_id, handle_id_rank))) {
                QLErr("GetHandleIDRank ret %d", as_integer(ret));
                continue;
            }
        }

		std::set<int> consumer_group_ids;
        if (comm::RetCode::RET_OK != (ret = topic_config->GetConsumerGroupIDsByPubID(pub_id, consumer_group_ids))) {
            QLErr("GetConsumerGroupIDsByPubID ret %d pub_id %d", as_integer(ret), pub_id);
            continue;
        }

        {
            bool consumer_group_id_found = (consumer_group_ids.find(consumer_group_id) == consumer_group_ids.end()) ? false : true;
            QLVerb("consumer_group_id_found %d", (int)consumer_group_id_found);
            if (!consumer_group_id_found) continue;
        }

        set<int> valid_store_ids;
        {
            for (auto &&store_id : store_ids) {
                set<int> pub_ids;
                if (comm::RetCode::RET_OK != (ret = config::utils::GetPubIDsByStoreID(impl_->topic_id, store_id, pub_ids))) {
                    QLErr("GetPubIDsByStoreID ret %d topic_id %d store_id %d", as_integer(ret), impl_->topic_id, store_id);
                    continue;
                }
                if (pub_ids.end() != pub_ids.find(pub_id)) {
                    valid_store_ids.insert(store_id);
                }
            }
        }
        QLVerb("valid_store_ids.size %d", valid_store_ids.size());
        if (valid_store_ids.empty()) continue;

        set<int> valid_queue_ids;
        {
            if (comm::RetCode::RET_OK != (ret = topic_config->GetQueuesByQueueInfoID(queue_info_id, valid_queue_ids))) {
                QLErr("GetQueuesByQueueInfoID ret %d queue_info_id %d", as_integer(ret), queue_info_id);
            }
        }
        QLVerb("valid_queue_ids.size %d", valid_queue_ids.size());
        if (valid_queue_ids.empty()) continue;

        int proc_tot = valid_store_ids.size() * valid_queue_ids.size();
        QLVerb("proc_tot %d", proc_tot);
        if (!proc_tot) continue;

        int proc_used = 0;
        int nget = 0, nhandle_tot = 0, nhandle = 0;
        for (int vpid{0}; vpid < opt->nprocs; ++vpid) {
            auto &&queue = queues[vpid];
            if (!(queue.magic &&
                  queue.consumer_group_id == consumer_group_id &&
                  valid_store_ids.end() != valid_store_ids.find(queue.store_id) &&
                  valid_queue_ids.end() != valid_queue_ids.find(queue.queue_id))) continue;

            auto &&consume_stat = impl_->shm->consume_stats[vpid];
            ++proc_used;
            nget += consume_stat.nget;
            nhandle_tot += consume_stat.nhandle_tot;
            if (-1 == handle_id_rank) nhandle += consume_stat.nhandle_tot;
            else nhandle += consume_stat.handle_id_rank2nhandle[handle_id_rank];
        }
        QLVerb("proc_used %d nget %d nhandle_tot %d nhandle %d", proc_used, nget, nhandle_tot, nhandle);
        if (!proc_used) continue;
        if (!nget) nget = 1;
        if (!nhandle_tot) nhandle_tot = 1;
        if (!nhandle) nhandle = 1;

        double limit_per_proc = (double)limit_per_min / 60 * diff_time_ms / 1000.0 / proc_tot;
        double handled_pct_per_proc = (double)nhandle / proc_used / limit_per_proc;
        double handle_rate = (double)nhandle / nhandle_tot;
        double nhandle_limit = limit_per_proc / handle_rate;

        QLInfo("pub_id %d consumer_group_id %d queue_info_id %d "
               "proc_tot %d proc_used %d nget %d nhandle_tot %d handle_id %d nhandle %d handle_rate %.4lf nhandle_limit %.2lf limit_per_proc %.2lf handled_pct_per_proc %.4lf",
               pub_id, consumer_group_id, queue_info_id, proc_tot, proc_used,
               nget, nhandle_tot,
               handle_id, nhandle, handle_rate, nhandle_limit,
               limit_per_proc, handled_pct_per_proc);


        auto key = make_tuple(pub_id, consumer_group_id, queue_info_id);
        auto &&it = pub_consumer_group_queue_info_id2max_handle_pct.find(key);
        if (pub_consumer_group_queue_info_id2max_handle_pct.end() != it && handled_pct_per_proc < it->second) continue;
        pub_consumer_group_queue_info_id2max_handle_pct[key] = handled_pct_per_proc;

        {
            nrefill = 1;
            refill_interval_ms = diff_time_ms / (limit_per_proc / (nrefill * handle_rate));
            if (refill_interval_ms == 0) {
                refill_interval_ms = 1;
                nrefill = (double)refill_interval_ms * limit_per_proc / diff_time_ms / handle_rate;
            }

            QLInfo("nrefill %d refill_interval_ms %d", nrefill, refill_interval_ms);

            /********************************
            const int batch_min = 1; // minimal batchget


            int l = batch_min;
            int r = batch_limit;
            if (l > r) l = r;
            while (l <= r) {
                nrefill = (l + r) / 2;
                refill_interval_ms = diff_time_ms / (limit_per_proc / (nrefill * handle_rate));
                if (refill_interval_ms > max_refill_interval_ms_on_freq_limit) {
                    r = nrefill - 1;
                } else {
                    l = nrefill + 1;
                }
            }
            *********************************/

            /********************************
              nrefill = nhandle_tot / nget;
              if (nrefill < batch_min) nrefill = batch_min;
              if (nrefill > batch_limit) nrefill = batch_limit;
              refill_interval_ms = diff_time_ms / (limit_per_proc / (nrefill * handle_rate));

            *********************************/


            for (int vpid{0}; vpid < opt->nprocs; ++vpid) {
                auto &&queue = queues[vpid];
                if (!(queue.magic &&
                      queue.consumer_group_id == consumer_group_id &&
                      valid_store_ids.end() != valid_store_ids.find(queue.store_id) &&
                      valid_queue_ids.end() != valid_queue_ids.find(queue.queue_id))) continue;

                auto &&consume_stat = impl_->shm->consume_stats[vpid];
                auto handle_id_rank2nhandle = consume_stat.handle_id_rank2nhandle[handle_id_rank];
                double prop = handle_id_rank2nhandle / ((double)nhandle / proc_used);
                if (prop < 0.1) prop = 0.1;
                if (!handle_id_rank2nhandle) prop = 1;

                auto &&limit_info = impl_->shm->limit_infos[vpid];
                updated[vpid] = true;
                int nhandle_limit_tmp = nhandle_limit * prop;
                limit_info.nhandle_limit = (nhandle_limit_tmp ? nhandle_limit_tmp : 1);
                limit_info.nrefill = nrefill;
                limit_info.refill_interval_ms = refill_interval_ms / prop;
                if (!limit_info.refill_interval_ms) limit_info.refill_interval_ms = 1;

                QLInfo("consumer_group_id %d store_id %d queue_id %d nhandle_limit %d nrefill %d refill_interval_ms %d",
                       queue.consumer_group_id, queue.store_id, queue.queue_id,
                       limit_info.nhandle_limit, limit_info.nrefill, limit_info.refill_interval_ms);
            }
        }
    }

    for (int vpid{0}; vpid < opt->nprocs; ++vpid) {
        if (updated[vpid]) continue;
        auto &&limit_info = impl_->shm->limit_infos[vpid];
        limit_info.nhandle_limit = 0;
        limit_info.nrefill = 0;
        limit_info.refill_interval_ms = 0;
    }

    return comm::RetCode::RET_OK;
}

void FreqMan::Judge(const int vpid, bool &need_block, bool &need_freqlimit,
                    int &nrefill, int &refill_interval_ms) {
    need_block = need_freqlimit = false;
    nrefill = refill_interval_ms = 0;

    if (vpid > MAX_VPID) {
        QLErr("vpid(%d) > MAX_VPID(%d)", vpid, MAX_VPID);
        return;
    }
    auto &&limit_info = impl_->shm->limit_infos[vpid];
    auto &&consume_stat = impl_->shm->consume_stats[vpid];

    int nhandle_limit = limit_info.nhandle_limit;
    int nhandle_tot = consume_stat.nhandle_tot;

    if (!nhandle_limit) return;

    need_freqlimit = true;
    nrefill = limit_info.nrefill;
    refill_interval_ms = limit_info.refill_interval_ms;

    if (nhandle_tot > nhandle_limit) {
        need_block = true;
        return;
    }
}


}  // namespace consumer

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

