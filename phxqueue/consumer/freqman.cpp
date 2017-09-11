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
    int nhandle_per_get_recommand;
    int sleep_ms_per_get_recommand;
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

    impl_->last_update_time = time(nullptr);

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
    ++consume_stat.nhandle_tot += items.size();

    for (auto &&item : items) {
        if (impl_->consumer->SkipHandle(cc, *item)) continue;

        int rank;
        if (comm::RetCode::RET_OK != (ret = topic_config->GetHandleIDRank(item->meta().handle_id(), rank))) {
            QLErr("GetHandleIDRank ret %d handle_id %d", as_integer(ret), item->meta().handle_id());
            continue;
        }
        if (rank > MAX_HANDLE_ID_NUM) {
            QLErr("handle_id %d rank(%d) > MAX_HANDLE_ID_NUM(%d)", item->meta().handle_id(), rank, MAX_HANDLE_ID_NUM);
            continue;
        }
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
    uint64_t now = time(nullptr);
    uint64_t diff_time_s = now - impl_->last_update_time;
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

    map<tuple<int, int, int>, double> pub_sub_queue_info_id2max_handle_pct;
    for (auto &&freq_info : freq_infos) {
        auto pub_id = freq_info->pub_id();
        auto sub_id = freq_info->sub_id();
        auto handle_id = freq_info->handle_id();
        auto limit_per_min = freq_info->limit_per_min();
        auto queue_info_id = freq_info->queue_info_id();

        QLVerb("pub_id %d sub_id %d handle_id %d limit_per_min %d", pub_id, sub_id, handle_id, limit_per_min);

        if (!limit_per_min) continue;

        int nhandle_per_get_recommand = 0;
        int sleep_ms_per_get_recommand = 0;

        int handle_id_rank = -1;
        if (-1 != handle_id) {
            if (comm::RetCode::RET_OK != (ret = topic_config->GetHandleIDRank(handle_id, handle_id_rank))) {
                QLErr("GetHandleIDRank ret %d", as_integer(ret));
                continue;
            }
        }

        shared_ptr<const config::proto::Pub> pub;
        if (comm::RetCode::RET_OK != (ret = topic_config->GetPubByPubID(pub_id, pub))) {
            QLErr("GetPubByPubID ret %d pub_id %d", as_integer(ret), pub_id);
            continue;
        }

        {
            bool sub_id_found = false;
            for (int i{0}; i < pub->sub_ids_size(); ++i) {
                if (pub->sub_ids(i) == sub_id) {
                    sub_id_found = true;
                    break;
                }
            }
            QLVerb("sub_id_found %d", (int)sub_id_found);
            if (!sub_id_found) continue;
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
                  queue.sub_id == sub_id &&
                  valid_store_ids.end() != valid_store_ids.find(queue.store_id) &&
                  valid_queue_ids.end() != valid_queue_ids.find(queue.queue_id))) continue;

            auto &&consume_stat = impl_->shm->consume_stats[vpid];
            ++proc_used;
            nget += consume_stat.nget;
            nhandle_tot += consume_stat.nhandle_tot;
            if (-1 == handle_id_rank) nhandle += consume_stat.nhandle_tot;
            else nhandle += consume_stat.handle_id_rank2nhandle[handle_id_rank];
        }
        QLVerb("proc_used %d nget nhandle_tot nhandle", proc_used, nget, nhandle_tot, nhandle);
        if (!proc_used || !nget || !nhandle_tot || !nhandle) continue;

        int limit_per_proc = (int)((double)limit_per_min / 60 * diff_time_s / proc_tot);
        double handled_pct_per_proc = (double)nhandle / proc_used / limit_per_proc;
        double handle_rate = (double)nhandle / nhandle_tot;
        int nhandle_limit = limit_per_proc / handle_rate;
        if (!nhandle_limit) nhandle_limit = 1;

        QLInfo("pub_id %d sub_id %d queue_info_id %d "
               "proc_tot %d proc_used %d nget %d nhandle_tot %d handle_id %d nhandle %d handle_rate %.4lf nhandle_limit %d limit_per_proc %d handled_pct_per_proc %.4lf",
               pub_id, sub_id, queue_info_id, proc_tot, proc_used,
               nget, nhandle_tot,
               handle_id, nhandle, handle_rate, nhandle_limit,
               limit_per_proc, handled_pct_per_proc);


        auto key = make_tuple(pub_id, sub_id, queue_info_id);
        auto &&it = pub_sub_queue_info_id2max_handle_pct.find(key);
        if (pub_sub_queue_info_id2max_handle_pct.end() != it && handled_pct_per_proc < it->second) continue;
        pub_sub_queue_info_id2max_handle_pct[key] = handled_pct_per_proc;

        {
            const int batch_min = 10; // minimal batchget


            int l = batch_min;
            int r = batch_limit;
            if (l > r) l = r;
            while (l <= r) {
                nhandle_per_get_recommand = (l + r) / 2;
                sleep_ms_per_get_recommand = 1000.0 * diff_time_s / (limit_per_proc / (nhandle_per_get_recommand * handle_rate));
                if (sleep_ms_per_get_recommand > 500) {
                    r = nhandle_per_get_recommand - 1;
                } else {
                    l = nhandle_per_get_recommand + 1;
                }
            }

            /*
              nhandle_per_get_recommand = nhandle_tot / nget;
              if (nhandle_per_get_recommand < batch_min) nhandle_per_get_recommand = batch_min;
              if (nhandle_per_get_recommand > batch_limit) nhandle_per_get_recommand = batch_limit;
              sleep_ms_per_get_recommand = 1000.0 * diff_time_s / (limit_per_proc / (nhandle_per_get_recommand * handle_rate));
            */

            if (nhandle_per_get_recommand == batch_min && sleep_ms_per_get_recommand > 500) sleep_ms_per_get_recommand = 500;

            for (int vpid{0}; vpid < opt->nprocs; ++vpid) {
                auto &&queue = queues[vpid];
                if (!(queue.magic &&
                      queue.sub_id == sub_id &&
                      valid_store_ids.end() != valid_store_ids.find(queue.store_id) &&
                      valid_queue_ids.end() != valid_queue_ids.find(queue.queue_id))) continue;

                auto &&limit_info = impl_->shm->limit_infos[vpid];
                updated[vpid] = true;
                limit_info.nhandle_limit = nhandle_limit;
                limit_info.nhandle_per_get_recommand = nhandle_per_get_recommand;
                limit_info.sleep_ms_per_get_recommand = sleep_ms_per_get_recommand;
            }
        }
    }

    for (int vpid{0}; vpid < opt->nprocs; ++vpid) {
        if (updated[vpid]) continue;
        auto &&limit_info = impl_->shm->limit_infos[vpid];
        limit_info.nhandle_limit = 0;
        limit_info.nhandle_per_get_recommand = 0;
        limit_info.sleep_ms_per_get_recommand = 0;
    }

    return comm::RetCode::RET_OK;
}

void FreqMan::Judge(const int vpid, bool &need_block, bool &need_freqlimit,
                    int &nhandle_per_get_recommand, int &sleep_ms_per_get_recommand) {
    need_block = need_freqlimit = false;
    nhandle_per_get_recommand = sleep_ms_per_get_recommand = 0;

    if (vpid > MAX_VPID) {
        QLErr("vpid(%d) > MAX_VPID(%d)", vpid, MAX_VPID);
        return;
    }
    auto &&limit_info = impl_->shm->limit_infos[vpid];
    auto &&consume_stat = impl_->shm->consume_stats[vpid];

    int nhandle_limit = limit_info.nhandle_limit;
    int nhandle_tot = consume_stat.nhandle_tot;

    if (!nhandle_limit) return;

    if (nhandle_tot >= nhandle_limit) {
        need_block = true;
        return;
    }

    if (!nhandle_limit) {
        return;
    }
    need_freqlimit = true;

    nhandle_per_get_recommand = limit_info.nhandle_per_get_recommand;
    sleep_ms_per_get_recommand = limit_info.sleep_ms_per_get_recommand;

    if (nhandle_per_get_recommand > nhandle_limit - nhandle_tot) {
        nhandle_per_get_recommand = nhandle_limit - nhandle_tot;
    }
}


}  // namespace consumer

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL: http://scm-gy.tencent.com/gzrd/gzrd_mail_rep/phoenix_proj/trunk/phoenix/phxqueue/lib/consumer/freq.cpp $ $Id: freq.cpp 1816455 2016-11-09 14:31:50Z unixliang $ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

