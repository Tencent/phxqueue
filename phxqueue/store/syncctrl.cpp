/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/store/syncctrl.h"

#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <mutex>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "phxpaxos/node.h"

#include "phxqueue/comm.h"
#include "phxqueue/config.h"

#include "phxqueue/store/store.h"
#include "phxqueue/store/basemgr.h"
#include "phxqueue/store/storemeta.h"
#include "phxqueue/store/proto/store.pb.h"


namespace phxqueue {

namespace store {


using namespace std;


#define SYNCCTRL_MAGIC 9980256


struct SyncCtrlItem_t {
    uint32_t prev_magic;
    uint32_t next_magic;
    uint64_t prev_cursor_id;
    uint64_t next_cursor_id;
    char reserved[104];
};


class SyncCtrl::SyncCtrlImpl {
  public:
    SyncCtrlImpl() {}
    virtual ~SyncCtrlImpl() {}
  public:
    Store *store{nullptr};

    SyncCtrlItem_t *buf{nullptr};
    int buf_size{0};

    std::unique_ptr<mutex[]> locks;
};


SyncCtrl::SyncCtrl(Store *const store) : impl_(new SyncCtrlImpl()) {
    assert(impl_);
    assert(store);

    impl_->store = store;
}

SyncCtrl::~SyncCtrl() {
    if (impl_->buf) munmap(impl_->buf, impl_->buf_size);
}

comm::RetCode SyncCtrl::Init() {
    auto opt(impl_->store->GetStoreOption());

    impl_->buf_size = opt->nconsumer_group * opt->nqueue * sizeof (SyncCtrlItem_t);

    auto sync_path(opt->data_dir_path + "/sync");
    int fd = open(sync_path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        QLErr("open err %s path %s", strerror(errno), sync_path.c_str());
        return comm::RetCode::RET_ERR_SYS;
    }

    if (ftruncate(fd, (off_t)impl_->buf_size) < 0) {
        QLErr("ftruncate err %s", strerror(errno));
        close(fd);
        return comm::RetCode::RET_ERR_SYS;
    }

    void *pa{mmap(nullptr, impl_->buf_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)};

    close(fd);

    if ((char *)pa == MAP_FAILED) {
        QLErr("mmap err %s", strerror(errno));
        return comm::RetCode::RET_ERR_SYS;
    }

    impl_->buf = (SyncCtrlItem_t*)pa;

    impl_->locks.reset(new mutex[opt->nconsumer_group * opt->nqueue]);

    return comm::RetCode::RET_OK;;
}

static inline void GetIdx(const int nconsumer_group, const int consumer_group_id, const int queue_id, size_t &idx) {
    idx = queue_id * nconsumer_group + consumer_group_id - 1;
}

comm::RetCode SyncCtrl::AdjustNextCursorID(const int consumer_group_id, const int queue_id,
                                           uint64_t &prev_cursor_id, uint64_t &next_cursor_id) {
    auto opt = impl_->store->GetStoreOption();

    if (!consumer_group_id || consumer_group_id > opt->nconsumer_group || queue_id >= opt->nqueue)
        return comm::RetCode::RET_ERR_ARG;

    size_t idx;
    GetIdx(opt->nconsumer_group, consumer_group_id, queue_id, idx);

    std::lock_guard<mutex> lock_guard(impl_->locks[idx]);

    auto &&item = impl_->buf[idx];

    if (item.next_magic != SYNCCTRL_MAGIC || item.next_cursor_id != next_cursor_id ||
        (-1 != prev_cursor_id && prev_cursor_id > next_cursor_id)) {
        if (item.prev_magic != SYNCCTRL_MAGIC) return comm::RetCode::RET_ERR_CURSOR_NOT_FOUND;
        prev_cursor_id = next_cursor_id = item.prev_cursor_id;
    }

    if (item.prev_magic == SYNCCTRL_MAGIC &&
        (prev_cursor_id == -1 || item.prev_cursor_id > prev_cursor_id)) {
        prev_cursor_id = item.prev_cursor_id;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode SyncCtrl::UpdateCursorID(const int consumer_group_id, const int queue_id,
                                       const uint64_t cursor_id, const bool is_prev) {
    auto opt = impl_->store->GetStoreOption();

    if (!consumer_group_id || consumer_group_id > opt->nconsumer_group || queue_id >= opt->nqueue)
        return comm::RetCode::RET_ERR_ARG;

    size_t idx;
    GetIdx(opt->nconsumer_group, consumer_group_id, queue_id, idx);

    lock_guard<mutex> lock_guard(impl_->locks[idx]);

    auto &&item = impl_->buf[idx];

    if (is_prev) {
        if (item.prev_magic != SYNCCTRL_MAGIC) item.prev_magic = SYNCCTRL_MAGIC;
        item.prev_cursor_id = cursor_id;
    } else {
        if (item.next_magic != SYNCCTRL_MAGIC) item.next_magic = SYNCCTRL_MAGIC;
        item.next_cursor_id = cursor_id;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode SyncCtrl::GetCursorID(const int consumer_group_id, const int queue_id,
                                    uint64_t &cursor_id, const bool is_prev) const {
    cursor_id = -1;

    auto opt = impl_->store->GetStoreOption();

    if (!consumer_group_id || consumer_group_id > opt->nconsumer_group || queue_id >= opt->nqueue)
        return comm::RetCode::RET_ERR_ARG;

    size_t idx;
    GetIdx(opt->nconsumer_group, consumer_group_id, queue_id, idx);

    lock_guard<mutex> lock_guard(impl_->locks[idx]);

    auto &&item(impl_->buf[idx]);

    if (is_prev) {
        if (item.prev_magic != SYNCCTRL_MAGIC) return comm::RetCode::RET_ERR_CURSOR_NOT_FOUND;
        cursor_id = item.prev_cursor_id;
    } else {
        if (item.next_magic != SYNCCTRL_MAGIC) return comm::RetCode::RET_ERR_CURSOR_NOT_FOUND;
        cursor_id = item.next_cursor_id;
    }

    return comm::RetCode::RET_OK;
}

void SyncCtrl::ClearSyncCtrl() {
    if (!impl_->buf) {
        QLErr("impl_->buf null");
        return;
    }

    const int topic_id{impl_->store->GetTopicID()};

    comm::RetCode ret;
    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));
        return;
    }

    thread_local uint64_t topic_config_last_mod_time{0};
    auto tmp_last_mod_time = topic_config->GetLastModTime();
    if (topic_config_last_mod_time == tmp_last_mod_time) return;
    topic_config_last_mod_time = tmp_last_mod_time;

    auto opt(impl_->store->GetStoreOption());
    comm::proto::Addr addr;
    addr.set_ip(opt->ip);
    addr.set_port(opt->port);
    addr.set_paxos_port(opt->paxos_port);

    shared_ptr<const config::StoreConfig> store_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetStoreConfig(topic_id, store_config))) {
        QLErr("GetStoreConfig ret %d topic_id %d", as_integer(ret), topic_id);
        return;
    }

    int store_id;
    if (comm::RetCode::RET_OK != (ret = store_config->GetStoreIDByAddr(addr, store_id))) {
        QLErr("GetStoreIDByAddr ret %d", as_integer(ret));
        return;
    }

    set<int> pub_ids;
    if (comm::RetCode::RET_OK !=
        (ret = config::utils::GetPubIDsByStoreID(topic_id, store_id, pub_ids))) {
        QLErr("GetPubIDsByStoreID ret %d topic_id %d store_id %d",
              as_integer(ret), topic_id, store_id);
        return;
    }

    uint64_t cursor_id;
    for (int queue_id{0}; queue_id < opt->nqueue; ++queue_id) {
        for (int consumer_group_id{1}; consumer_group_id <= opt->nconsumer_group; ++consumer_group_id) {
            bool valid = false;
            for (auto &&pub_id : pub_ids) {
                if (topic_config->IsValidQueue(queue_id, pub_id, consumer_group_id)) {
                    valid = true;
                    break;
                }
            }

            bool skip = topic_config->QueueShouldSkip(queue_id, consumer_group_id);
            
            if (!valid || skip){
                if (comm::RetCode::RET_OK == (ret = GetCursorID(consumer_group_id, queue_id, cursor_id))) {
                    QLInfo("start to clear. consumer_group_id %d queue_id %d cursor_id %" PRIu64,
                           consumer_group_id, queue_id, cursor_id);
                    if (comm::RetCode::RET_OK != (ret = ClearCursorID(consumer_group_id, queue_id))) {
                        QLErr("ClearCursorID ret %d consumer_group_id %d queue_id %u",
                              ret, consumer_group_id, queue_id);
                    }
                }
            }
        }
    }
}


comm::RetCode SyncCtrl::ClearCursorID(const int consumer_group_id, const int queue_id) {
    auto opt = impl_->store->GetStoreOption();

    if (!consumer_group_id || consumer_group_id > opt->nconsumer_group || queue_id >= opt->nqueue)
        return comm::RetCode::RET_ERR_ARG;

    size_t idx;
    GetIdx(opt->nconsumer_group, consumer_group_id, queue_id, idx);

    lock_guard<mutex> lock_guard(impl_->locks[idx]);

    auto &&item(impl_->buf[idx]);
    memset(&item, 0, sizeof(item));

    return comm::RetCode::RET_OK;
}

comm::RetCode SyncCtrl::Flush(const int consumer_group_id, const int queue_id) {
    auto opt(impl_->store->GetStoreOption());

    if (!consumer_group_id || consumer_group_id > opt->nconsumer_group || queue_id >= opt->nqueue)
        return comm::RetCode::RET_ERR_ARG;

    size_t idx;
    GetIdx(opt->nconsumer_group, consumer_group_id, queue_id, idx);

    std::lock_guard<mutex> lock_guard(impl_->locks[idx]);

    auto &&item(impl_->buf[idx]);
    if (0 != msync(&item, ((size_t)(&item.reserved[0]) - (size_t)(&item)), MS_SYNC)) {
        QLErr("msync err %s", strerror(errno));
        return comm::RetCode::RET_ERR_SYS;
    }
    return comm::RetCode::RET_OK;
}

comm::RetCode SyncCtrl::GetBackLogByCursorID(const int queue_id,
                                             const uint64_t cursor_id, int &backlog) {
    backlog = 0;

    auto meta_queue(impl_->store->GetBaseMgr()->GetMetaQueue(queue_id));
    if (!meta_queue) {
        QLErr("GetMetaQueue fail. queue_id %d", queue_id);
        return comm::RetCode::RET_ERR_RANGE_QUEUE;
    }
    if (-1 == cursor_id) backlog = meta_queue->Size();
    else backlog = meta_queue->SizeGT(StoreMeta(cursor_id));

    return comm::RetCode::RET_OK;
}


comm::RetCode SyncCtrl::SyncCursorID(const proto::SyncCtrlInfo &sync_ctrl_info) {
    comm::RetCode ret;

    uint64_t cur_prev_cursor_id;

    for (size_t i{0}; i < sync_ctrl_info.queue_details_size(); ++i) {
        const proto::SyncCtrlInfo::QueueDetail &queue_detail = sync_ctrl_info.queue_details(i);
        int queue_id = queue_detail.queue_id();

        uint64_t max_prev_cursor_id = -1;
        for (size_t j{0}; j < queue_detail.consumer_group_details_size(); ++j) {
            const proto::SyncCtrlInfo::QueueDetail::ConsumerGroupDetail &
            consumer_group_detail(queue_detail.consumer_group_details(j));
            int consumer_group_id = consumer_group_detail.consumer_group_id();
            uint64_t prev_cursor_id = consumer_group_detail.prev_cursor_id();
            if (-1 == max_prev_cursor_id || prev_cursor_id > max_prev_cursor_id)
                max_prev_cursor_id = prev_cursor_id;

            if (0 > as_integer(ret = GetCursorID(consumer_group_id, queue_id, cur_prev_cursor_id))) {
                QLErr("GetCursorID ret %d consumer_group_id %d queue %d",
                      as_integer(ret), consumer_group_id, queue_id);
            } else if (!as_integer(ret) && cur_prev_cursor_id >= prev_cursor_id) {
                continue;
            }

            QLVerb("sync prev_cursor_id. consumer_group_id %d queue_id %d", consumer_group_id, queue_id);

            if (comm::RetCode::RET_OK !=
                (ret = UpdateCursorID(consumer_group_id, queue_id, prev_cursor_id))) {
                QLErr("UpdateCursorID ret %d consumer_group_id %d queue_id %d",
                      as_integer(ret), consumer_group_id, queue_id);
                return ret;
            }

            if (comm::RetCode::RET_OK !=
                (ret = UpdateCursorID(consumer_group_id, queue_id, prev_cursor_id, false))) {
                QLErr("UpdateCursorID ret %d consumer_group_id %d queue_id %d",
                      as_integer(ret), consumer_group_id, queue_id);
                return ret;
            }
        }

        auto meta_queue(impl_->store->GetBaseMgr()->GetMetaQueue(queue_id));
        if (-1 == max_prev_cursor_id) meta_queue->ClearDequeueStat();
        else meta_queue->UpdateDeuqueStat(StoreMeta(max_prev_cursor_id));
    }

    return comm::RetCode::RET_OK;
}


}  // namespace store

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

