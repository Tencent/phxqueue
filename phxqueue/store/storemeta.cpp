/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/store/storemeta.h"

#include <mutex>
#include <set>

#include <zlib.h>

#include "phxqueue/comm.h"


namespace phxqueue {

namespace store {


using namespace std;

class StoreMeta::StoreMetaImpl {
  public:
    StoreMetaImpl() {}
    virtual ~StoreMetaImpl() {}

    uint64_t cursor_id{UINT64_MAX};
    uint64_t idx{0};
    uint32_t crc{0};
};

StoreMeta::StoreMeta() : impl_(new StoreMetaImpl) {}


StoreMeta::StoreMeta(uint64_t cursor_id) : impl_(new StoreMetaImpl) {
    impl_->cursor_id = cursor_id;
}

StoreMeta::~StoreMeta() {}

StoreMeta::StoreMeta(const StoreMeta &other) : impl_(new StoreMetaImpl) {
    impl_->cursor_id = other.impl_->cursor_id;
    impl_->idx = other.impl_->idx;
    impl_->crc = other.impl_->crc;
}

StoreMeta & StoreMeta::operator=(const StoreMeta &other) {
    impl_->cursor_id = other.impl_->cursor_id;
    impl_->idx = other.impl_->idx;
    impl_->crc = other.impl_->crc;
    return *this;
}


StoreMeta::StoreMeta(StoreMeta &&other) : impl_(new StoreMetaImpl) {
    impl_->cursor_id = other.GetCursorID();
    impl_->idx = other.GetIndex();
    impl_->crc = other.GetCrc();
}

StoreMeta &StoreMeta::operator=(StoreMeta &&other) {
    impl_->cursor_id = other.GetCursorID();
    impl_->idx = other.GetIndex();
    impl_->crc = other.GetCrc();
    return *this;
}

uint64_t StoreMeta::GetCursorID() const {
    return impl_->cursor_id;
}

bool StoreMeta::operator<(const StoreMeta &other) const {
    return impl_->cursor_id < other.GetCursorID();
};

void StoreMeta::SetIndex(const int idx) {
    impl_->idx = idx;
}

int StoreMeta::GetIndex() const {
    return impl_->idx;
}

void StoreMeta::UpdateCrc(uint32_t pre_crc) {
    if (!impl_->crc)
        impl_->crc = crc32(pre_crc, (const unsigned char *)&impl_->cursor_id, sizeof(uint64_t));
}

uint32_t StoreMeta::GetCrc() const{
    return impl_->crc;
}

bool StoreMeta::CheckCrc(uint32_t pre_crc) const {
    auto tmp_crc = crc32(pre_crc, (const unsigned char *)&impl_->cursor_id, sizeof(uint64_t));
    return impl_->crc == tmp_crc;
}


class StoreMetaQueue::StoreMetaQueueImpl {
  public:
    StoreMetaQueueImpl() {}
    virtual ~StoreMetaQueueImpl() {}

    mutex lock;
    set<StoreMeta> metas;
    StoreMeta last_dequeue_meta;  // for cal crc
};


StoreMetaQueue::StoreMetaQueue() : impl_(new StoreMetaQueueImpl()) {}

StoreMetaQueue::~StoreMetaQueue() {}

bool StoreMetaQueue::Next(const StoreMeta &meta, StoreMeta &next_meta, bool &crc_chk_pass) {
    lock_guard<mutex> lock_guard(impl_->lock);

    crc_chk_pass = true;

    if (impl_->metas.empty()) return false;

    set<StoreMeta>::iterator it_next = impl_->metas.end();

    if (-1 == meta.GetCursorID()) {
        it_next = impl_->metas.begin();
    } else {
        it_next = impl_->metas.upper_bound(meta);
        if (impl_->metas.end() == it_next) return false;
    }

    next_meta = *it_next;

    if (-1 != impl_->last_dequeue_meta.GetCursorID() && impl_->last_dequeue_meta < next_meta) {
        crc_chk_pass = next_meta.CheckCrc(impl_->last_dequeue_meta.GetCrc());
        if (!crc_chk_pass) {
            QLErr("CheckCrc fail. next_meta(idx %d cursor_id %" PRIu64 " crc %u) "
                  "last_dequeue_meta(idc %d cursor_id %" PRIu64 " crc %u",
                  next_meta.GetIndex(), next_meta.GetCursorID(), next_meta.GetCrc(),
                  impl_->last_dequeue_meta.GetIndex(), impl_->last_dequeue_meta.GetCursorID(),
                  impl_->last_dequeue_meta.GetCrc());
        }
    }

    if (-1 == impl_->last_dequeue_meta.GetCursorID() || impl_->last_dequeue_meta < next_meta) {
        impl_->last_dequeue_meta = next_meta;
    }

    return true;
}

bool StoreMetaQueue::PushBack(StoreMeta &&meta) {
    lock_guard<mutex> lock_guard(impl_->lock);

    if (-1 == meta.GetCursorID()) return false;
    if (!impl_->metas.empty() && !((*impl_->metas.rbegin()) < meta)) return false;
    if (!impl_->metas.empty()) {
        auto &&pre_meta = *impl_->metas.rbegin();
        meta.UpdateCrc(pre_meta.GetCrc());
        meta.SetIndex(pre_meta.GetIndex() + 1);
    } else {
        meta.UpdateCrc(impl_->last_dequeue_meta.GetCrc());
        meta.SetIndex(0);
    }
    impl_->metas.insert(forward<StoreMeta>(meta));
    return true;
}


bool StoreMetaQueue::Front(StoreMeta &meta) {
    lock_guard<mutex> lock_guard(impl_->lock);

    if (impl_->metas.empty()) return false;
    meta = *impl_->metas.begin();
    return true;
}

bool StoreMetaQueue::Back(StoreMeta &meta) {
    lock_guard<mutex> lock_guard(impl_->lock);

    if (impl_->metas.empty()) return false;
    meta = *impl_->metas.rbegin();
    return true;
}

void StoreMetaQueue::EraseFrontTill(const StoreMeta &meta) {
    lock_guard<mutex> lock_guard(impl_->lock);

    if (-1 == meta.GetCursorID()) return;
    if (impl_->metas.empty()) return;

    auto &&it = impl_->metas.upper_bound(meta);
    if (it == impl_->metas.end()) impl_->metas.clear();
    else impl_->metas.erase(impl_->metas.begin(), it);
}

void StoreMetaQueue::EraseFrontUntill(const StoreMeta &meta) {
    lock_guard<mutex> lock_guard(impl_->lock);

    if (-1 == meta.GetCursorID()) return;
    if (impl_->metas.empty()) return;

    auto &&it = impl_->metas.lower_bound(meta);
    if (it == impl_->metas.end()) impl_->metas.clear();
    else impl_->metas.erase(impl_->metas.begin(), it);
}

bool StoreMetaQueue::LowerBound(const StoreMeta &meta, StoreMeta &lower_bound_meta) {
    lock_guard<mutex> lock_guard(impl_->lock);

    if (-1 == meta.GetCursorID()) return false;
    if (impl_->metas.empty()) return false;

    auto &&it = impl_->metas.lower_bound(meta);
    if (it == impl_->metas.end()) return false;
    else lower_bound_meta = *it;

    return true;
}

void StoreMetaQueue::UpdateDeuqueStat(const StoreMeta &meta) {
    lock_guard<mutex> lock_guard(impl_->lock);

    if (-1 == meta.GetCursorID()) return;

    auto &&it = impl_->metas.lower_bound(meta);
    if (it == impl_->metas.end()) return;
    if (-1 == impl_->last_dequeue_meta.GetCursorID() || impl_->last_dequeue_meta < *it) {
        impl_->last_dequeue_meta = *it;
    }
}

int StoreMetaQueue::Size() {
    lock_guard<mutex> lock_guard(impl_->lock);

    return impl_->metas.size();
}

int StoreMetaQueue::SizeGT(const StoreMeta &meta) {
    lock_guard<mutex> lock_guard(impl_->lock);

    auto &&it = impl_->metas.upper_bound(meta);
    if (it == impl_->metas.end()) return 0;
    return impl_->metas.rbegin()->GetIndex() - it->GetIndex() + 1;
}


void StoreMetaQueue::Clear() {
    lock_guard<mutex> lock_guard(impl_->lock);

    if (impl_->metas.empty()) return;

    impl_->metas.clear();
}

void StoreMetaQueue::ClearDequeueStat() {
    lock_guard<mutex> lock_guard(impl_->lock);

    if (-1 != impl_->last_dequeue_meta.GetCursorID()) {
        impl_->last_dequeue_meta = StoreMeta();
    }

}


}  // namespace store

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

