/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <cinttypes>
#include <cstdio>
#include <memory>


namespace phxqueue {

namespace store {


class StoreMeta {
  public:
    StoreMeta();
    StoreMeta(uint64_t cursor_id);
    virtual ~StoreMeta();

    StoreMeta(const StoreMeta &other);
    StoreMeta &operator=(const StoreMeta &other);
    StoreMeta(StoreMeta &&other);
    StoreMeta &operator=(StoreMeta &&other);

    uint64_t GetCursorID() const;
    bool operator<(const StoreMeta &other) const;
    void SetIndex(const int idx);
    int GetIndex() const;
    void UpdateCrc(uint32_t pre_crc);
    uint32_t GetCrc() const;
    bool CheckCrc(uint32_t pre_crc) const;

  private:
    class StoreMetaImpl;
    std::unique_ptr<StoreMetaImpl> impl_;
};


class StoreMetaQueue {
  public:
    StoreMetaQueue();
    virtual ~StoreMetaQueue();
    StoreMetaQueue(const StoreMetaQueue &) = delete;
    StoreMetaQueue &operator=(const StoreMetaQueue &other) = delete;

    bool Next(const StoreMeta &meta, StoreMeta &next_meta, bool &crc_chk_pass);
    bool PushBack(StoreMeta &&meta);
    bool Front(StoreMeta &meta);
    bool Back(StoreMeta &meta);
    void EraseFrontTill(const StoreMeta &meta);
    void EraseFrontUntill(const StoreMeta &meta);
    bool LowerBound(const StoreMeta &meta, StoreMeta &lower_bound_meta);
    void UpdateDeuqueStat(const StoreMeta &meta);
    int Size();
    int SizeGT(const StoreMeta &meta);
    void Clear();
    void ClearDequeueStat();

  private:
    class StoreMetaQueueImpl;
    std::unique_ptr<StoreMetaQueueImpl> impl_;
};


}  // namespace store

}  // namespace phxqueue

