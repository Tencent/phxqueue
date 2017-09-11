/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <cinttypes>
#include <functional>
#include <memory>
#include <string>

#include "phxqueue/store/store.h"


namespace phxqueue {

namespace store {


class CheckPointStat {
  public:
    CheckPointStat();
    virtual ~CheckPointStat();

    comm::RetCode Init(const std::string &dir, const std::string &file);

    comm::RetCode GetCheckPoint(uint64_t &cp);

    comm::RetCode UpdateCheckPointAndFlush(const uint64_t cp);

    std::string GetDir() const;
    std::string GetFile() const;

  private:
    class CheckPointStatImpl;
    std::unique_ptr<CheckPointStatImpl> impl_;
};


class CheckPointStatMgr {
  public:
    CheckPointStatMgr(Store *const store);
    virtual ~CheckPointStatMgr();

    comm::RetCode Init();

    CheckPointStat *GetCheckPointStat(const int paxos_group_id);

  private:
    class CheckPointStatMgrImpl;
    std::unique_ptr<CheckPointStatMgrImpl> impl_;
};


}  // namespace store

}  // namespace phxqueue

