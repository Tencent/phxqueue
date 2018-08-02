/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>

#include "phxqueue/comm.h"


namespace phxqueue {

namespace comm {


class ConsumerBP;
class ConsumerConsumeBP;
class ConsumerHeartBeatLockBP;

class StoreBP;
class StoreBaseMgrBP;
class StoreIMMasterBP;
class StoreSnatchMasterBP;
class StoreBacklogBP;
class StoreSMBP;

class ProducerBP;
class ProducerConsumerGroupBP;

class SchedulerBP;
class SchedulerMgrBP;
class SchedulerLoadBalanceBP;
class SchedulerKeepMasterBP;

class LockBP;
class LockMgrBP;
class LockDbBP;
class LockCleanThreadBP;
class LockKeepMasterThreadBP;
class LockIMMasterBP;
class LockSnatchMasterBP;
class LockSMBP;


} // namespace comm


namespace plugin {


class BreakPointFactory;

using BreakPointFactoryCreateFunc = std::function<std::unique_ptr<BreakPointFactory> ()>;


class BreakPointFactory {
  public:
    BreakPointFactory() {}
    virtual ~BreakPointFactory() {}

    static void SetBreakPointFactoryCreateFunc(BreakPointFactoryCreateFunc break_point_factory_create_func) {
        break_point_factory_create_func_ = break_point_factory_create_func;
    }

    static BreakPointFactory *GetInstance();

    virtual std::unique_ptr<comm::ConsumerBP> NewConsumerBP();
    virtual std::unique_ptr<comm::ConsumerConsumeBP> NewConsumerConsumeBP();
    virtual std::unique_ptr<comm::ConsumerHeartBeatLockBP> NewConsumerHeartBeatLockBP();

    virtual std::unique_ptr<comm::StoreBP> NewStoreBP();
    virtual std::unique_ptr<comm::StoreBaseMgrBP> NewStoreBaseMgrBP();
    virtual std::unique_ptr<comm::StoreIMMasterBP> NewStoreIMMasterBP();
    virtual std::unique_ptr<comm::StoreSnatchMasterBP> NewStoreSnatchMasterBP();
    virtual std::unique_ptr<comm::StoreBacklogBP> NewStoreBacklogBP();
    virtual std::unique_ptr<comm::StoreSMBP> NewStoreSMBP();

    virtual std::unique_ptr<comm::ProducerBP> NewProducerBP();
    virtual std::unique_ptr<comm::ProducerConsumerGroupBP> NewProducerConsumerGroupBP();

    virtual std::unique_ptr<comm::SchedulerBP> NewSchedulerBP();
    virtual std::unique_ptr<comm::SchedulerMgrBP> NewSchedulerMgrBP();
    virtual std::unique_ptr<comm::SchedulerLoadBalanceBP> NewSchedulerLoadBalanceBP();
    virtual std::unique_ptr<comm::SchedulerKeepMasterBP> NewSchedulerKeepMasterBP();

    virtual std::unique_ptr<comm::LockBP> NewLockBP();
    virtual std::unique_ptr<comm::LockMgrBP> NewLockMgrBP();
    virtual std::unique_ptr<comm::LockDbBP> NewLockDbBP();
    virtual std::unique_ptr<comm::LockCleanThreadBP> NewLockCleanThreadBP();
    virtual std::unique_ptr<comm::LockKeepMasterThreadBP> NewLockKeepMasterThreadBP();
    virtual std::unique_ptr<comm::LockIMMasterBP> NewLockIMMasterBP();
    virtual std::unique_ptr<comm::LockSnatchMasterBP> NewLockSnatchMasterBP();
    virtual std::unique_ptr<comm::LockSMBP> NewLockSMBP();

  private:
    static BreakPointFactoryCreateFunc break_point_factory_create_func_;
};


}  // namespace plugin

}  // namespace phxqueue

