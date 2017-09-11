/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/plugin/breakpointfactory.h"

#include <cassert>

#include "phxqueue/comm.h"


namespace phxqueue {

namespace plugin {


using namespace std;


BreakPointFactoryCreateFunc BreakPointFactory::break_point_factory_create_func_ = nullptr;

BreakPointFactory *BreakPointFactory::GetInstance() {
    static BreakPointFactory *bpf = nullptr;

    if (!bpf) {
        if (break_point_factory_create_func_) bpf = break_point_factory_create_func_().release();
        else bpf = new BreakPointFactory();
    }

    assert(bpf);
    return bpf;
}

unique_ptr<comm::ConsumerBP> BreakPointFactory::NewConsumerBP() {
    return unique_ptr<comm::ConsumerBP>(new comm::ConsumerBP());
}

unique_ptr<comm::ConsumerConsumeBP> BreakPointFactory::NewConsumerConsumeBP() {
    return unique_ptr<comm::ConsumerConsumeBP>(new comm::ConsumerConsumeBP());
}

unique_ptr<comm::ConsumerHeartBeatLockBP> BreakPointFactory::NewConsumerHeartBeatLockBP() {
    return unique_ptr<comm::ConsumerHeartBeatLockBP>(new comm::ConsumerHeartBeatLockBP());
}

unique_ptr<comm::StoreBP> BreakPointFactory::NewStoreBP() {
    return unique_ptr<comm::StoreBP>(new comm::StoreBP());
}

unique_ptr<comm::StoreBaseMgrBP> BreakPointFactory::NewStoreBaseMgrBP() {
    return unique_ptr<comm::StoreBaseMgrBP>(new comm::StoreBaseMgrBP());
}

unique_ptr<comm::StoreIMMasterBP> BreakPointFactory::NewStoreIMMasterBP() {
    return unique_ptr<comm::StoreIMMasterBP>(new comm::StoreIMMasterBP());
}

unique_ptr<comm::StoreSnatchMasterBP> BreakPointFactory::NewStoreSnatchMasterBP() {
    return unique_ptr<comm::StoreSnatchMasterBP>(new comm::StoreSnatchMasterBP());
}

unique_ptr<comm::StoreBacklogBP> BreakPointFactory::NewStoreBacklogBP() {
    return unique_ptr<comm::StoreBacklogBP>(new comm::StoreBacklogBP());
}

unique_ptr<comm::StoreSMBP> BreakPointFactory::NewStoreSMBP() {
    return unique_ptr<comm::StoreSMBP>(new comm::StoreSMBP());
}

unique_ptr<comm::ProducerBP> BreakPointFactory::NewProducerBP() {
    return unique_ptr<comm::ProducerBP>(new comm::ProducerBP());
}

unique_ptr<comm::ProducerSubBP> BreakPointFactory::NewProducerSubBP() {
    return unique_ptr<comm::ProducerSubBP>(new comm::ProducerSubBP());
}


unique_ptr<comm::SchedulerBP> BreakPointFactory::NewSchedulerBP() {
    return unique_ptr<comm::SchedulerBP>(new comm::SchedulerBP());
}


unique_ptr<comm::SchedulerMgrBP> BreakPointFactory::NewSchedulerMgrBP() {
    return unique_ptr<comm::SchedulerMgrBP>(new comm::SchedulerMgrBP());
}

unique_ptr<comm::SchedulerLoadBalanceBP> BreakPointFactory::NewSchedulerLoadBalanceBP() {
    return unique_ptr<comm::SchedulerLoadBalanceBP>(new comm::SchedulerLoadBalanceBP());
}

unique_ptr<comm::SchedulerKeepMasterBP> BreakPointFactory::NewSchedulerKeepMasterBP() {
    return unique_ptr<comm::SchedulerKeepMasterBP>(new comm::SchedulerKeepMasterBP());
}

unique_ptr<comm::LockBP> BreakPointFactory::NewLockBP() {
    return unique_ptr<comm::LockBP>(new comm::LockBP());
}


unique_ptr<comm::LockMgrBP> BreakPointFactory::NewLockMgrBP() {
    return unique_ptr<comm::LockMgrBP>(new comm::LockMgrBP());
}


unique_ptr<comm::LockDbBP> BreakPointFactory::NewLockDbBP() {
    return unique_ptr<comm::LockDbBP>(new comm::LockDbBP());
}


unique_ptr<comm::LockCleanThreadBP> BreakPointFactory::NewLockCleanThreadBP() {
    return unique_ptr<comm::LockCleanThreadBP>(new comm::LockCleanThreadBP());
}


unique_ptr<comm::LockKeepMasterThreadBP> BreakPointFactory::NewLockKeepMasterThreadBP() {
    return unique_ptr<comm::LockKeepMasterThreadBP>(new comm::LockKeepMasterThreadBP());
}


unique_ptr<comm::LockIMMasterBP> BreakPointFactory::NewLockIMMasterBP() {
    return unique_ptr<comm::LockIMMasterBP>(new comm::LockIMMasterBP());
}


unique_ptr<comm::LockSnatchMasterBP> BreakPointFactory::NewLockSnatchMasterBP() {
    return unique_ptr<comm::LockSnatchMasterBP>(new comm::LockSnatchMasterBP());
}


unique_ptr<comm::LockSMBP> BreakPointFactory::NewLockSMBP() {
    return unique_ptr<comm::LockSMBP>(new comm::LockSMBP());
}


}  // namespace plugin

}  // namespace phxqueue

