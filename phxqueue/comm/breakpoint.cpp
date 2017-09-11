/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/comm/breakpoint.h"

#include "phxqueue/plugin.h"


namespace phxqueue {

namespace comm {


ConsumerBP *ConsumerBP::GetThreadInstance() {
    static thread_local ConsumerBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewConsumerBP().release();
    }
    if (!bp) {
        bp = new ConsumerBP();
    }
    assert(bp);

    return bp;
}

ConsumerConsumeBP *ConsumerConsumeBP::GetThreadInstance() {
    static thread_local ConsumerConsumeBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewConsumerConsumeBP().release();
    }
    if (!bp) {
        bp = new ConsumerConsumeBP();
    }
    assert(bp);

    return bp;
}

ConsumerHeartBeatLockBP *ConsumerHeartBeatLockBP::GetThreadInstance() {
    static thread_local ConsumerHeartBeatLockBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewConsumerHeartBeatLockBP().release();
    }
    if (!bp) {
        bp = new ConsumerHeartBeatLockBP();
    }
    assert(bp);

    return bp;
}

StoreBP *StoreBP::GetThreadInstance() {
    static thread_local StoreBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewStoreBP().release();
    }
    if (!bp) {
        bp = new StoreBP();
    }
    assert(bp);

    return bp;
}

StoreBaseMgrBP *StoreBaseMgrBP::GetThreadInstance() {
    static thread_local StoreBaseMgrBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewStoreBaseMgrBP().release();
    }
    if (!bp) {
        bp = new StoreBaseMgrBP();
    }
    assert(bp);

    return bp;
}

StoreIMMasterBP *StoreIMMasterBP::GetThreadInstance() {
    static thread_local StoreIMMasterBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewStoreIMMasterBP().release();
    }
    if (!bp) {
        bp = new StoreIMMasterBP();
    }
    assert(bp);

    return bp;
}

StoreSnatchMasterBP *StoreSnatchMasterBP::GetThreadInstance() {
    static thread_local StoreSnatchMasterBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewStoreSnatchMasterBP().release();
    }
    if (!bp) {
        bp = new StoreSnatchMasterBP();
    }
    assert(bp);

    return bp;
}

StoreBacklogBP *StoreBacklogBP::GetThreadInstance() {
    static thread_local StoreBacklogBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewStoreBacklogBP().release();
    }
    if (!bp) {
        bp = new StoreBacklogBP();
    }
    assert(bp);

    return bp;
}

StoreSMBP *StoreSMBP::GetThreadInstance() {
    static thread_local StoreSMBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewStoreSMBP().release();
    }
    if (!bp) {
        bp = new StoreSMBP();
    }
    assert(bp);

    return bp;
}


ProducerBP *ProducerBP::GetThreadInstance() {
    static thread_local ProducerBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewProducerBP().release();
    }
    if (!bp) {
        bp = new ProducerBP();
    }
    assert(bp);

    return bp;
}

ProducerSubBP *ProducerSubBP::GetThreadInstance() {
    static thread_local ProducerSubBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewProducerSubBP().release();
    }
    if (!bp) {
        bp = new ProducerSubBP();
    }
    assert(bp);

    return bp;
}


SchedulerBP *SchedulerBP::GetThreadInstance() {
    static thread_local SchedulerBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewSchedulerBP().release();
    }
    if (!bp) {
        bp = new SchedulerBP();
    }
    assert(bp);

    return bp;
}

SchedulerMgrBP *SchedulerMgrBP::GetThreadInstance() {
    static thread_local SchedulerMgrBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewSchedulerMgrBP().release();
    }
    if (!bp) {
        bp = new SchedulerMgrBP();
    }
    assert(bp);

    return bp;
}

SchedulerLoadBalanceBP *SchedulerLoadBalanceBP::GetThreadInstance() {
    static thread_local SchedulerLoadBalanceBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewSchedulerLoadBalanceBP().release();
    }
    if (!bp) {
        bp = new SchedulerLoadBalanceBP();
    }
    assert(bp);

    return bp;
}

SchedulerKeepMasterBP *SchedulerKeepMasterBP::GetThreadInstance() {
    static thread_local SchedulerKeepMasterBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewSchedulerKeepMasterBP().release();
    }
    if (!bp) {
        bp = new SchedulerKeepMasterBP();
    }
    assert(bp);

    return bp;
}


LockBP *LockBP::GetThreadInstance() {
    static thread_local LockBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewLockBP().release();
    }
    if (!bp) {
        bp = new LockBP();
    }
    assert(bp);

    return bp;
}


LockMgrBP *LockMgrBP::GetThreadInstance() {
    static thread_local LockMgrBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewLockMgrBP().release();
    }
    if (!bp) {
        bp = new LockMgrBP();
    }
    assert(bp);

    return bp;
}


LockDbBP *LockDbBP::GetThreadInstance() {
    static thread_local LockDbBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewLockDbBP().release();
    }
    if (!bp) {
        bp = new LockDbBP();
    }
    assert(bp);

    return bp;
}


LockCleanThreadBP *LockCleanThreadBP::GetThreadInstance() {
    static thread_local LockCleanThreadBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewLockCleanThreadBP().release();
    }
    if (!bp) {
        bp = new LockCleanThreadBP();
    }
    assert(bp);

    return bp;
}


LockKeepMasterThreadBP *LockKeepMasterThreadBP::GetThreadInstance() {
    static thread_local LockKeepMasterThreadBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewLockKeepMasterThreadBP().release();
    }
    if (!bp) {
        bp = new LockKeepMasterThreadBP();
    }
    assert(bp);

    return bp;
}


LockIMMasterBP *LockIMMasterBP::GetThreadInstance() {
    static thread_local LockIMMasterBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewLockIMMasterBP().release();
    }
    if (!bp) {
        bp = new LockIMMasterBP();
    }
    assert(bp);

    return bp;
}


LockSnatchMasterBP *LockSnatchMasterBP::GetThreadInstance() {
    static thread_local LockSnatchMasterBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewLockSnatchMasterBP().release();
    }
    if (!bp) {
        bp = new LockSnatchMasterBP();
    }
    assert(bp);

    return bp;
}


LockSMBP *LockSMBP::GetThreadInstance() {
    static thread_local LockSMBP *bp{nullptr};
    if (!bp) {
        bp = plugin::BreakPointFactory::GetInstance()->NewLockSMBP().release();
    }
    if (!bp) {
        bp = new LockSMBP();
    }
    assert(bp);

    return bp;
}


}  // namespace comm

}  // namespace phxqueue

