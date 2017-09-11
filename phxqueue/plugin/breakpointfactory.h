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
class ProducerSubBP;

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
    virtual std::unique_ptr<comm::ProducerSubBP> NewProducerSubBP();

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

