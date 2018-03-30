/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>
#include <set>

#include "phxqueue/comm/handler.h"
#include "phxqueue/comm/errdef.h"
#include "phxqueue/comm/proto/comm.pb.h"


namespace phxqueue {

namespace comm {


class ConsumerBP {
  public:
    static ConsumerBP *GetThreadInstance();

    ConsumerBP() {}
    virtual ~ConsumerBP() {}

    virtual void OnChildRun(const int topic_id) {}
    virtual void OnLock(const int topic_id) {}
    virtual void OnLockSucc(const proto::ConsumerContext &cc) {}
    virtual void OnProcess(const proto::ConsumerContext &cc) {}
    virtual void OnProcessFail(const proto::ConsumerContext &cc) {}
    virtual void OnProcessSucc(const proto::ConsumerContext &cc) {}
    virtual void OnGetQueueDelayFail(const proto::ConsumerContext &cc) {}
    virtual void OnLoop(const proto::ConsumerContext &cc) {}
    virtual void OnLoopBreak(const proto::ConsumerContext &cc) {}
    virtual void OnGet(const proto::ConsumerContext &cc) {}
    virtual void OnGetFail(const proto::ConsumerContext &cc) {}
    virtual void OnGetSucc(const proto::ConsumerContext &cc) {}
    virtual void OnGetNoItem(const proto::ConsumerContext &cc) {}
    virtual void OnSleepAfterGet(const proto::ConsumerContext &cc) {}
    virtual void OnGetWithItem(const proto::ConsumerContext &cc, const std::vector<std::shared_ptr<proto::QItem>> &items) {}
    virtual void OnDropAll(const proto::ConsumerContext &cc, const std::vector<std::shared_ptr<proto::QItem>> &items) {}
    virtual void OnGetSizeTooSmall(const proto::ConsumerContext &cc, const std::vector<std::shared_ptr<proto::QItem>> &items) {}

    virtual void OnMemCheck(const int topic_id) {}
    virtual void OnMemCheckUnpass(const int topic_id) {}
    virtual void OnMemCheckPass(const int topic_id) {}

    virtual void OnMaxLoopCheck(const int topic_id) {}
    virtual void OnMaxLoopCheckUnpass(const int topic_id) {}
    virtual void OnMaxLoopCheckPass(const int topic_id) {}

    virtual void OnConsume(const proto::ConsumerContext &cc, const std::vector<std::shared_ptr<proto::QItem>> &items) {}
    virtual void OnConsumeSucc(const proto::ConsumerContext &cc, const std::vector<std::shared_ptr<proto::QItem>> &items,
                               const std::vector<HandleResult> &handle_results) {}
};

class ConsumerConsumeBP {
  public:
    static ConsumerConsumeBP *GetThreadInstance();

    ConsumerConsumeBP() {}
    virtual ~ConsumerConsumeBP() {}

    virtual void OnConsumeThreadRun(const proto::ConsumerContext &cc) {}
    virtual void OnConsumeThreadRunEnd(const proto::ConsumerContext &cc) {}
    virtual void OnTaskDispatch(const proto::ConsumerContext &cc) {}
    virtual void OnTaskWait(const proto::ConsumerContext &cc) {}
    virtual void OnTaskDispatchFinish(const proto::ConsumerContext &cc) {}
    virtual void OnMakeHandleBucket(const proto::ConsumerContext &cc) {}
    virtual void OnHandleRoutineFunc(const proto::ConsumerContext &cc, const int cid) {}
    virtual void OnHandle(const proto::ConsumerContext &cc, const proto::QItem &item) {}
    virtual void OnHandleEnd(const proto::ConsumerContext &cc, const proto::QItem &item, const HandleResult &handle_result) {}
    virtual void OnBatchHandleRoutineFunc(const proto::ConsumerContext &cc, const int cid) {}
    virtual void OnSkipHandle(const proto::ConsumerContext &cc, const proto::QItem &item) {}
};

class ConsumerHeartBeatLockBP {
  public:
    static ConsumerHeartBeatLockBP *GetThreadInstance();

    ConsumerHeartBeatLockBP() {}
    virtual ~ConsumerHeartBeatLockBP() {}

    virtual void OnSync(const int topic_id) {}
    virtual void OnProcUsed(const int topic_id, const int nproc, const int proc_used) {}
    virtual void OnScaleHash(const int topic_id, const int consumer_group_id, const uint32_t scale_hash) {}
    virtual void OnAdjustScale(const int topic_id, const int consumer_group_id) {}
    virtual void OnProcUsedExceed(const int topic_id, const int nproc, const int proc_used) {}
    virtual void OnSyncSucc(const int topic_id) {}
    virtual void OnSyncFail(const int topic_id) {}

    virtual void OnLock(const int topic_id, const int consumer_group_id, const int store_id, const int queue_id) {}
    virtual void OnProcLack(const int topic_id) {}
    virtual void OnNoLockTarget(const int topic_id) {}
    virtual void OnSkipLock(const int topic_id, const int consumer_group_id) {}
    virtual void OnConcurrentIdempotent(const int topic_id, const int queue_info_id) {}
    virtual void OnLockSucc(const int topic_id, const int consumer_group_id, const int store_id, const int queue_id) {}
    virtual void OnLockFail(const int topic_id, const int consumer_group_id, const int store_id, const int queue_id) {}
};

class StoreBP {
  public:
    static StoreBP *GetThreadInstance();

    StoreBP() {}
    virtual ~StoreBP() {}

    virtual void OnAdd(const proto::AddRequest &req) {}
    virtual void OnAddRequestInvalid(const proto::AddRequest &req) {}
    virtual void OnAddCheckMasterUnpass(const proto::AddRequest &req) {}
    virtual void OnAddCheckMasterPass(const proto::AddRequest &req) {}
    virtual void OnAddSkip(const proto::AddRequest &req) {}

    virtual void OnPaxosAdd(const proto::AddRequest &req) {}
    virtual void OnBatchPropose(const proto::AddRequest &req) {}
    virtual void OnBatchProposeErr(const proto::AddRequest &req, const uint64_t used_time_ms) {}
    virtual void OnBatchProposeErrTimeout(const proto::AddRequest &req) {}
    virtual void OnBatchProposeErrTooManyThreadWaitingReject(const proto::AddRequest &req) {}
    virtual void OnBatchProposeErrValueSizeTooLarge(const proto::AddRequest &req) {}
    virtual void OnBatchProposeErrOther(const proto::AddRequest &req) {}
    virtual void OnBatchProposeSucc(const proto::AddRequest &req, const uint64_t instance_id, const uint32_t batch_idx, const uint64_t used_time_ms) {}

    virtual void OnGet(const proto::GetRequest &req) {}
    virtual void OnGetRequestInvalid(const proto::GetRequest &req) {}
    virtual void OnGetCheckMasterUnpass(const proto::GetRequest &req) {}
    virtual void OnGetCheckMasterPass(const proto::GetRequest &req) {}
    virtual void OnBaseMgrGet(const proto::GetRequest &req) {}
    virtual void OnBaseMgrGetFail(const proto::GetRequest &req) {}
    virtual void OnBaseMgrGetSucc(const proto::GetRequest &req, const proto::GetResponse &resp) {}

    virtual void OnInit(const int topic_id) {}
    virtual void OnInitErrTopic(const int topic_id) {}
    virtual void OnInitErrBaseMgr(const int topic_id) {}
    virtual void OnInitErrSyncCtrl(const int topic_id) {}
    virtual void OnInitErrCPMgr(const int topic_id) {}
    virtual void OnInitErrPaxos(const int topic_id) {}
    virtual void OnInitSucc(const int topic_id) {}
};

class StoreBaseMgrBP {
  public:
    static StoreBaseMgrBP *GetThreadInstance();

    StoreBaseMgrBP() {}
    virtual ~StoreBaseMgrBP() {}

    virtual void OnAdd(const proto::AddRequest &req, const int queue_info_id) {}
    virtual void OnAddSkip(const proto::AddRequest &req) {}
    virtual void OnAddSucc(const proto::AddRequest &req, const uint64_t instance_id) {}

    virtual void OnGet(const proto::GetRequest &req) {}
    virtual void OnAdjustNextCursorIDFail(const proto::GetRequest &req) {}
    virtual void OnCursorIDNotFound(const proto::GetRequest &req) {}
    virtual void OnCursorIDChange(const proto::GetRequest &req) {}
    virtual void OnGetNoMoreItem(const proto::GetRequest &req, const uint64_t cur_cursor_id) {}
    virtual void OnGetItemsByCursorIDFail(const proto::GetRequest &req, const uint64_t cursor_id) {}
    virtual void OnGetLastItemNotChosenInPaxos(const proto::GetRequest &req, const uint64_t cursor_id) {}
    virtual void OnGetNoMoreItemBeforeATime(const proto::GetRequest &req) {}
    virtual void OnGetSkip(const proto::GetRequest &req, const proto::QItem &item) {}
    virtual void OnGetRespSizeExceed(const proto::GetRequest &req, const size_t byte_size) {}
    virtual void OnGetItemTooBig(const proto::GetRequest &req, const proto::QItem &item) {}
    virtual void OnUpdateCursorIDFail(const proto::GetRequest &req) {}
    virtual void OnItemInResp(const proto::GetRequest &req) {}
    virtual void OnGetSucc(const proto::GetRequest &req, const proto::GetResponse &resp) {}
    virtual void OnCrcCheckPass(const proto::GetRequest &req) {}
    virtual void OnCrcCheckUnpass(const proto::GetRequest &req) {}
    virtual void OnGetItemFromStoreMetaQueue(const proto::GetRequest &req) {}
    virtual void OnGetItemBeforeCheckPoint(const proto::GetRequest &req) {}
    virtual void OnGetLoopReachMaxTime(const proto::GetRequest &req) {}
};

class StoreIMMasterBP {
  public:
    static StoreIMMasterBP *GetThreadInstance();

    StoreIMMasterBP() {}
    virtual ~StoreIMMasterBP() {}

    virtual void OnIMMaster(const int topic_id, const int paxos_group_id) {}
};

class StoreSnatchMasterBP {
  public:
    static StoreSnatchMasterBP *GetThreadInstance();

    StoreSnatchMasterBP() {}
    virtual ~StoreSnatchMasterBP() {}

    virtual void OnSnatchMaster(const int topic_id, const int paxos_group_id) {}
};

class StoreBacklogBP {
  public:
    static StoreBacklogBP *GetThreadInstance();

    StoreBacklogBP() {}
    virtual ~StoreBacklogBP() {}

    virtual void OnBackLogReport(const int topic_id, const int consumer_group_id, const int queue_id, const int backlog) {}
};

class StoreSMBP {
  public:
    static StoreSMBP *GetThreadInstance();

    StoreSMBP() {}
    virtual ~StoreSMBP() {}

    virtual void OnGetCheckpointInstanceID(const int paxos_group_id, const uint64_t cp) {}
};

class ProducerBP {
  public:
    static ProducerBP *GetThreadInstance();

    ProducerBP() {}
    virtual ~ProducerBP() {}

    virtual void OnEnqueue(const int topic_id, const int pub_id, const int handle_id, const uint64_t uin) {}
    virtual void OnSelectAndAddFail(const int topic_id, const int pub_id, const int handle_id, const uint64_t uin) {}
    virtual void OnEnqueueSucc(const int topic_id, const int pub_id, const int handle_id, const uint64_t uin) {}

    virtual void OnSelectAndAdd(const proto::AddRequest &req) {}
    virtual void OnTopicIDInvalid(const proto::AddRequest &req) {}
    virtual void OnUseDefaultQueueSelector(const proto::AddRequest &req) {}
    virtual void OnUseCustomQueueSelector(const proto::AddRequest &req) {}
    virtual void OnGetQueueIDFail(const proto::AddRequest &req) {}
    virtual void OnUseDefaultStoreSelector(const proto::AddRequest &req) {}
    virtual void OnUseCustomStoreSelector(const proto::AddRequest &req) {}
    virtual void OnGetStoreIDFail(const proto::AddRequest &req) {}
    virtual void OnRawAddFail(const proto::AddRequest &req) {}
    virtual void OnSelectAndAddSucc(const proto::AddRequest &req) {}

    virtual void OnRawAdd(const proto::AddRequest &req) {}
    virtual void OnMasterClientCallFail(const proto::AddRequest &req) {}
    virtual void OnRawAddSucc(const proto::AddRequest &req) {}

    virtual void OnMakeAddRequests(const int topic_id, const std::vector<std::shared_ptr<proto::QItem>> &items) {}
    virtual void OnValidTopicID(const int topic_id) {}
    virtual void OnValidPubID(const int topic_id, const int pub_id) {}
    virtual void OnItemSizeTooLarge(const int topic_id, const int pub_id) {}
    virtual void OnMakeAddRequestsSucc(const int topic_id, const std::vector<std::shared_ptr<proto::QItem>> &items) {}

    virtual void OnCountLimit(const int topic_id, const int pub_id, const proto::QItem &item) {}

    virtual void OnBatchRawAdd(const proto::AddRequest &req) {}
    virtual void OnBatchRawAddSucc(const proto::AddRequest &req) {}
    virtual void OnBatchRawAddFail(const proto::AddRequest &req) {}
    virtual void OnBatchStat(const proto::AddRequest &req, const RetCode &retcode, const uint64_t time_wait_ms, bool is_timeout) {}

};

class ProducerConsumerGroupBP {
  public:
    static ProducerConsumerGroupBP *GetThreadInstance();

    ProducerConsumerGroupBP() {}
    virtual ~ProducerConsumerGroupBP() {}

    virtual void OnConsumerGroupDistribute(const int topic_id, const int pub_id, const int handle_id, const uint64_t uin, const std::set<int> *consumer_group_ids) {}
};


class SchedulerBP {
  public:
    static SchedulerBP *GetThreadInstance();

    SchedulerBP() {}
    virtual ~SchedulerBP() {}

    virtual void OnInit() {}
    virtual void OnDispose() {}

    virtual void OnGetAddrScale(const proto::GetAddrScaleRequest &req) {}
    virtual void OnGetAddrScaleSucc(const proto::GetAddrScaleRequest &req,
                                    const proto::GetAddrScaleResponse &resp) {}
    virtual void OnGetAddrScaleFail(const proto::GetAddrScaleRequest &req) {}

    virtual void OnGetLockInfoSucc(const proto::GetLockInfoRequest &req,
                                   const proto::GetLockInfoResponse &resp) {}
    virtual void OnGetLockInfoFail(const proto::GetLockInfoRequest &req) {}

    virtual void OnAcquireLockSucc(const proto::AcquireLockRequest &req,
                                   const proto::AcquireLockResponse &resp) {}
    virtual void OnAcquireLockFail(const proto::AcquireLockRequest &req) {}
};


class SchedulerMgrBP {
  public:
    static SchedulerMgrBP *GetThreadInstance();

    SchedulerMgrBP() {}
    virtual ~SchedulerMgrBP() {}

    virtual void OnInit() {}
    virtual void OnDispose() {}

    virtual void OnIMMaster(const proto::GetAddrScaleRequest &req) {}
    virtual void OnIMNotMaster(const proto::GetAddrScaleRequest &req) {}

    virtual void OnGetAddrScale(const proto::GetAddrScaleRequest &req) {}
    virtual void OnConsumerNotFound(const proto::GetAddrScaleRequest &req) {}
    virtual void OnUpdateStickyLoad(const proto::GetAddrScaleRequest &req) {}
    virtual void OnBuildTopicScaleResponseSucc(const proto::GetAddrScaleRequest &req,
                                               const proto::GetAddrScaleResponse &resp) {}
    virtual void OnBuildTopicScaleResponseFail(const proto::GetAddrScaleRequest &req) {}

    virtual void OnSkipUpdateLoad(const proto::Addr &addr) {}

};


class SchedulerLoadBalanceBP {
  public:
    static SchedulerLoadBalanceBP *GetThreadInstance();

    SchedulerLoadBalanceBP() {}
    virtual ~SchedulerLoadBalanceBP() {}

    virtual void OnInit() {}
    virtual void OnDispose() {}

    virtual void OnLoadBalance(const int topic_id) {}
    virtual void OnReloadConsumerConfigFail(const int topic_id) {}
    virtual void OnReloadConsumerConfigSucc(const int topic_id,
                                            const bool consumer_conf_modified) {}

    virtual void OnConsumerAdd(const int topic_id, const proto::Addr &addr) {}
    virtual void OnConsumerRemove(const int topic_id, const proto::Addr &addr) {}
    virtual void OnConsumerChange(const int topic_id, const int nconsumer) {}

    virtual void OnDynamicMode(const int topic_id) {}

    virtual void OnUpdateLiveFail(const int topic_id) {}
    virtual void OnUpdateLiveSucc(const int topic_id, const bool live_modified) {}

    virtual void OnConsumerNewDie(const int topic_id, const proto::Addr &addr) {}
    virtual void OnConsumerNewLive(const int topic_id, const proto::Addr &addr) {}

    virtual void OnGetMeanLoadFail(const int topic_id) {}
    virtual void OnGetMeanLoadSucc(const int topic_id) {}

    virtual void OnCheckImbalanceFail(const int topic_id) {}
    virtual void OnCheckImbalanceSucc(const int topic_id, const double mean_load,
                                      const bool balanced) {}

    virtual void OnAdjustScaleFail(const int topic_id) {}
    virtual void OnAdjustScaleSucc(const int topic_id) {}

    virtual void OnPreviewAdjustChange(const int topic_id, const proto::Addr &addr,
                                       const int init_weight, const int old_weight,
                                       const int new_weight) {}
    virtual void OnPreviewAdjustUnchange(const int topic_id, const proto::Addr &addr,
                                         const int init_weight, const int cur_weight) {}

    virtual void OnAdjustApply(const int topic_id) {}
    virtual void OnAdjustNotApply(const int topic_id) {}
};

class SchedulerKeepMasterBP {
  public:
    static SchedulerKeepMasterBP *GetThreadInstance();

    SchedulerKeepMasterBP() {}
    virtual ~SchedulerKeepMasterBP() {}

    virtual void OnInit() {}
    virtual void OnDispose() {}

    virtual void OnKeepMaster(const int topic_id) {}
    virtual void OnRawGetLockInfoFail(const int topic_id, const proto::GetLockInfoRequest &req) {}
    virtual void OnRawGetLockInfoSucc(const int topic_id, const proto::GetLockInfoRequest &req,
                                      const proto::GetLockInfoResponse &resp) {}

    virtual void OnAcquireLock(const int topic_id, const proto::AcquireLockRequest &req) {}
    virtual void OnAcquireLockSucc(const int topic_id, const proto::AcquireLockRequest &req,
                                   const proto::AcquireLockResponse &resp) {}
    virtual void OnAcquireLockFail(const int topic_id, const proto::AcquireLockRequest &req) {}
};

class LockBP {
  public:
    static LockBP *GetThreadInstance();

    LockBP() {}
    virtual ~LockBP() {}

    virtual void OnInit(const int topic_id) {}
    virtual void OnDispose(const int topic_id) {}

    virtual void OnGetString(const proto::GetStringRequest &req) {}
    virtual void OnGetStringRequestInvalid(const proto::GetStringRequest &req) {}
    virtual void OnGetStringCheckMasterPass(const proto::GetStringRequest &req) {}

    virtual void OnSetString(const proto::SetStringRequest &req) {}
    virtual void OnSetStringRequestInvalid(const proto::SetStringRequest &req) {}
    virtual void OnSetStringCheckMasterPass(const proto::SetStringRequest &req) {}

    virtual void OnDeleteString(const proto::DeleteStringRequest &req) {}
    virtual void OnDeleteStringRequestInvalid(const proto::DeleteStringRequest &req) {}
    virtual void OnDeleteStringCheckMasterPass(const proto::DeleteStringRequest &req) {}

    virtual void OnAcquireLock(const proto::AcquireLockRequest &req) {}
    virtual void OnAcquireLockRequestInvalid(const proto::AcquireLockRequest &req) {}
    virtual void OnAcquireLockCheckMasterPass(const proto::AcquireLockRequest &req) {}

    virtual void OnGetLockInfo(const proto::GetLockInfoRequest &req) {}
    virtual void OnGetLockInfoRequestInvalid(const proto::GetLockInfoRequest &req) {}
    virtual void OnGetLockInfoCheckMasterPass(const proto::GetLockInfoRequest &req) {}

    virtual void OnPaxosSetString(const proto::SetStringRequest &req) {}
    virtual void OnPaxosAcquireLock(const proto::AcquireLockRequest &req) {}

    virtual void OnSetStringPropose(const proto::SetStringRequest &req) {}
    virtual void OnSetStringProposeErr(const proto::SetStringRequest &req,
                                       const uint64_t used_time_ms) {}
    virtual void OnSetStringProposeErrTimeout(const proto::SetStringRequest &req) {}
    virtual void OnSetStringProposeErrTooManyThreadWaitingReject(const proto::SetStringRequest &req) {}
    virtual void OnSetStringProposeErrValueSizeTooLarge(const proto::SetStringRequest &req) {}
    virtual void OnSetStringProposeErrOther(const proto::SetStringRequest &req) {}
    virtual void OnSetStringProposeErrResult(const proto::SetStringRequest &req,
                                             const uint64_t instance_id,
                                             const uint64_t used_time_ms) {}
    virtual void OnSetStringProposeSucc(const proto::SetStringRequest &req,
                                        const uint64_t instance_id,
                                        const uint64_t used_time_ms) {}

    virtual void OnAcquireLockPropose(const proto::AcquireLockRequest &req) {}
    virtual void OnAcquireLockProposeErr(const proto::AcquireLockRequest &req,
                                         const uint64_t used_time_ms) {}
    virtual void OnAcquireLockProposeErrTimeout(const proto::AcquireLockRequest &req) {}
    virtual void OnAcquireLockProposeErrTooManyThreadWaitingReject(const proto::AcquireLockRequest &req) {}
    virtual void OnAcquireLockProposeErrValueSizeTooLarge(const proto::AcquireLockRequest &req) {}
    virtual void OnAcquireLockProposeErrOther(const proto::AcquireLockRequest &req) {}
    virtual void OnAcquireLockProposeErrResult(const proto::AcquireLockRequest &req,
                                               const uint64_t instance_id,
                                               const uint64_t used_time_ms) {}
    virtual void OnAcquireLockProposeSucc(const proto::AcquireLockRequest &req,
                                          const uint64_t instance_id,
                                          const uint64_t used_time_ms) {}
};


class LockMgrBP {
  public:
    static LockMgrBP *GetThreadInstance();

    LockMgrBP() {}
    virtual ~LockMgrBP() {}

    virtual void OnReadCheckpoint(const int topic_id, const uint32_t i) {}
    virtual void OnWriteCheckpoint(const int topic_id, const uint32_t i,
                                   uint64_t const checkpoint) {}

    virtual void OnReadRestartCheckpoint(const int topic_id, const uint32_t i) {}
    virtual void OnWriteRestartCheckpoint(const int topic_id, const uint32_t i,
                                          uint64_t const checkpoint) {}
};


class LockDbBP {
  public:
    static LockDbBP *GetThreadInstance();

    LockDbBP() {}
    virtual ~LockDbBP() {}

    virtual void OnVersionSetString(const bool sync) {}
    virtual void OnVersionDeleteString(const bool sync) {}
    virtual void OnAcquireLock(const bool sync) {}
};


class LockCleanThreadBP {
  public:
    static LockCleanThreadBP *GetThreadInstance();

    LockCleanThreadBP() {}
    virtual ~LockCleanThreadBP() {}

    virtual void OnProposeCleanSucc(const int topic_id, const int paxos_group_id) {}
    virtual void OnProposeCleanErr(const int topic_id, const int paxos_group_id) {}

    virtual void OnNrKey(const int topic_id, const int nr) {}
    virtual void OnNrCleanKey(const int topic_id, const int nr) {}
};


class LockKeepMasterThreadBP {
  public:
    static LockKeepMasterThreadBP *GetThreadInstance();

    LockKeepMasterThreadBP() {}
    virtual ~LockKeepMasterThreadBP() {}

    virtual void OnProposeMasterSucc(const int topic_id, const int paxos_group_id) {}
    virtual void OnProposeMasterErr(const int topic_id, const int paxos_group_id) {}
};


class LockIMMasterBP {
  public:
    static LockIMMasterBP *GetThreadInstance();

    LockIMMasterBP() {}
    virtual ~LockIMMasterBP() {}

    virtual void OnIMMaster(const int topic_id, const int paxos_group_id) {}
};


class LockSnatchMasterBP {
  public:
    static LockSnatchMasterBP *GetThreadInstance();

    LockSnatchMasterBP() {}
    virtual ~LockSnatchMasterBP() {}

    virtual void OnSnatchMaster(const int topic_id, const int paxos_group_id) {}
};


class LockSMBP {
  public:
    static LockSMBP *GetThreadInstance();

    LockSMBP() {}
    virtual ~LockSMBP() {}

    virtual void OnExecute(const int topic_id, const int paxos_group_id,
                           const uint64_t instance_id, const std::string &paxos_value) {}
    virtual void OnExecuteForCheckpoint(const int topic_id, const int paxos_group_id,
                                        const uint64_t instance_id,
                                        const std::string &paxos_value) {}
    virtual void OnExecuteForCheckpointSync(const int topic_id, const int paxos_group_id,
                                            const uint64_t instance_id,
                                            const std::string &paxos_value) {}
    virtual void OnExecuteForCheckpointNoSync(const int topic_id, const int paxos_group_id,
                                              const uint64_t instance_id,
                                              const std::string &paxos_value) {}
    virtual void OnGetCheckpointInstanceID(const int topic_id, const int paxos_group_id) {}
    virtual void OnGetCheckpointState(const int topic_id, const int paxos_group_id) {}
    virtual void OnLoadCheckpointState(const int topic_id, const int paxos_group_id) {}
};


}  // namespace comm

}  // namespace phxqueue

