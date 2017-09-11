/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/test/simplescheduler.h"

#include <iostream>
#include <memory>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"


namespace phxqueue {

namespace test {


using namespace std;


comm::RetCode SimpleScheduler::GetLockInfo(const comm::proto::GetLockInfoRequest &req,
                                           comm::proto::GetLockInfoResponse &resp) {
    QLVerb("GetLockInfo topic_id %d lock_id %d lock_key %s",
           req.topic_id(), req.lock_id(), req.lock_key().c_str());

    shared_ptr<const config::LockConfig> lock_config;
    comm::RetCode ret{config::GlobalConfig::GetThreadInstance()->
            GetLockConfig(req.topic_id(), lock_config)};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetLockConfig ret %d topic_id %d", as_integer(ret), req.topic_id());

        return ret;
    }

    shared_ptr<const config::proto::Lock> lock;
    ret = lock_config->GetLockByLockID(req.lock_id(), lock);
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetLockByLockID ret %d lock_id %d", as_integer(ret), req.lock_id());

        return ret;
    }

    assert(lock->addrs_size() >= 2);
    comm::proto::Addr master_addr = lock->addrs(1);

    if (req.master_addr().ip() == master_addr.ip() &&
        req.master_addr().port() == master_addr.port() &&
        req.master_addr().paxos_port() == master_addr.paxos_port()) {

        auto &&lock_info(resp.mutable_lock_info());
        lock_info->set_lock_key(req.lock_key());
        lock_info->set_version(1);
        lock_info->set_client_id("test_client_1");
        lock_info->set_lease_time_ms(10000000);

        return comm::RetCode::RET_OK;
    }
    resp.mutable_redirect_addr()->CopyFrom(master_addr);

    return comm::RetCode::RET_ERR_NOT_MASTER;
}

comm::RetCode SimpleScheduler::AcquireLock(const comm::proto::AcquireLockRequest &req,
                                           comm::proto::AcquireLockResponse &resp) {
    QLVerb("AcquireLock topic_id %d lock_id %d lock_key %s",
           req.topic_id(), req.lock_id(), req.lock_info().lock_key().c_str());

    shared_ptr<const config::LockConfig> lock_config;
    comm::RetCode ret{config::GlobalConfig::GetThreadInstance()->
            GetLockConfig(req.topic_id(), lock_config)};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetLockConfig ret %d topic_id %d", as_integer(ret), req.topic_id());

        return ret;
    }

    shared_ptr<const config::proto::Lock> lock;
    ret = lock_config->GetLockByLockID(req.lock_id(), lock);
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetLockByLockID ret %d lock_id %d", as_integer(ret), req.lock_id());

        return ret;
    }

    assert(lock->addrs_size() >= 2);
    comm::proto::Addr master_addr = lock->addrs(1);

    if (req.master_addr().ip() == master_addr.ip() &&
        req.master_addr().port() == master_addr.port() &&
        req.master_addr().paxos_port() == master_addr.paxos_port()) {

        return comm::RetCode::RET_OK;
    }
    resp.mutable_redirect_addr()->CopyFrom(master_addr);

    return comm::RetCode::RET_ERR_NOT_MASTER;
}


}  // namespace test

}  // namespace phxqueue

