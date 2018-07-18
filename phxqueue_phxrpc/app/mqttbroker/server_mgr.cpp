/*
Tencent is pleased to support the open source community by making
PhxRPC available.
Copyright (C) 2016 THL A29 Limited, a Tencent company.
All rights reserved.

Licensed under the BSD 3-Clause License (the "License"); you may
not use this file except in compliance with the License. You may
obtain a copy of the License at

https://opensource.org/licenses/BSD-3-Clause

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

See the AUTHORS file for names of contributors.
*/

#include "server_mgr.h"

#include "event_loop_server.h"


using namespace std;


ServerMgr::ServerMgr(const phxrpc::HshaServerConfig *const config) : config_(config) {
}

ServerMgr::~ServerMgr() {}

phxrpc::HshaServer *ServerMgr::hsha_server() const {
    return hsha_server_;
}

void ServerMgr::set_hsha_server(phxrpc::HshaServer *const hsha_server) {
    hsha_server_ = hsha_server;
}

EventLoopServer *ServerMgr::event_loop_server() const {
    return event_loop_server_;
}

void ServerMgr::set_event_loop_server(EventLoopServer *const event_loop_server) {
    event_loop_server_ = event_loop_server;
}

void ServerMgr::Send(const uint64_t session_id, phxrpc::BaseResponse *const resp) {
    event_loop_server_->SendResponse(session_id, resp);
}

int ServerMgr::SendAndWaitAck(const uint64_t session_id, phxrpc::BaseResponse *const resp,
                              phxrpc::UThreadEpollScheduler *const uthread_scheduler,
                              const string &ack_key, void *&ack_value) {
    auto &&notifier(notifier_map_[ack_key]);
    notifier.reset(new phxrpc::UThreadNotifier);
    int ret{notifier->Init(uthread_scheduler, config_->GetSocketTimeoutMS())};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "%s notifier.Init err %d", __func__, ret);
        notifier_map_.erase(ack_key);

        return ret;
    }

    event_loop_server_->SendResponse(session_id, resp);

    // yield and wait
    void *data{nullptr};
    ret = notifier->WaitNotify(data);
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "%s notifier.WaitNotify err %d", __func__, ret);
        notifier_map_.erase(ack_key);

        return ret;
    }

    ack_value = data;
    notifier_map_.erase(ack_key);

    return ret;
}

int ServerMgr::Ack(const string &ack_key, void *const ack_value) {
    auto &&notifier_it(notifier_map_.find(ack_key));
    if (notifier_map_.end() == notifier_it) {
        phxrpc::log(LOG_ERR, "%s notifier not found", __func__);

        return -1;
    }

    int ret{notifier_it->second->SendNotify(ack_value)};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "%s notifier.SendNotify err %d", __func__, ret);
        notifier_map_.erase(ack_key);

        return ret;
    }

    return ret;
}

void ServerMgr::DestroySession(const uint64_t session_id) {
    event_loop_server_->DestroySession(session_id);
}

