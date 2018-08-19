/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "publish_mgr.h"

#include <functional>

#include "phxqueue/comm.h"

#include "../mqtt/mqtt_msg.h"
#include "../mqtt/mqtt_packet_id.h"
#include "../mqtt/mqtt_session.h"
#include "../mqttbroker_server_config.h"
#include "../server_mgr.h"
#include "publish_memory.h"


namespace {


using namespace phxqueue_phxrpc::logic::mqtt;
using namespace phxqueue_phxrpc::mqttbroker;


struct DispatchCtx {
    stCoRoutine_t *co{nullptr};
    uint64_t session_id{0uL};
    const MqttBrokerServerConfig *config{nullptr};
    ServerMgr *server_mgr{nullptr};
};

void CoSleep(const int sleep_time_ms) {
    struct pollfd pf{0};
    pf.fd = -1;
    poll(nullptr, 0, sleep_time_ms);
}

int Publish(ServerMgr *const server_mgr, const HttpPublishPb &req, const int sleep_time_ms) {
    // 1. check local session
    const auto sub_mqtt_session(MqttSessionMgr::GetInstance()->
                                GetByClientId(req.sub_client_id()));
    if (!sub_mqtt_session) {
        NLErr("GetByClientId err sub_client_id \"%s\"", req.sub_client_id().c_str());

        return -1;
    }

    // 2. publish to event_loop_server
    MqttPublishPb mqtt_publish_pb;
    mqtt_publish_pb.CopyFrom(req.mqtt_publish());
    // mqtt-3.3.1-3: reset dup
    mqtt_publish_pb.set_dup(false);

    if (1 == mqtt_publish_pb.qos()) {
        uint16_t sub_packet_id{0};
        if (!MqttPacketIdMgr::GetInstance()->AllocPacketId(req.cursor_id(), req.pub_client_id(),
                req.mqtt_publish().packet_identifier(), req.sub_client_id(), &sub_packet_id)) {
            NLErr("sub_session_id %" PRIx64 " AllocPacketId err sub_client_id \"%s\"",
                  sub_mqtt_session->session_id, req.sub_client_id().c_str());

            return -1;
        }
        mqtt_publish_pb.set_packet_identifier(sub_packet_id);

        auto *mqtt_publish(new MqttPublish);
        mqtt_publish->FromPb(mqtt_publish_pb);
        int ret{-1};
        while (true) {
            ret = server_mgr->Send(sub_mqtt_session->session_id, mqtt_publish);
            if (0 != ret) {
                NLErr("sub_session_id %" PRIx64 " server_mgr.Send err %d sub_client_id \"%s\" qos %u",
                      sub_mqtt_session->session_id, ret, req.sub_client_id().c_str(),
                      req.mqtt_publish().qos());
                CoSleep(sleep_time_ms);

                continue;
            }

            //// ack_key = sub_client_id + sub_packet_id
            //const string ack_key(req.sub_client_id() + ':' + to_string(sub_packet_id));
            //void *data{nullptr};
            //auto *mqtt_publish(new MqttPublish);
            //mqtt_publish->FromPb(mqtt_publish_pb);
            //int ret{server_mgr->SendAndWaitAck(sub_mqtt_session->session_id, mqtt_publish,
            //                                         worker_uthread_scheduler_, ack_key, data)};
            //// TODO: check ret

            //MqttPuback *puback{
            //        (MqttPuback *)data};
            //if (!puback) {
            //    NLErr("sub_session_id %" PRIx64 " server_mgr.SendAndWaitAck nullptr "
            //          "qos %u ack_key \"%s\"", sub_mqtt_session->session_id,
            //          req.mqtt_publish().qos(), ack_key.c_str());

            //    return -1;
            //}

            //NLInfo("sub_session_id %" PRIx64 " server_mgr.SendAndWaitAck ack_key \"%s\" qos %u",
            //       sub_mqtt_session->session_id, ack_key.c_str(), req.mqtt_publish().qos());

            //ret = puback->ToPb(resp->mutable_mqtt_puback());

            //delete puback;

            //MqttPacketIdMgr::GetInstance()->ReleasePacketId(req.pub_client_id(),
            //        req.mqtt_publish().packet_identifier(), req.sub_client_id());

            //if (0 != ret) {
            //    NLErr("ToPb err %d", ret);

            //    return -1;
            //}

            break;  // finish send
        }
    } else {
        auto *mqtt_publish(new MqttPublish);
        mqtt_publish->FromPb(mqtt_publish_pb);
        int ret{-1};
        while (true) {
            ret = server_mgr->Send(sub_mqtt_session->session_id, mqtt_publish);
            if (0 != ret) {
                NLErr("sub_session_id %" PRIx64 " server_mgr.Send err %d sub_client_id \"%s\" qos %u",
                      sub_mqtt_session->session_id, ret, req.sub_client_id().c_str(),
                      req.mqtt_publish().qos());
                CoSleep(sleep_time_ms);

                continue;
            }

            break;  // finish send
        }

        NLInfo("sub_session_id %" PRIx64 " server_mgr.Send sub_client_id \"%s\" qos %u",
               sub_mqtt_session->session_id, req.sub_client_id().c_str(),
               req.mqtt_publish().qos());
    }

    return 0;
}


void *PublishRoutineRun(void *arg) {
    co_enable_hook_sys();

    DispatchCtx *ctx{static_cast<DispatchCtx *>(arg)};
    auto it(PublishQueue::GetInstance()->cbegin());
    uint64_t last_curosr_id(-1);

    while (true) {
        // 1. pick from publish queue
        if (!it.Valid()) {
            it = PublishQueue::GetInstance()->cbegin();

            // 2. if lost msg
            auto &&kv(*it);
            if (-1 != last_curosr_id && last_curosr_id + 1 < kv.first) {
                for (uint64_t cursor_id{last_curosr_id + 1}; kv.first > cursor_id; ++cursor_id) {
                    // 2.1. get from lru cache
                    HttpPublishPb http_publish_pb;
                    int ret{PublishLruCache::GetInstance()->Get(cursor_id, http_publish_pb)};
                    if (0 != ret) {
                        // 2.2. if no cache, rpc get from store
                        // TODO:
                        //for () {
                            // 2.3. set to lru cache
                            //PublishLruCache::GetInstance()->Put(cursor_id, http_publish_pb);
                            // 2.4. add to eventloop server out queue
                            //ret = Publish(ctx->server_mgr, http_publish_pb);
                        //}

                        break;
                    }
                }
            }
            last_curosr_id = kv.first;
        }
        if (PublishQueue::GetInstance()->cend() == it) {
            CoSleep(ctx->config->publish_sleep_time_ms());

            continue;
        }

        auto &&kv(*it);
        // 3. add to el out queue
        int ret{Publish(ctx->server_mgr, kv.second, ctx->config->publish_sleep_time_ms())};
        // 3.1. if session die
        return nullptr;
        // 4. rpc set prev to lock
        // if done > 10 reqs
        //CoSleep(ctx->config->publish_sleep_time_ms());

        last_curosr_id = kv.first;
        ++it;
    }
}


}  // namespace


namespace phxqueue_phxrpc {

namespace mqttbroker {


using namespace std;


PublishThread::PublishThread(const MqttBrokerServerConfig *const config,
                             ServerMgr *const server_mgr)
        : config_(config), server_mgr_(server_mgr), thread_(&PublishThread::RunFunc, this) {
}

PublishThread::PublishThread(PublishThread &&publish_thread)
        : server_mgr_(publish_thread.server_mgr_) {
    config_ = publish_thread.config_;
    publish_thread.config_ = nullptr;
    thread_ = move(publish_thread.thread_);
}

PublishThread::~PublishThread() {
    thread_.join();
}

void PublishThread::RunFunc() {
    co_eventloop(co_get_epoll_ct(), nullptr, this);
}

int PublishThread::CreateSession(const uint64_t session_id) {
    stCoRoutineAttr_t attr;
    attr.stack_size = 1024 * config_->share_stack_size_kb();

    {
        DispatchCtx ctx;
        ctx.co = nullptr;
        ctx.session_id = session_id;
        ctx.config = config_;
        ctx.server_mgr = server_mgr_;
        co_create(&(ctx.co), &attr, PublishRoutineRun, &ctx);
        co_resume(ctx.co);
    }

    return 0;
}

int PublishThread::DestroySession(const uint64_t session_id) {
    // TODO:

    return 0;
}


PublishMgr::PublishMgr(const MqttBrokerServerConfig *const config, ServerMgr *server_mgr) {
    for (int i{0}; config->nr_publish_thread() > i; ++i) {
        publish_threads_.emplace_back(move(unique_ptr<PublishThread>(new PublishThread(config, server_mgr))));
    }
}

PublishMgr::~PublishMgr() {
}

int PublishMgr::CreateSession(const uint64_t session_id) {
    return publish_threads_[hash<uint64_t>{}(session_id)]->CreateSession(session_id);
}

int PublishMgr::DestroySession(const uint64_t session_id) {
    return publish_threads_[hash<uint64_t>{}(session_id)]->DestroySession(session_id);
}


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

