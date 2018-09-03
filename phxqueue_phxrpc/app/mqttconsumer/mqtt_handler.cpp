/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "mqtt_handler.h"

#include <cinttypes>
#include <string>

#include "phxqueue/comm.h"
#include "phxqueue_phxrpc/app/logic/mqtt.h"
#include "phxqueue_phxrpc/app/mqttbroker/mqttbroker_client.h"


namespace phxqueue_phxrpc {

namespace mqttconsumer {


using namespace phxqueue_phxrpc::logic::mqtt;
using namespace std;


phxqueue::comm::HandleResult MqttHandler::Handle(const phxqueue::comm::proto::ConsumerContext &cc,
                                                 phxqueue::comm::proto::QItem &item,
                                                 string &uncompressed_buffer) {
    // 1. parse message
    phxqueue_phxrpc::logic::mqtt::HttpPublishPb message;
    if (!message.ParseFromString(item.buffer())) {
        QLErr("consumer_group_id %d store_id %d queue_id %d item_uin %" PRIu64
              " ParseFromString err", cc.consumer_group_id(), cc.store_id(), cc.queue_id(),
              (uint64_t)item.meta().uin());

        return phxqueue::comm::HandleResult::RES_ERROR;
    }
    // TODO: remove
    printf("%s:%d message: {\n%s}\n", __func__, __LINE__, message.DebugString().c_str());

    // 2. get remote topic_name -> session_ids
    TableMgr table_mgr(cc.topic_id());
    uint64_t version{0uLL};
    TopicPb topic_pb;
    phxqueue::comm::RetCode ret{table_mgr.GetTopicSubscribeRemote(
            message.mqtt_publish().topic_name(), &version, &topic_pb)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("consumer_group_id %d store_id %d queue_id %d item_uin %" PRIu64
              " GetTopicSubscribeRemote err %d topic \"%s\"",
              cc.consumer_group_id(), cc.store_id(), cc.queue_id(),
              (uint64_t)item.meta().uin(), phxqueue::comm::as_integer(ret),
              message.mqtt_publish().topic_name().c_str());

        return phxqueue::comm::HandleResult::RES_ERROR;
    }
    // TODO: remove
    QLInfo("topic \"%s\" nr_sub %d", message.mqtt_publish().topic_name().c_str(),
           topic_pb.subscribes().size());

    // 3. publish
    for (const auto &subscribe_pb : topic_pb.subscribes()) {
        // 3.1. get remote session
        uint64_t version{0uLL};
        SessionPb session_pb;
        ret = table_mgr.GetSessionByClientIdRemote(subscribe_pb.client_identifier(),
                                                   &version, &session_pb);
        if (phxqueue::comm::RetCode::RET_OK != ret) {
            QLErr("consumer_group_id %d store_id %d queue_id %d item_uin %" PRIu64
                  " GetSessionByClientIdRemote err %d",
                  cc.consumer_group_id(), cc.store_id(), cc.queue_id(),
                  (uint64_t)item.meta().uin(), phxqueue::comm::as_integer(ret));

            // TODO: retry
            continue;
        }

        phxqueue_phxrpc::logic::mqtt::HttpPublishPb req(message);
        req.set_sub_client_id(subscribe_pb.client_identifier());
        req.set_cursor_id(item.cursor_id());
        req.set_store_id(cc.store_id());
        req.set_queue_id(cc.queue_id());
        req.mutable_addr()->set_ip(session_pb.addr().ip());
        req.mutable_addr()->set_port(session_pb.addr().port());
        phxqueue_phxrpc::logic::mqtt::HttpPubackPb resp;
        MqttBrokerClient mqttbroker_client;
        ret = mqttbroker_client.ProtoHttpPublish(req, &resp);
        if (phxqueue::comm::RetCode::RET_OK != ret) {
            QLErr("ProtoHttpPublish addr %s:%d err %d",
                  session_pb.addr().ip().c_str(), session_pb.addr().port(),
                  phxqueue::comm::as_integer(ret));

            return phxqueue::comm::HandleResult::RES_ERROR;
        }
    }

    return phxqueue::comm::HandleResult::RES_OK;
}


}  // namespace mqttconsumer

}  // namespace phxqueue_phxrpc

