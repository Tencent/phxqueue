/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "mqttconsumer.h"

#include "phxqueue_phxrpc/app/logic/mqtt.h"


namespace phxqueue_phxrpc {

namespace mqttconsumer {


using namespace phxqueue_phxrpc::logic::mqtt;
using namespace std;


MqttConsumer::MqttConsumer(const phxqueue::consumer::ConsumerOption &opt)
        : phxqueue_phxrpc::consumer::Consumer(opt) {}

MqttConsumer::~MqttConsumer() {}

void MqttConsumer::CustomGetRequest(const phxqueue::comm::proto::ConsumerContext &cc,
                                    const phxqueue::comm::proto::GetRequest &req,
                                    uint64_t &pre_cursor_id, uint64_t &next_cursor_id,
                                    int &limit) {
    // 1. get topic_name
    shared_ptr<const phxqueue::config::TopicConfig> topic_config;
    phxqueue::comm::RetCode ret{phxqueue::config::GlobalConfig::GetThreadInstance()->
            GetTopicConfigByTopicID(cc.topic_id(), topic_config)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("consumer_group_id %d store_id %d topic %d queue_id %d "
              "GetTopicConfigByTopicID err %d",
              cc.consumer_group_id(), cc.store_id(), cc.topic_id(), cc.queue_id(),
              phxqueue::comm::as_integer(ret));

        return;
    }

    vector<shared_ptr<const phxqueue::config::proto::Pub>> pubs;
    ret = topic_config->GetAllPub(pubs);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("consumer_group_id %d store_id %d topic %d queue_id %d "
              "GetAllPub err %d",
              cc.consumer_group_id(), cc.store_id(), cc.topic_id(), cc.queue_id(),
              phxqueue::comm::as_integer(ret));

        return;
    }

    int pub_id{-1};
    for (const auto &pub : pubs) {
        for (const auto &queue_info_id : pub->queue_info_ids()) {
            set<int> queues;
            ret = topic_config->GetQueuesByQueueInfoID(queue_info_id, queues);
            if (phxqueue::comm::RetCode::RET_OK != ret) {
                QLErr("consumer_group_id %d store_id %d topic %d queue_id %d "
                      "GetQueuesByQueueInfoID err %d queue_info_id %d",
                      cc.consumer_group_id(), cc.store_id(), cc.topic_id(), cc.queue_id(),
                      phxqueue::comm::as_integer(ret), queue_info_id);

                continue;
            }
            for (const auto &queue : queues)
            if (cc.queue_id() == queue) {
                pub_id = pub->pub_id();
                break;
            }
        }
    }
    if (0 > pub_id) {
        QLErr("consumer_group_id %d store_id %d topic %d queue_id %d not found",
              cc.consumer_group_id(), cc.store_id(), cc.topic_id(), cc.queue_id());

        return;
    }

    string mqtt_topic_name;
    ret = PhxQueueTopicPubId2MqttTopicName(GetTopicID(), pub_id, &mqtt_topic_name);
    if (phxqueue::comm::RetCode::RET_OK != ret || mqtt_topic_name.empty()) {
        QLErr("consumer_group_id %d store_id %d topic %d queue_id %d pub_id %d "
              "PhxQueueTopicPubId2MqttTopicName err %d",
              cc.consumer_group_id(), cc.store_id(), cc.topic_id(), cc.queue_id(), pub_id,
              phxqueue::comm::as_integer(ret));

        return;
    }

    // 2. get remote topic_name -> session_ids
    TableMgr table_mgr(GetTopicID());
    uint64_t version{0uLL};
    TopicPb topic_pb;
    ret = table_mgr.GetTopicSubscribeRemote(mqtt_topic_name, &version, &topic_pb);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("consumer_group_id %d store_id %d topic %d queue_id %d "
              "GetTopicSubscribeRemote err %d topic \"%s\"",
              cc.consumer_group_id(), cc.store_id(), cc.topic_id(), cc.queue_id(),
              phxqueue::comm::as_integer(ret),
              mqtt_topic_name.c_str());

        return;
    }
    QLInfo("topic \"%s\" nr_sub %d", mqtt_topic_name.c_str(),
           topic_pb.subscribes().size());

    // 3. get remote session
    uint64_t min_prev_cursor_id(-1);
    for (const auto &subscribe_pb : topic_pb.subscribes()) {
        // 3.1. get remote session
        uint64_t version{0uLL};
        SessionPb session_pb;
        ret = table_mgr.GetSessionByClientIdRemote(subscribe_pb.client_identifier(),
                                                   &version, &session_pb);
        if (phxqueue::comm::RetCode::RET_OK != ret) {
            QLErr("consumer_group_id %d store_id %d topic %d queue_id %d "
                  "GetSessionByClientIdRemote err %d",
                  cc.consumer_group_id(), cc.store_id(), cc.topic_id(), cc.queue_id(),
                  phxqueue::comm::as_integer(ret));

            continue;
        }

        // TODO: remove
        //printf("%s:%d topic \"%s\" client \"%s\" prev_cursor_id %" PRIu64
        //       " min_prev_cursor_id %" PRIu64 "\n", __func__, __LINE__,
        //       mqtt_topic_name.c_str(), subscribe_pb.client_identifier().c_str(),
        //       session_pb.prev_cursor_id(), min_prev_cursor_id);
        QLInfo("topic \"%s\" client \"%s\" prev_cursor_id %" PRIu64 " min_prev_cursor_id %" PRIu64,
               mqtt_topic_name.c_str(), subscribe_pb.client_identifier().c_str(),
               session_pb.prev_cursor_id(), min_prev_cursor_id);
        if (static_cast<uint64_t>(-1) == min_prev_cursor_id ||
            session_pb.prev_cursor_id() < min_prev_cursor_id) {
            min_prev_cursor_id = session_pb.prev_cursor_id();
        }
    }

    // 4. set prev_cursor_id
    if (static_cast<uint64_t>(-1) != min_prev_cursor_id && pre_cursor_id != min_prev_cursor_id) {
        // TODO: remove
        printf("%s:%d topic \"%s\" prev_cursor_id %" PRIu64 " -> %" PRIu64 " (-1==%" PRIu64 ")\n",
               __func__, __LINE__, mqtt_topic_name.c_str(), pre_cursor_id, min_prev_cursor_id, (uint64_t)(-1));
        QLInfo("topic \"%s\" prev_cursor_id %" PRIu64 " -> %" PRIu64 " (-1==%" PRIu64 ")",
               mqtt_topic_name.c_str(), pre_cursor_id, min_prev_cursor_id, (uint64_t)(-1));
        pre_cursor_id = min_prev_cursor_id;
    }
}


}  // namespace mqttconsumer

}  // namespace phxqueue_phxrpc

