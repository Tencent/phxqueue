/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include "phxqueue/comm.h"

#include "mqtt.pb.h"


namespace phxqueue_phxrpc {

namespace logic {

namespace mqtt {


phxqueue::comm::RetCode
MqttTopicName2PhxQueueTopicPubId(const std::string &mqtt_topic_name,
                                 std::string *const phxqueue_topic_name,
                                 int *const phxqueue_topic_id,
                                 int *const phxqueue_pub_id);

phxqueue::comm::RetCode
PhxQueueTopicPubId2MqttTopicName(const int phxqueue_topic_id,
                                 const int phxqueue_pub_id,
                                 std::string *const mqtt_topic_name);

phxqueue::comm::RetCode
PhxQueueTopicNamePubId2MqttTopicName(const std::string &phxqueue_topic_name,
                                     const int phxqueue_pub_id,
                                     std::string *const mqtt_topic_name);

phxqueue::comm::RetCode
EnqueueMessage(const phxqueue_phxrpc::logic::mqtt::HttpPublishPb &message);


}  // namespace mqtt

}  // namespace logic

}  // namespace phxqueue_phxrpc

