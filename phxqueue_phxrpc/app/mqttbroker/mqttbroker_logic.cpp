/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "mqttbroker_logic.h"

#include <algorithm>


namespace phxqueue_phxrpc {

namespace mqttbroker {


using namespace phxqueue_phxrpc::logic::mqtt;
using namespace std;


phxqueue::comm::RetCode
AddSubscribe(const string &client_id, const uint32_t qos, TopicPb *const topic_pb) {
    if (!topic_pb) {
        return phxqueue::comm::RetCode::RET_ERR_ARG;
    }

    const auto &subscribe_pb(topic_pb->add_subscribes());
    subscribe_pb->set_client_identifier(client_id);
    subscribe_pb->set_qos(qos);

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode
RemoveSubscribe(const string &client_id, TopicPb *const topic_pb) {
    if (!topic_pb) {
        return phxqueue::comm::RetCode::RET_ERR_ARG;
    }

    TopicPb tmp_topic_pb;

    for_each(topic_pb->subscribes().cbegin(), topic_pb->subscribes().cend(),
            [&](const SubscribePb &subscribe_pb) {
        if (subscribe_pb.client_identifier() != client_id) {
            const auto &new_subscribe(tmp_topic_pb.add_subscribes());
            new_subscribe->CopyFrom(subscribe_pb);
        }
    });

    *topic_pb = tmp_topic_pb;

    return phxqueue::comm::RetCode::RET_OK;
}


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

