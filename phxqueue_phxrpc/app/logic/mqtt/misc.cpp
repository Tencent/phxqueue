/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "misc.h"

#include "phxqueue_phxrpc/producer.h"


namespace phxqueue_phxrpc {

namespace logic {

namespace mqtt {


using namespace std;


phxqueue::comm::RetCode
MqttTopicName2PhxQueueTopicPubId(const string &mqtt_topic_name,
                                 string *const phxqueue_topic_name,
                                 int *const phxqueue_topic_id,
                                 int *const phxqueue_pub_id) {
    if (!phxqueue_topic_name || !phxqueue_topic_id || !phxqueue_pub_id) {
        NLErr("out args nullptr topic \"%s\"", mqtt_topic_name.c_str());

        return phxqueue::comm::RetCode::RET_ERR_ARG;
    }

    *phxqueue_topic_name = "";
    *phxqueue_topic_id = -1;
    *phxqueue_pub_id = -1;

    vector<string> arr;
    phxqueue::comm::utils::StrSplitList(mqtt_topic_name, "/", arr);
    if (2 != arr.size()) {
        NLErr("topic \"%s\" invalid", mqtt_topic_name.c_str());

        return phxqueue::comm::RetCode::RET_ERR_RANGE_TOPIC;
    }
    char *str_end{nullptr};
    errno = 0;
    const long pub_id{strtol(arr.at(1).c_str(), &str_end, 10)};
    if ((arr.at(1).c_str() == str_end) || (0 != errno && 0 == pub_id)) {
        NLErr("pub \"%s\" invalid", arr.at(1).c_str());

        return phxqueue::comm::RetCode::RET_ERR_RANGE_TOPIC;
    }

    *phxqueue_pub_id = static_cast<int>(pub_id);

    *phxqueue_topic_name = arr.at(0).c_str();
    phxqueue::comm::RetCode ret{phxqueue::config::GlobalConfig::GetThreadInstance()->
            GetTopicIDByTopicName(*phxqueue_topic_name, *phxqueue_topic_id)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        NLErr("GetTopicIDByTopicName ret %d topic \"%s\"",
              phxqueue::comm::as_integer(ret), phxqueue_topic_name->c_str());

        return ret;
    }

    return phxqueue::comm::RetCode::RET_OK;

}

phxqueue::comm::RetCode
EnqueueMessage(const phxqueue_phxrpc::logic::mqtt::HttpPublishPb &message) {
    phxqueue::producer::ProducerOption opt;
    unique_ptr<phxqueue::producer::Producer> producer;
    producer.reset(new phxqueue_phxrpc::producer::Producer(opt));
    producer->Init();

    string phxqueue_topic_name;
    int phxqueue_topic_id{-1};
    int phxqueue_pub_id{-1};
    phxqueue::comm::RetCode ret{MqttTopicName2PhxQueueTopicPubId(
            message.mqtt_publish().topic_name(), &phxqueue_topic_name,
            &phxqueue_topic_id, &phxqueue_pub_id)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        NLErr("MqttTopicName2PhxQueueTopicPubId err %d pub_client_id \"%s\" topic \"%s\"",
              phxqueue::comm::as_integer(ret), message.pub_client_id().c_str(),
              message.mqtt_publish().topic_name().c_str());

        return ret;
    }

    string message_string;
    if (!message.SerializeToString(&message_string)) {
        NLErr("SerializeToString err pub_client_id \"%s\"", message.pub_client_id().c_str());

        return phxqueue::comm::RetCode::RET_ERR_PROTOBUF_SERIALIZE;
    }

    const uint64_t uin{0uLL};
    const int handle_id{1};
    ret = producer->Enqueue(phxqueue_topic_id, uin, handle_id,
                            message_string, phxqueue_pub_id);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        NLErr("Enqueue err %d pub_client_id \"%s\" topic %d \"%s\"",
              phxqueue::comm::as_integer(ret), message.pub_client_id().c_str(),
              phxqueue_topic_id, phxqueue_topic_name.c_str());

        return ret;
    }

    NLInfo("pub_client_id \"%s\" topic %d \"%s\"", message.pub_client_id().c_str(),
           phxqueue_topic_id, phxqueue_topic_name.c_str());

    return phxqueue::comm::RetCode::RET_OK;
}


}  // namespace mqtt

}  // namespace logic

}  // namespace phxqueue_phxrpc

