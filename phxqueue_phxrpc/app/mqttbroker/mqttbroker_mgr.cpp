/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "mqttbroker_mgr.h"

#include "phxqueue/lock.h"

#include "phxqueue_phxrpc/app/lock/lock_client.h"
#include "phxqueue_phxrpc/producer.h"

#include "mqttbroker_server_config.h"


namespace phxqueue_phxrpc {

namespace mqttbroker {


using namespace std;


constexpr char *KEY_BROKER_CLIENT2SESSION_PREFIX{"__broker__:client2session:"};
constexpr char *KEY_BROKER_TOPIC2LOCK{"__broker__:topic2lock:"};


MqttBrokerMgr::MqttBrokerMgr(const MqttBrokerServerConfig *const config) : config_(config) {}

MqttBrokerMgr::~MqttBrokerMgr() {}

phxqueue::comm::RetCode
MqttBrokerMgr::FinishRemoteSession(const string &client_id,
        const phxqueue_phxrpc::mqttbroker::SessionPb &session_pb) {
    const auto &session_attribute(session_pb.session_attribute());
    if (!session_attribute.will_topic().empty() && !session_attribute.will_message().empty()) {
        phxqueue_phxrpc::mqttbroker::HttpPublishPb http_publish_pb;
        http_publish_pb.set_pub_client_id(string("__will__:") + client_id);
        auto &&mqtt_publish_pb(http_publish_pb.mutable_mqtt_publish());
        mqtt_publish_pb->set_qos(session_attribute.will_qos());
        mqtt_publish_pb->set_retain(session_attribute.will_retain());
        mqtt_publish_pb->set_packet_identifier(0);
        mqtt_publish_pb->set_topic_name(session_attribute.will_topic());
        mqtt_publish_pb->set_data(session_attribute.will_message());
        phxqueue::comm::RetCode ret{EnqueueMessage(http_publish_pb)};
        // TODO: ignore ret?
    }

    if (session_attribute.clean_session()) {
        // 1. delete remote session
        phxqueue::comm::RetCode ret{DeleteSessionByClientIdRemote(client_id, -1)};
        if (phxqueue::comm::RetCode::RET_OK != ret) {
            QLErr("session_id %" PRIx64 " DeleteString err %d",
                  session_pb.session_id(), phxqueue::comm::as_integer(ret));

            return ret;
        }
    } else {
        // 2.1. clear remote session_ip
        auto session_pb2(session_pb);
        session_pb2.set_session_ip(0);

        // 2.2. set remote session
        phxqueue::comm::RetCode ret{SetSessionByClientIdRemote(client_id, -1, session_pb2)};
        if (phxqueue::comm::RetCode::RET_OK != ret) {
            QLErr("session_id %" PRIx64 " client_id \"%s\" SetSessionByClientIdRemote err %d",
                  session_pb.session_id(), client_id.c_str(),
                  phxqueue::comm::as_integer(ret));

            return ret;
        }
    }

    QLInfo("session_id %" PRIx64 " client_id \"%s\"", session_pb.session_id(), client_id.c_str());

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode
MqttBrokerMgr::EnqueueMessage(const phxqueue_phxrpc::mqttbroker::HttpPublishPb &message) {
    phxqueue::producer::ProducerOption opt;
    unique_ptr<phxqueue::producer::Producer> producer;
    producer.reset(new phxqueue_phxrpc::producer::Producer(opt));
    producer->Init();

    const uint64_t uin{0};
    const int handle_id{1};
    vector<string> arr;
    phxqueue::comm::utils::StrSplitList(message.mqtt_publish().topic_name(), "/", arr);
    if (2 != arr.size()) {
        QLErr("pub_client_id \"%s\" topic \"%s\" invalid", message.pub_client_id().c_str(),
              message.mqtt_publish().topic_name().c_str());

        return phxqueue::comm::RetCode::RET_ERR_RANGE_TOPIC;
    }
    char *str_end{nullptr};
    errno = 0;
    const long pub_id{strtol(arr.at(1).c_str(), &str_end, 10)};
    if ((arr.at(1).c_str() == str_end) || (0 != errno && 0 == pub_id)) {
        QLErr("pub_client_id \"%s\" pub \"%s\" invalid", message.pub_client_id().c_str(),
              arr.at(1).c_str());

        return phxqueue::comm::RetCode::RET_ERR_RANGE_TOPIC;
    }

    int phxqueue_topic_id{0};
    string phxqueue_topic_name(arr.at(0).c_str());
    phxqueue::comm::RetCode ret{phxqueue::config::GlobalConfig::GetThreadInstance()->
            GetTopicIDByTopicName(phxqueue_topic_name, phxqueue_topic_id)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetTopicIDByTopicName ret %d pub_client_id \"%s\" topic \"%s\"",
              phxqueue::comm::as_integer(ret), message.pub_client_id().c_str(),
              phxqueue_topic_name.c_str());

        return ret;
    }
    string message_string;
    if (!message.SerializeToString(&message_string)) {
        QLErr("SerializeToString err pub_client_id \"%s\"", message.pub_client_id().c_str());

        return phxqueue::comm::RetCode::RET_ERR_PROTOBUF_SERIALIZE;
    }

    ret = producer->Enqueue(phxqueue_topic_id, uin, handle_id,
                            message_string, static_cast<int>(pub_id));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("Enqueue err %d pub_client_id \"%s\" topic %d \"%s\"",
              phxqueue::comm::as_integer(ret), message.pub_client_id().c_str(),
              phxqueue_topic_id, phxqueue_topic_name.c_str());

        return ret;
    }

    QLInfo("pub_client_id \"%s\" topic %d \"%s\"", message.pub_client_id().c_str(),
           phxqueue_topic_id, phxqueue_topic_name.c_str());

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode MqttBrokerMgr::GetStringRemote(const string &prefix,
        const string &key, uint64_t &version, string &value) {
    int topic_id{-1};
    int lock_id{-1};
    phxqueue::comm::RetCode ret{GetTopicIdAndLockId(topic_id, lock_id)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetTopicIdAndLockId err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    phxqueue::comm::proto::GetStringRequest get_string_req;
    phxqueue::comm::proto::GetStringResponse get_string_resp;

    get_string_req.set_topic_id(topic_id);
    get_string_req.set_lock_id(lock_id);
    get_string_req.set_key(prefix + key);

    phxqueue::lock::LockMasterClient<phxqueue::comm::proto::GetStringRequest,
            phxqueue::comm::proto::GetStringResponse> lock_master_client_get_string;
    ret = lock_master_client_get_string.ClientCall(get_string_req, get_string_resp,
            bind(&MqttBrokerMgr::GetString, this, placeholders::_1, placeholders::_2));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetString err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    version = get_string_resp.string_info().version();
    value = get_string_resp.string_info().value();

    QLInfo("key \"%s%s\"", prefix.c_str(), key.c_str());

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode MqttBrokerMgr::SetStringRemote(const string &prefix,
        const string &key, const uint64_t version, const string &value) {
    int topic_id{-1};
    int lock_id{-1};
    phxqueue::comm::RetCode ret{GetTopicIdAndLockId(topic_id, lock_id)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetTopicIdAndLockId err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    phxqueue::comm::proto::SetStringRequest set_string_req;
    phxqueue::comm::proto::SetStringResponse set_string_resp;

    set_string_req.set_topic_id(topic_id);
    set_string_req.set_lock_id(lock_id);
    const auto &string_info(set_string_req.mutable_string_info());
    string_info->set_key(prefix + key);
    string_info->set_version(version);
    string_info->set_value(value);
    string_info->set_lease_time_ms(-1);

    phxqueue::lock::LockMasterClient<phxqueue::comm::proto::SetStringRequest,
            phxqueue::comm::proto::SetStringResponse> lock_master_client_set_string;
    ret = lock_master_client_set_string.ClientCall(set_string_req, set_string_resp,
            bind(&MqttBrokerMgr::SetString, this, placeholders::_1, placeholders::_2));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("SetString err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    QLInfo("key \"%s%s\"", prefix.c_str(), key.c_str());

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode MqttBrokerMgr::DeleteStringRemote(const string &prefix,
        const string &key, const uint64_t version) {
    int topic_id{-1};
    int lock_id{-1};
    phxqueue::comm::RetCode ret{GetTopicIdAndLockId(topic_id, lock_id)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetTopicIdAndLockId err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    phxqueue::comm::proto::DeleteStringRequest delete_string_req;
    phxqueue::comm::proto::DeleteStringResponse delete_string_resp;

    delete_string_req.set_topic_id(topic_id);
    delete_string_req.set_lock_id(lock_id);
    const auto &string_key_info(delete_string_req.mutable_string_key_info());
    string_key_info->set_key(prefix + key);
    string_key_info->set_version(version);

    phxqueue::lock::LockMasterClient<phxqueue::comm::proto::DeleteStringRequest,
            phxqueue::comm::proto::DeleteStringResponse> lock_master_client_set_string;
    ret = lock_master_client_set_string.ClientCall(delete_string_req, delete_string_resp,
            bind(&MqttBrokerMgr::DeleteString, this, placeholders::_1, placeholders::_2));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("DeleteString err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    QLInfo("key \"%s%s\"", prefix.c_str(), key.c_str());

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode MqttBrokerMgr::LockRemote(const string &lock_key,
        const string &client_id, const uint64_t lease_time) {
    // 1. get topic_id and lock_id
    int topic_id{-1};
    int lock_id{-1};
    string full_lock_key(KEY_BROKER_TOPIC2LOCK);
    full_lock_key += lock_key;
    phxqueue::comm::RetCode ret{GetTopicIdAndLockId(topic_id, lock_id)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetTopicIdAndLockId err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    // 2. get lock info
    phxqueue::comm::proto::GetLockInfoRequest get_lock_info_req;
    phxqueue::comm::proto::GetLockInfoResponse get_lock_info_resp;

    get_lock_info_req.set_topic_id(topic_id);
    get_lock_info_req.set_lock_id(lock_id);
    get_lock_info_req.set_lock_key(full_lock_key);

    phxqueue::lock::LockMasterClient<phxqueue::comm::proto::GetLockInfoRequest,
            phxqueue::comm::proto::GetLockInfoResponse> lock_master_client_get_lock_info;
    ret = lock_master_client_get_lock_info.ClientCall(get_lock_info_req, get_lock_info_resp,
            bind(&MqttBrokerMgr::GetLockInfo, this, placeholders::_1, placeholders::_2));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetLockInfo err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    // 3. acquire lock
    phxqueue::comm::proto::AcquireLockRequest acquire_lock_req;
    phxqueue::comm::proto::AcquireLockResponse acquire_lock_resp;

    acquire_lock_req.set_topic_id(topic_id);
    acquire_lock_req.set_lock_id(lock_id);
    auto &&lock_info = acquire_lock_req.mutable_lock_info();
    lock_info->set_lock_key(full_lock_key);
    lock_info->set_version(get_lock_info_resp.lock_info().version());
    lock_info->set_client_id(client_id);
    lock_info->set_lease_time_ms(lease_time);

    phxqueue::lock::LockMasterClient<phxqueue::comm::proto::AcquireLockRequest,
            phxqueue::comm::proto::AcquireLockResponse> lock_master_client_acquire_lock;
    ret = lock_master_client_acquire_lock.ClientCall(acquire_lock_req, acquire_lock_resp,
            bind(&MqttBrokerMgr::AcquireLock, this, placeholders::_1, placeholders::_2));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("AcquireLock err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    QLInfo("key \"%s%s\"", full_lock_key.c_str());

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode MqttBrokerMgr::GetSessionByClientIdRemote(const string &client_id,
        uint64_t &version, phxqueue_phxrpc::mqttbroker::SessionPb &session_pb) {
    // 1. get client_id -> session from lock
    string session_pb_string;
    phxqueue::comm::RetCode ret{GetStringRemote(string(KEY_BROKER_CLIENT2SESSION_PREFIX),
                                                client_id, version, session_pb_string)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("client_id \"%s\" GetStringRemote err %d",
              client_id.c_str(), phxqueue::comm::as_integer(ret));

        return ret;
    }

    // 2. parse
    if (!session_pb.ParseFromString(session_pb_string)) {
        QLErr("client_id \"%s\" ParseFromString err", client_id.c_str());

        return phxqueue::comm::RetCode::RET_ERR_PROTOBUF_PARSE;
    }

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode MqttBrokerMgr::SetSessionByClientIdRemote(const string &client_id,
        const uint64_t version, phxqueue_phxrpc::mqttbroker::SessionPb const &session_pb) {
    // 1. serialize
    string session_pb_string;
    if (!session_pb.SerializeToString(&session_pb_string)) {
        QLErr("client_id \"%s\" SerializeToString err", client_id.c_str());

        return phxqueue::comm::RetCode::RET_ERR_PROTOBUF_SERIALIZE;
    }

    // 2. set client_id -> session to lock
    phxqueue::comm::RetCode ret{SetStringRemote(string(KEY_BROKER_CLIENT2SESSION_PREFIX),
                                                client_id, version, session_pb_string)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("client_id \"%s\" SetStringRemote err %d",
              client_id.c_str(), phxqueue::comm::as_integer(ret));

        return ret;
    }

    session_pb_string.clear();
    uint64_t version2;
    ret = GetStringRemote(string(KEY_BROKER_CLIENT2SESSION_PREFIX),
                          client_id, version2, session_pb_string);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("client_id \"%s\" GetStringRemote err %d",
              client_id.c_str(), phxqueue::comm::as_integer(ret));

        return ret;
    }

    // TODO: remove
    phxqueue_phxrpc::mqttbroker::SessionPb session_pb2;
    if (!session_pb2.ParseFromString(session_pb_string)) {
        QLErr("client_id \"%s\" ParseFromString err", client_id.c_str());

        return phxqueue::comm::RetCode::RET_ERR_PROTOBUF_PARSE;
    }
    QLInfo("client_id \"%s\" ok ip %u session %" PRIx64, client_id.c_str(),
           session_pb2.session_ip(), session_pb2.session_id());

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode MqttBrokerMgr::DeleteSessionByClientIdRemote(const string &client_id,
                                                                     const uint64_t version) {
    phxqueue::comm::RetCode ret{DeleteStringRemote(string(KEY_BROKER_CLIENT2SESSION_PREFIX),
                                                   client_id, version)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("client_id \"%s\" DeleteStringRemote err %d",
              client_id.c_str(), phxqueue::comm::as_integer(ret));

        return ret;
    }

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode
MqttBrokerMgr::GetLockInfo(const phxqueue::comm::proto::GetLockInfoRequest &req,
                           phxqueue::comm::proto::GetLockInfoResponse &resp) {
    thread_local LockClient lock_client;
    auto ret = lock_client.ProtoGetLockInfo(req, resp);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("ProtoGetLockInfo ret %d", phxqueue::comm::as_integer(ret));

        return ret;
    }
    QLInfo("ProtoGetLockInfo ok");

    return ret;
}

phxqueue::comm::RetCode
MqttBrokerMgr::AcquireLock(const phxqueue::comm::proto::AcquireLockRequest &req,
                           phxqueue::comm::proto::AcquireLockResponse &resp) {
    thread_local LockClient lock_client;
    auto ret = lock_client.ProtoAcquireLock(req, resp);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("ProtoAcquireLock ret %d", phxqueue::comm::as_integer(ret));

        return ret;
    }
    QLInfo("ProtoAcquireLock ok");

    return ret;
}

phxqueue::comm::RetCode
MqttBrokerMgr::GetString(const phxqueue::comm::proto::GetStringRequest &req,
                         phxqueue::comm::proto::GetStringResponse &resp) {
    thread_local LockClient lock_client;
    const auto ret(lock_client.ProtoGetString(req, resp));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("ProtoGetString ret %d", phxqueue::comm::as_integer(ret));

        return ret;
    }
    QLInfo("ProtoGetString ok");

    return ret;
}

phxqueue::comm::RetCode
MqttBrokerMgr::SetString(const phxqueue::comm::proto::SetStringRequest &req,
                         phxqueue::comm::proto::SetStringResponse &resp) {
    thread_local LockClient lock_client;
    const auto ret(lock_client.ProtoSetString(req, resp));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("ProtoSetString ret %d", phxqueue::comm::as_integer(ret));

        return ret;
    }
    QLInfo("ProtoSetString ok");

    return ret;
}

phxqueue::comm::RetCode
MqttBrokerMgr::DeleteString(const phxqueue::comm::proto::DeleteStringRequest &req,
                            phxqueue::comm::proto::DeleteStringResponse &resp) {
    thread_local LockClient lock_client;
    const auto ret(lock_client.ProtoDeleteString(req, resp));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("ProtoDeleteString ret %d", phxqueue::comm::as_integer(ret));

        return ret;
    }
    QLInfo("ProtoDeleteString ok");

    return ret;
}

phxqueue::comm::RetCode MqttBrokerMgr::GetTopicIdAndLockId(int &topic_id, int &lock_id) {
    topic_id = config_->GetTopicID();

    shared_ptr<const phxqueue::config::LockConfig> lock_config;
    phxqueue::comm::RetCode ret{phxqueue::config::GlobalConfig::GetThreadInstance()->GetLockConfig(topic_id, lock_config)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetLockConfig ret %d topic_id %u", phxqueue::comm::as_integer(ret), topic_id);

        return ret;
    }

    set<int> lock_ids;
    ret = lock_config->GetAllLockID(lock_ids);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetAllLockID ret %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    if (lock_ids.empty()) {
        QLErr("lock_ids empty");

        return phxqueue::comm::RetCode::RET_ERR_RANGE_LOCK;
    }

    lock_id = *lock_ids.begin();

    return phxqueue::comm::RetCode::RET_OK;
}


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

