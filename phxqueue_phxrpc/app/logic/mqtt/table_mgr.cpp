/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "table_mgr.h"

#include "phxqueue/lock.h"
#include "phxqueue_phxrpc/app/lock/lock_client.h"

#include "misc.h"


namespace {


static const char *KEY_BROKER_CLIENT2SESSION_PREFIX{"__broker__:client2session:"};
static const char *KEY_BROKER_TOPIC2LOCK{"__broker__:topic2lock:"};
static const char *KEY_BROKER_TOPIC2CLIENT_PREFIX{"__broker__:topic2client:"};


}  // namespace


namespace phxqueue_phxrpc {

namespace logic {

namespace mqtt {


using namespace std;


TableMgr::TableMgr(const int topic_id) : topic_id_(topic_id) {}

TableMgr::~TableMgr() {}

phxqueue::comm::RetCode
TableMgr::FinishRemoteSession(const string &client_id,
        const phxqueue_phxrpc::logic::mqtt::SessionPb &session_pb) {
    const auto &session_attribute(session_pb.session_attribute());
    if (!session_attribute.will_topic().empty() && !session_attribute.will_message().empty()) {
        phxqueue_phxrpc::logic::mqtt::HttpPublishPb http_publish_pb;
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

phxqueue::comm::RetCode TableMgr::GetStringRemote(const string &prefix,
        const string &key, uint64_t *const version, string *const value) {
    if (!version || !value) {
        QLErr("out args nullptr prefix+key \"%s%s\"", prefix.c_str(), key.c_str());

        return phxqueue::comm::RetCode::RET_ERR_ARG;
    }

    int lock_id{-1};
    phxqueue::comm::RetCode ret{GetLockId(lock_id)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetLockId err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    phxqueue::comm::proto::GetStringRequest get_string_req;
    phxqueue::comm::proto::GetStringResponse get_string_resp;

    get_string_req.set_topic_id(topic_id_);
    get_string_req.set_lock_id(lock_id);
    get_string_req.set_key(prefix + key);

    phxqueue::lock::LockMasterClient<phxqueue::comm::proto::GetStringRequest,
            phxqueue::comm::proto::GetStringResponse> lock_master_client_get_string;
    ret = lock_master_client_get_string.ClientCall(get_string_req, get_string_resp,
            bind(&TableMgr::GetString, this, placeholders::_1, placeholders::_2));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetString err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    *version = get_string_resp.string_info().version();
    *value = get_string_resp.string_info().value();

    QLInfo("key \"%s%s\"", prefix.c_str(), key.c_str());

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode TableMgr::SetStringRemote(const string &prefix,
        const string &key, const uint64_t version, const string &value) {
    int lock_id{-1};
    phxqueue::comm::RetCode ret{GetLockId(lock_id)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetLockId err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    phxqueue::comm::proto::SetStringRequest set_string_req;
    phxqueue::comm::proto::SetStringResponse set_string_resp;

    set_string_req.set_topic_id(topic_id_);
    set_string_req.set_lock_id(lock_id);
    const auto &string_info(set_string_req.mutable_string_info());
    string_info->set_key(prefix + key);
    string_info->set_version(version);
    string_info->set_value(value);
    string_info->set_lease_time_ms(-1);

    phxqueue::lock::LockMasterClient<phxqueue::comm::proto::SetStringRequest,
            phxqueue::comm::proto::SetStringResponse> lock_master_client_set_string;
    ret = lock_master_client_set_string.ClientCall(set_string_req, set_string_resp,
            bind(&TableMgr::SetString, this, placeholders::_1, placeholders::_2));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("SetString err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    QLInfo("key \"%s%s\"", prefix.c_str(), key.c_str());

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode TableMgr::DeleteStringRemote(const string &prefix,
        const string &key, const uint64_t version) {
    int lock_id{-1};
    phxqueue::comm::RetCode ret{GetLockId(lock_id)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetLockId err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    phxqueue::comm::proto::DeleteStringRequest delete_string_req;
    phxqueue::comm::proto::DeleteStringResponse delete_string_resp;

    delete_string_req.set_topic_id(topic_id_);
    delete_string_req.set_lock_id(lock_id);
    const auto &string_key_info(delete_string_req.mutable_string_key_info());
    string_key_info->set_key(prefix + key);
    string_key_info->set_version(version);

    phxqueue::lock::LockMasterClient<phxqueue::comm::proto::DeleteStringRequest,
            phxqueue::comm::proto::DeleteStringResponse> lock_master_client_set_string;
    ret = lock_master_client_set_string.ClientCall(delete_string_req, delete_string_resp,
            bind(&TableMgr::DeleteString, this, placeholders::_1, placeholders::_2));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("DeleteString err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    QLInfo("key \"%s%s\"", prefix.c_str(), key.c_str());

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode TableMgr::LockRemote(const string &lock_key,
        const string &client_id, const uint64_t lease_time) {
    // 1. get topic_id and lock_id
    int lock_id{-1};
    string full_lock_key(KEY_BROKER_TOPIC2LOCK);
    full_lock_key += lock_key;
    phxqueue::comm::RetCode ret{GetLockId(lock_id)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetLockId err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    // 2. get lock info
    phxqueue::comm::proto::GetLockInfoRequest get_lock_info_req;
    phxqueue::comm::proto::GetLockInfoResponse get_lock_info_resp;

    get_lock_info_req.set_topic_id(topic_id_);
    get_lock_info_req.set_lock_id(lock_id);
    get_lock_info_req.set_lock_key(full_lock_key);

    phxqueue::lock::LockMasterClient<phxqueue::comm::proto::GetLockInfoRequest,
            phxqueue::comm::proto::GetLockInfoResponse> lock_master_client_get_lock_info;
    ret = lock_master_client_get_lock_info.ClientCall(get_lock_info_req, get_lock_info_resp,
            bind(&TableMgr::GetLockInfo, this, placeholders::_1, placeholders::_2));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetLockInfo err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    // 3. acquire lock
    phxqueue::comm::proto::AcquireLockRequest acquire_lock_req;
    phxqueue::comm::proto::AcquireLockResponse acquire_lock_resp;

    acquire_lock_req.set_topic_id(topic_id_);
    acquire_lock_req.set_lock_id(lock_id);
    auto &&lock_info = acquire_lock_req.mutable_lock_info();
    lock_info->set_lock_key(full_lock_key);
    lock_info->set_version(get_lock_info_resp.lock_info().version());
    lock_info->set_client_id(client_id);
    lock_info->set_lease_time_ms(lease_time);

    phxqueue::lock::LockMasterClient<phxqueue::comm::proto::AcquireLockRequest,
            phxqueue::comm::proto::AcquireLockResponse> lock_master_client_acquire_lock;
    ret = lock_master_client_acquire_lock.ClientCall(acquire_lock_req, acquire_lock_resp,
            bind(&TableMgr::AcquireLock, this, placeholders::_1, placeholders::_2));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("AcquireLock err %d", phxqueue::comm::as_integer(ret));

        return ret;
    }

    QLInfo("key \"%s%s\"", full_lock_key.c_str());

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode TableMgr::GetSessionByClientIdRemote(const string &client_id,
        uint64_t *const version, phxqueue_phxrpc::logic::mqtt::SessionPb *const session_pb) {
    if (!version || !session_pb) {
        QLErr("out args nullptr client_id \"%s\"", client_id.c_str());

        return phxqueue::comm::RetCode::RET_ERR_ARG;
    }

    // 1. get client_id -> session from lock
    string session_pb_string;
    phxqueue::comm::RetCode ret{GetStringRemote(string(KEY_BROKER_CLIENT2SESSION_PREFIX),
                                                client_id, version, &session_pb_string)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("client_id \"%s\" GetStringRemote err %d",
              client_id.c_str(), phxqueue::comm::as_integer(ret));

        return ret;
    }

    // 2. parse
    if (!session_pb->ParseFromString(session_pb_string)) {
        QLErr("client_id \"%s\" ParseFromString err", client_id.c_str());

        return phxqueue::comm::RetCode::RET_ERR_PROTOBUF_PARSE;
    }

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode TableMgr::SetSessionByClientIdRemote(const string &client_id,
        const uint64_t version, phxqueue_phxrpc::logic::mqtt::SessionPb const &session_pb) {
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
                          client_id, &version2, &session_pb_string);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("client_id \"%s\" GetStringRemote err %d",
              client_id.c_str(), phxqueue::comm::as_integer(ret));

        return ret;
    }

    // TODO: remove
    phxqueue_phxrpc::logic::mqtt::SessionPb session_pb2;
    if (!session_pb2.ParseFromString(session_pb_string)) {
        QLErr("client_id \"%s\" ParseFromString err", client_id.c_str());

        return phxqueue::comm::RetCode::RET_ERR_PROTOBUF_PARSE;
    }
    QLInfo("client_id \"%s\" ok ip %u session %" PRIx64, client_id.c_str(),
           session_pb2.session_ip(), session_pb2.session_id());

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode TableMgr::DeleteSessionByClientIdRemote(const string &client_id,
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
TableMgr::GetTopicSubscribeRemote(const string &topic_filter, uint64_t *const version,
                                  TopicPb *const topic_pb) {
    if (topic_filter.empty() || !version || !topic_pb) {
        QLErr("out args nullptr topic \"%s\"", topic_filter.c_str());

        return phxqueue::comm::RetCode::RET_ERR_ARG;
    }

    // mqtt-3.8.3-2
    if (string::npos != topic_filter.find("#") ||
        string::npos != topic_filter.find("+")) {
        QLErr("not support wildcards topic \"%s\"", topic_filter.c_str());

        return phxqueue::comm::RetCode::RET_ERR_ARG;
    }

    string topic_pb_string;
    phxqueue::comm::RetCode ret{GetStringRemote(string(KEY_BROKER_TOPIC2CLIENT_PREFIX),
            topic_filter, version, &topic_pb_string)};
    if (phxqueue::comm::RetCode::RET_ERR_KEY_NOT_EXIST != ret) {
        if (phxqueue::comm::RetCode::RET_OK != ret) {
            QLErr("GetStringRemote err %d topic \"%s\"", phxqueue::comm::as_integer(ret),
                  topic_filter.c_str());

            return ret;
        }

        if (!topic_pb->ParseFromString(topic_pb_string)) {
            QLErr("ParseFromString err topic \"%s\"", topic_filter.c_str());

            return phxqueue::comm::RetCode::RET_ERR_PROTOBUF_PARSE;
        }
    }

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode
TableMgr::SetTopicSubscribeRemote(const string &topic_filter, const uint64_t version,
                                  const TopicPb &topic_pb) {
    // mqtt-4.7.1-1
    if (string::npos != topic_filter.find("#") ||
        string::npos != topic_filter.find("+")) {
        QLErr("not allowed wildcards topic \"%s\"", topic_filter.c_str());

        return phxqueue::comm::RetCode::RET_ERR_ARG;
    }

    string topic_pb_string;
    if (!topic_pb.SerializeToString(&topic_pb_string)) {
        QLErr("SerializeToString err topic \"%s\"", topic_filter.c_str());

        return phxqueue::comm::RetCode::RET_ERR_PROTOBUF_SERIALIZE;
    }

    phxqueue::comm::RetCode ret{SetStringRemote(string(KEY_BROKER_TOPIC2CLIENT_PREFIX),
            topic_filter, version, move(topic_pb_string))};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("SetStringRemote err %d topic \"%s\"", phxqueue::comm::as_integer(ret),
              topic_filter.c_str());

        return ret;
    }

    return phxqueue::comm::RetCode::RET_OK;
}

phxqueue::comm::RetCode
TableMgr::GetLockInfo(const phxqueue::comm::proto::GetLockInfoRequest &req,
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
TableMgr::AcquireLock(const phxqueue::comm::proto::AcquireLockRequest &req,
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
TableMgr::GetString(const phxqueue::comm::proto::GetStringRequest &req,
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
TableMgr::SetString(const phxqueue::comm::proto::SetStringRequest &req,
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
TableMgr::DeleteString(const phxqueue::comm::proto::DeleteStringRequest &req,
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

phxqueue::comm::RetCode TableMgr::GetLockId(int &lock_id) {
    shared_ptr<const phxqueue::config::LockConfig> lock_config;
    phxqueue::comm::RetCode ret{phxqueue::config::GlobalConfig::GetThreadInstance()->GetLockConfig(topic_id_, lock_config)};
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetLockConfig ret %d topic_id %u", phxqueue::comm::as_integer(ret), topic_id_);

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


}  // namespace mqtt

}  // namespace logic

}  // namespace phxqueue_phxrpc

