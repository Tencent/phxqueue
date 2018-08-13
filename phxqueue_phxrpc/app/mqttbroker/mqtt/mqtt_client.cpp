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

#include "mqtt_client.h"

#include <cassert>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "phxrpc/file/log_utils.h"
#include "phxrpc/network/socket_stream_base.h"

#include "mqtt_msg.h"
#include "mqtt_msg_handler.h"


namespace {


phxrpc::ReturnCode DoMethod(phxrpc::BaseTcpStream &socket,
                            const phxqueue_phxrpc::mqttbroker::MqttMessage *const req,
                            phxqueue_phxrpc::mqttbroker::MqttMessage *const resp,
                            phxqueue_phxrpc::mqttbroker::MqttClient::MqttStat &mqtt_stat) {
    phxrpc::ReturnCode ret{phxqueue_phxrpc::mqttbroker::MqttMessageHandler::SendMessage(socket, req)};
    if (phxrpc::ReturnCode::OK != ret) {
        if (phxrpc::ReturnCode::ERROR_SOCKET_STREAM_NORMAL_CLOSED != ret) {
            mqtt_stat.send_error_ = true;
            phxrpc::log(LOG_ERR, "SendMessage err %d", static_cast<int>(ret));
        }

        return ret;
    }

    if (!socket.flush().good()) {
        phxrpc::log(LOG_ERR, "socket err %d", socket.LastError());

        return static_cast<phxrpc::ReturnCode>(socket.LastError());
    }

    if (!resp->fake()) {
        ret = phxqueue_phxrpc::mqttbroker::MqttMessageHandler::RecvMessage(socket, resp);
        if (phxrpc::ReturnCode::OK != ret) {
            if (phxrpc::ReturnCode::ERROR_SOCKET_STREAM_NORMAL_CLOSED != ret) {
                mqtt_stat.recv_error_ = true;
                phxrpc::log(LOG_ERR, "RecvMessage err %d", static_cast<int>(ret));
            }

            return ret;
        }
    }

    return ret;
}

phxrpc::ReturnCode DoMethod(phxrpc::BaseTcpStream &socket,
                            const phxqueue_phxrpc::mqttbroker::MqttMessage *const req,
                            phxqueue_phxrpc::mqttbroker::MqttMessage *const resp) {
    phxqueue_phxrpc::mqttbroker::MqttClient::MqttStat mqtt_stat;
    return DoMethod(socket, req, resp, mqtt_stat);
}


}  // namespace


namespace phxqueue_phxrpc {

namespace mqttbroker {


int MqttClient::Connect(phxrpc::BaseTcpStream &socket, const MqttConnect &req,
                        MqttConnack &resp, MqttStat &mqtt_stat) {
    // TODO: remove
    printf("%s client_identifier %s\n", __func__, req.client_identifier().c_str());
    return static_cast<int>(DoMethod(socket, &req, &resp, mqtt_stat));
}

int MqttClient::Connect(phxrpc::BaseTcpStream &socket, const MqttConnect &req,
                        MqttConnack &resp) {
    return static_cast<int>(DoMethod(socket, &req, &resp));
}

int MqttClient::Publish(phxrpc::BaseTcpStream &socket, const MqttPublish &req,
                        MqttStat &mqtt_stat) {
    MqttFakeResponse resp;
    return static_cast<int>(DoMethod(socket, &req, &resp, mqtt_stat));
}

int MqttClient::Publish(phxrpc::BaseTcpStream &socket, const MqttPublish &req) {
    MqttFakeResponse resp;
    return static_cast<int>(DoMethod(socket, &req, &resp));
}

int MqttClient::Puback(phxrpc::BaseTcpStream &socket, const MqttPuback &req,
                       MqttStat &mqtt_stat) {
    MqttFakeResponse resp;
    return static_cast<int>(DoMethod(socket, &req, &resp, mqtt_stat));
}

int MqttClient::Puback(phxrpc::BaseTcpStream &socket, const MqttPuback &req) {
    MqttFakeResponse resp;
    return static_cast<int>(DoMethod(socket, &req, &resp));
}

int MqttClient::Pubrec(phxrpc::BaseTcpStream &socket, const MqttPubrec &req,
                       MqttStat &mqtt_stat) {
    MqttFakeResponse resp;
    return static_cast<int>(DoMethod(socket, &req, &resp, mqtt_stat));
}

int MqttClient::Pubrec(phxrpc::BaseTcpStream &socket, const MqttPubrec &req) {
    MqttFakeResponse resp;
    return static_cast<int>(DoMethod(socket, &req, &resp));
}

int MqttClient::Pubrel(phxrpc::BaseTcpStream &socket, const MqttPubrel &req,
                       MqttStat &mqtt_stat) {
    MqttFakeResponse resp;
    return static_cast<int>(DoMethod(socket, &req, &resp, mqtt_stat));
}

int MqttClient::Pubrel(phxrpc::BaseTcpStream &socket, const MqttPubrel &req) {
    MqttFakeResponse resp;
    return static_cast<int>(DoMethod(socket, &req, &resp));
}

int MqttClient::Pubcomp(phxrpc::BaseTcpStream &socket, const MqttPubcomp &req,
                       MqttStat &mqtt_stat) {
    MqttFakeResponse resp;
    return static_cast<int>(DoMethod(socket, &req, &resp, mqtt_stat));
}

int MqttClient::Pubcomp(phxrpc::BaseTcpStream &socket, const MqttPubcomp &req) {
    MqttFakeResponse resp;
    return static_cast<int>(DoMethod(socket, &req, &resp));
}

int MqttClient::Subscribe(phxrpc::BaseTcpStream &socket, const MqttSubscribe &req,
                          MqttSuback &resp, MqttStat &mqtt_stat) {
    return static_cast<int>(DoMethod(socket, &req, &resp, mqtt_stat));
}

int MqttClient::Subscribe(phxrpc::BaseTcpStream &socket, const MqttSubscribe &req,
                          MqttSuback &resp) {
    return static_cast<int>(DoMethod(socket, &req, &resp));
}

int MqttClient::Unsubscribe(phxrpc::BaseTcpStream &socket, const MqttUnsubscribe &req,
                            MqttUnsuback &resp, MqttStat &mqtt_stat) {
    return static_cast<int>(DoMethod(socket, &req, &resp, mqtt_stat));
}

int MqttClient::Unsubscribe(phxrpc::BaseTcpStream &socket, const MqttUnsubscribe &req,
                            MqttUnsuback &resp) {
    return static_cast<int>(DoMethod(socket, &req, &resp));
}

int MqttClient::Ping(phxrpc::BaseTcpStream &socket, const MqttPingreq &req,
                     MqttPingresp &resp, MqttStat &mqtt_stat) {
    return static_cast<int>(DoMethod(socket, &req, &resp, mqtt_stat));
}

int MqttClient::Ping(phxrpc::BaseTcpStream &socket, const MqttPingreq &req,
                     MqttPingresp &resp) {
    return static_cast<int>(DoMethod(socket, &req, &resp));
}

int MqttClient::Disconnect(phxrpc::BaseTcpStream &socket, const MqttDisconnect &req,
                           MqttStat &mqtt_stat) {
    MqttFakeResponse resp;
    return static_cast<int>(DoMethod(socket, &req, &resp, mqtt_stat));
}

int MqttClient::Disconnect(phxrpc::BaseTcpStream &socket, const MqttDisconnect &req) {
    MqttFakeResponse resp;
    return static_cast<int>(DoMethod(socket, &req, &resp));
}


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

