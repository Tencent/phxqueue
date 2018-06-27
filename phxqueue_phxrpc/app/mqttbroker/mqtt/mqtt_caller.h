/*
Tencent is pleased to support the open source community by making
phxqueue_phxrpc::mqttbroker available.
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

#pragma once

#include "phxrpc/rpc.h"
#include "phxrpc/rpc/phxrpc.pb.h"

#include "mqtt_client.h"
#include "mqtt_msg.h"


namespace google {

namespace protobuf {


class Empty;
class MessageLite;


}  // namespace protobuf

}  // namespace google


namespace phxrpc {


class BaseTcpStream;


}  // namespace phxrpc


namespace phxqueue_phxrpc {

namespace mqttbroker {


class MqttConnectPb;
class MqttConnackPb;
class MqttPublishPb;
class MqttPubackPb;
class MqttPubrecPb;
class MqttPubrelPb;
class MqttPubcompPb;
class MqttSubscribePb;
class MqttSubackPb;
class MqttUnsubscribePb;
class MqttUnsubackPb;
class MqttPingreqPb;
class MqttPingrespPb;
class MqttDisconnectPb;

class MqttCaller {
  public:
    MqttCaller(phxrpc::BaseTcpStream &socket, phxrpc::ClientMonitor &client_monitor);

    virtual ~MqttCaller();

    int PhxMqttConnectCall(const MqttConnectPb &req, MqttConnackPb *resp);
    int PhxMqttPublishCall(const MqttPublishPb &req, google::protobuf::Empty *resp);
    int PhxMqttPubackCall(const MqttPubackPb &req, google::protobuf::Empty *resp);
    int PhxMqttPubrecCall(const MqttPubrecPb &req, google::protobuf::Empty *resp);
    int PhxMqttPubrelCall(const MqttPubrelPb &req, google::protobuf::Empty *resp);
    int PhxMqttPubcompCall(const MqttPubcompPb &req, google::protobuf::Empty *resp);
    int PhxMqttSubscribeCall(const MqttSubscribePb &req, MqttSubackPb *resp);
    int PhxMqttUnsubscribeCall(const MqttUnsubscribePb &req, MqttUnsubackPb *resp);
    int PhxMqttPingCall(const MqttPingreqPb &req, MqttPingrespPb *resp);
    int PhxMqttDisconnectCall(const MqttDisconnectPb &req, google::protobuf::Empty *resp);

    void set_uri(const char *const uri, const int cmd_id);

    void SetKeepAlive(const bool keep_alive);

  private:
    void MonitorReport(phxrpc::ClientMonitor &client_monitor, bool send_error,
                       bool recv_error, size_t send_size, size_t recv_size,
                       uint64_t call_begin, uint64_t call_end);

    phxrpc::BaseTcpStream &socket_;
    phxrpc::ClientMonitor &client_monitor_;
    int cmd_id_;
};


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

