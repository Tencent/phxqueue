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

#pragma once


namespace phxrpc {


class BaseTcpStream;
class ClientMonitor;


}  // namespace phxrpc


namespace phxqueue_phxrpc {

namespace mqttbroker {


class MqttConnect;
class MqttConnack;
class MqttPublish;
class MqttPuback;
class MqttPubrec;
class MqttPubrel;
class MqttPubcomp;
class MqttSubscribe;
class MqttSuback;
class MqttUnsubscribe;
class MqttUnsuback;
class MqttPingreq;
class MqttPingresp;
class MqttDisconnect;

class MqttClient {
  public:
    struct MqttStat {
        bool send_error_{false};
        bool recv_error_{false};

        MqttStat() = default;

        MqttStat(bool send_error, bool recv_error)
                : send_error_(send_error), recv_error_(recv_error) {
        }
    };

    // @return true: socket ok; false: socket error
    static int Connect(phxrpc::BaseTcpStream &socket, const MqttConnect &req,
                       MqttConnack &resp, MqttStat &mqtt_stat);
    static int Connect(phxrpc::BaseTcpStream &socket, const MqttConnect &req,
                       MqttConnack &resp);

    // @return true: socket ok; false: socket error
    static int Publish(phxrpc::BaseTcpStream &socket, const MqttPublish &req,
                       MqttStat &mqtt_stat);
    static int Publish(phxrpc::BaseTcpStream &socket, const MqttPublish &req);

    // @return true: socket ok; false: socket error
    static int Puback(phxrpc::BaseTcpStream &socket, const MqttPuback &req,
                      MqttStat &mqtt_stat);
    static int Puback(phxrpc::BaseTcpStream &socket, const MqttPuback &req);

    // @return true: socket ok; false: socket error
    static int Pubrec(phxrpc::BaseTcpStream &socket, const MqttPubrec &req,
                       MqttStat &mqtt_stat);
    static int Pubrec(phxrpc::BaseTcpStream &socket, const MqttPubrec &req);

    // @return true: socket ok; false: socket error
    static int Pubrel(phxrpc::BaseTcpStream &socket, const MqttPubrel &req,
                       MqttStat &mqtt_stat);
    static int Pubrel(phxrpc::BaseTcpStream &socket, const MqttPubrel &req);

    // @return true: socket ok; false: socket error
    static int Pubcomp(phxrpc::BaseTcpStream &socket, const MqttPubcomp &req,
                       MqttStat &mqtt_stat);
    static int Pubcomp(phxrpc::BaseTcpStream &socket, const MqttPubcomp &req);

    // @return true: socket ok; false: socket error
    static int Subscribe(phxrpc::BaseTcpStream &socket, const MqttSubscribe &req,
                         MqttSuback &resp, MqttStat &mqtt_stat);
    static int Subscribe(phxrpc::BaseTcpStream &socket, const MqttSubscribe &req,
                         MqttSuback &resp);

    // @return true: socket ok; false: socket error
    static int Unsubscribe(phxrpc::BaseTcpStream &socket, const MqttUnsubscribe &req,
                           MqttUnsuback &resp, MqttStat &mqtt_stat);
    static int Unsubscribe(phxrpc::BaseTcpStream &socket, const MqttUnsubscribe &req,
                           MqttUnsuback &resp);

    // @return true: socket ok; false: socket error
    static int Ping(phxrpc::BaseTcpStream &socket, const MqttPingreq &req,
                    MqttPingresp &resp, MqttStat &mqtt_stat);
    static int Ping(phxrpc::BaseTcpStream &socket, const MqttPingreq &req,
                    MqttPingresp &resp);

    // @return true: socket ok; false: socket error
    static int Disconnect(phxrpc::BaseTcpStream &socket, const MqttDisconnect &req,
                          MqttStat &mqtt_stat);
    static int Disconnect(phxrpc::BaseTcpStream &socket, const MqttDisconnect &req);

  private:
    MqttClient();
};


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

