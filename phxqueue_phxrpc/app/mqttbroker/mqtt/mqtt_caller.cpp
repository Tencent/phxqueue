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

#include "mqtt_caller.h"

#include <syslog.h>

#include <google/protobuf/message_lite.h>

#include "phxrpc/file.h"
#include "phxrpc/network.h"
#include "phxrpc/rpc/monitor_factory.h"

#include "../mqttbroker.pb.h"


namespace phxqueue_phxrpc {

namespace mqttbroker {


MqttCaller::MqttCaller(phxrpc::BaseTcpStream &socket, phxrpc::ClientMonitor &client_monitor)
        : socket_(socket), client_monitor_(client_monitor), cmd_id_(-1) {
}

MqttCaller::~MqttCaller() {
}

void MqttCaller::MonitorReport(phxrpc::ClientMonitor &client_monitor, bool send_error,
                               bool recv_error, size_t send_size,
                               size_t recv_size, uint64_t call_begin,
                               uint64_t call_end) {
    if (send_error) {
        client_monitor.SendError();
    }

    if (recv_error) {
        client_monitor.RecvError();
    }

    client_monitor.SendBytes(send_size);
    client_monitor.RecvBytes(recv_size);
    client_monitor.RequestCost(call_begin, call_end);
    if (0 < cmd_id_) {
        client_monitor.ClientCall(cmd_id_, "");
    }
}

int MqttCaller::PhxMqttConnectCall(const MqttConnectPb &req, MqttConnackPb *resp) {
    int ret{-1};
    phxqueue_phxrpc::mqttbroker::MqttConnect connect;
    phxqueue_phxrpc::mqttbroker::MqttConnack connack;

    // unpack request
    {
        ret = static_cast<int>(connect.FromPb(req));
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "FromPb err %d", ret);

            return ret;
        }
    }

    uint64_t call_begin{phxrpc::Timer::GetSteadyClockMS()};
    MqttClient::MqttStat mqtt_stat;
    ret = MqttClient::Connect(socket_, connect, connack, mqtt_stat);
    MonitorReport(client_monitor_, mqtt_stat.send_error_,
                  mqtt_stat.recv_error_, connect.GetContent().size(),
                  connack.GetContent().size(), call_begin,
                  phxrpc::Timer::GetSteadyClockMS());

    if (0 != ret) {
        phxrpc::log(LOG_ERR, "mqtt connect call err %d", ret);
        return ret;
    }

    // pack response
    {
        ret = static_cast<int>(connack.ToPb(resp));
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "ToPb ret %d", ret);

            return ret;
        }
    }

    return ret;
}

int MqttCaller::PhxMqttPublishCall(const MqttPublishPb &req, google::protobuf::Empty *resp) {
    int ret{-1};
    phxqueue_phxrpc::mqttbroker::MqttPublish publish;

    // unpack request
    {
        ret = static_cast<int>(publish.FromPb(req));
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "FromPb err %d", ret);

            return ret;
        }
    }

    uint64_t call_begin{phxrpc::Timer::GetSteadyClockMS()};
    MqttClient::MqttStat mqtt_stat;
    ret = MqttClient::Publish(socket_, publish, mqtt_stat);
    MonitorReport(client_monitor_, mqtt_stat.send_error_,
                  mqtt_stat.recv_error_, publish.GetContent().size(),
                  0, call_begin, phxrpc::Timer::GetSteadyClockMS());

    if (0 != ret) {
        phxrpc::log(LOG_ERR, "mqtt publish call err %d", ret);
        return ret;
    }

    // pack response
    //{
    //    ret = static_cast<int>(puback.ToPb(resp));
    //    if (0 != ret) {
    //        phxrpc::log(LOG_ERR, "ToPb ret %d", ret);

    //        return ret;
    //    }
    //}

    return ret;
}

int MqttCaller::PhxMqttPubackCall(const MqttPubackPb &req, google::protobuf::Empty *resp) {
    int ret{-1};
    phxqueue_phxrpc::mqttbroker::MqttPuback puback;

    // unpack request
    {
        ret = static_cast<int>(puback.FromPb(req));
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "FromPb err %d", ret);

            return ret;
        }
    }

    uint64_t call_begin{phxrpc::Timer::GetSteadyClockMS()};
    MqttClient::MqttStat mqtt_stat;
    ret = MqttClient::Puback(socket_, puback, mqtt_stat);
    MonitorReport(client_monitor_, mqtt_stat.send_error_,
                  mqtt_stat.recv_error_, puback.GetContent().size(),
                  0, call_begin, phxrpc::Timer::GetSteadyClockMS());

    if (0 != ret) {
        phxrpc::log(LOG_ERR, "mqtt puback call err %d", ret);
        return ret;
    }

    return ret;
}

int MqttCaller::PhxMqttPubrecCall(const MqttPubrecPb &req, google::protobuf::Empty *resp) {
    int ret{-1};
    phxqueue_phxrpc::mqttbroker::MqttPubrec pubrec;

    // unpack request
    {
        ret = static_cast<int>(pubrec.FromPb(req));
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "FromPb err %d", ret);

            return ret;
        }
    }

    uint64_t call_begin{phxrpc::Timer::GetSteadyClockMS()};
    MqttClient::MqttStat mqtt_stat;
    ret = MqttClient::Pubrec(socket_, pubrec, mqtt_stat);
    MonitorReport(client_monitor_, mqtt_stat.send_error_,
                  mqtt_stat.recv_error_, pubrec.GetContent().size(),
                  0, call_begin, phxrpc::Timer::GetSteadyClockMS());

    if (0 != ret) {
        phxrpc::log(LOG_ERR, "mqtt pubrec call err %d", ret);
        return ret;
    }

    return ret;
}

int MqttCaller::PhxMqttPubrelCall(const MqttPubrelPb &req, google::protobuf::Empty *resp) {
    int ret{-1};
    phxqueue_phxrpc::mqttbroker::MqttPubrel pubrel;

    // unpack request
    {
        ret = static_cast<int>(pubrel.FromPb(req));
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "FromPb err %d", ret);

            return ret;
        }
    }

    uint64_t call_begin{phxrpc::Timer::GetSteadyClockMS()};
    MqttClient::MqttStat mqtt_stat;
    ret = MqttClient::Pubrel(socket_, pubrel, mqtt_stat);
    MonitorReport(client_monitor_, mqtt_stat.send_error_,
                  mqtt_stat.recv_error_, pubrel.GetContent().size(),
                  0, call_begin, phxrpc::Timer::GetSteadyClockMS());

    if (0 != ret) {
        phxrpc::log(LOG_ERR, "mqtt pubrel call err %d", ret);
        return ret;
    }

    return ret;
}

int MqttCaller::PhxMqttPubcompCall(const MqttPubcompPb &req, google::protobuf::Empty *resp) {
    int ret{-1};
    phxqueue_phxrpc::mqttbroker::MqttPubcomp pubcomp;

    // unpack request
    {
        ret = static_cast<int>(pubcomp.FromPb(req));
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "FromPb err %d", ret);

            return ret;
        }
    }

    uint64_t call_begin{phxrpc::Timer::GetSteadyClockMS()};
    MqttClient::MqttStat mqtt_stat;
    ret = MqttClient::Pubcomp(socket_, pubcomp, mqtt_stat);
    MonitorReport(client_monitor_, mqtt_stat.send_error_,
                  mqtt_stat.recv_error_, pubcomp.GetContent().size(),
                  0, call_begin, phxrpc::Timer::GetSteadyClockMS());

    if (0 != ret) {
        phxrpc::log(LOG_ERR, "mqtt pubcomp call err %d", ret);
        return ret;
    }

    return ret;
}

int MqttCaller::PhxMqttSubscribeCall(const MqttSubscribePb &req, MqttSubackPb *resp) {
    int ret{-1};
    phxqueue_phxrpc::mqttbroker::MqttSubscribe subscribe;
    phxqueue_phxrpc::mqttbroker::MqttSuback suback;

    // unpack request
    {
        ret = static_cast<int>(subscribe.FromPb(req));
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "FromPb err %d", ret);

            return ret;
        }
    }

    uint64_t call_begin{phxrpc::Timer::GetSteadyClockMS()};
    MqttClient::MqttStat mqtt_stat;
    ret = MqttClient::Subscribe(socket_, subscribe, suback, mqtt_stat);
    MonitorReport(client_monitor_, mqtt_stat.send_error_,
                  mqtt_stat.recv_error_, subscribe.GetContent().size(),
                  suback.GetContent().size(), call_begin,
                  phxrpc::Timer::GetSteadyClockMS());

    if (0 != ret) {
        phxrpc::log(LOG_ERR, "mqtt publish call err %d", ret);
        return ret;
    }

    // pack response
    {
        ret = static_cast<int>(suback.ToPb(resp));
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "ToPb ret %d", ret);

            return ret;
        }
    }

    return ret;
}

int MqttCaller::PhxMqttUnsubscribeCall(const MqttUnsubscribePb &req, MqttUnsubackPb *resp) {
    int ret{-1};
    phxqueue_phxrpc::mqttbroker::MqttUnsubscribe unsubscribe;
    phxqueue_phxrpc::mqttbroker::MqttUnsuback unsuback;

    // unpack request
    {
        ret = static_cast<int>(unsubscribe.FromPb(req));
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "FromPb err %d", ret);

            return ret;
        }
    }

    uint64_t call_begin{phxrpc::Timer::GetSteadyClockMS()};
    MqttClient::MqttStat mqtt_stat;
    ret = MqttClient::Unsubscribe(socket_, unsubscribe, unsuback, mqtt_stat);
    MonitorReport(client_monitor_, mqtt_stat.send_error_,
                  mqtt_stat.recv_error_, unsubscribe.GetContent().size(),
                  unsuback.GetContent().size(), call_begin,
                  phxrpc::Timer::GetSteadyClockMS());

    if (0 != ret) {
        phxrpc::log(LOG_ERR, "mqtt publish call err %d", ret);
        return ret;
    }

    // pack response
    {
        ret = static_cast<int>(unsuback.ToPb(resp));
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "ToPb ret %d", ret);

            return ret;
        }
    }

    return ret;
}

int MqttCaller::PhxMqttPingCall(const MqttPingreqPb &req, MqttPingrespPb *resp) {
    int ret{-1};
    phxqueue_phxrpc::mqttbroker::MqttPingreq pingreq;
    phxqueue_phxrpc::mqttbroker::MqttPingresp pingresp;

    // unpack request
    {
        ret = static_cast<int>(pingreq.FromPb(req));
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "FromPb err %d", ret);

            return ret;
        }
    }

    uint64_t call_begin{phxrpc::Timer::GetSteadyClockMS()};
    MqttClient::MqttStat mqtt_stat;
    ret = MqttClient::Ping(socket_, pingreq, pingresp, mqtt_stat);
    MonitorReport(client_monitor_, mqtt_stat.send_error_,
                  mqtt_stat.recv_error_, pingreq.GetContent().size(),
                  pingresp.GetContent().size(), call_begin,
                  phxrpc::Timer::GetSteadyClockMS());

    if (0 != ret) {
        phxrpc::log(LOG_ERR, "mqtt publish call err %d", ret);
        return ret;
    }

    // pack response
    {
        ret = static_cast<int>(pingresp.ToPb(resp));
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "ToPb ret %d", ret);

            return ret;
        }
    }

    return ret;
}

int MqttCaller::PhxMqttDisconnectCall(const MqttDisconnectPb &req, google::protobuf::Empty *resp) {
    int ret{-1};
    phxqueue_phxrpc::mqttbroker::MqttDisconnect disconnect;

    // unpack request
    {
        ret = static_cast<int>(disconnect.FromPb(req));
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "FromPb err %d", ret);

            return ret;
        }
    }

    uint64_t call_begin{phxrpc::Timer::GetSteadyClockMS()};
    MqttClient::MqttStat mqtt_stat;
    ret = MqttClient::Disconnect(socket_, disconnect, mqtt_stat);
    MonitorReport(client_monitor_, mqtt_stat.send_error_,
                  mqtt_stat.recv_error_, disconnect.GetContent().size(),
                  0, call_begin, phxrpc::Timer::GetSteadyClockMS());

    if (0 != ret) {
        phxrpc::log(LOG_ERR, "mqtt disconnect call err %d", ret);
        return ret;
    }

    return ret;
}

void MqttCaller::SetURI(const char *const uri, const int cmdid) {
    cmd_id_ = cmdid;
}

void MqttCaller::SetKeepAlive(const bool keep_alive) {
}


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

