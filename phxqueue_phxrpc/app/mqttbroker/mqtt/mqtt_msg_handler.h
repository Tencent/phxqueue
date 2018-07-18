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

#include "phxrpc/msg.h"

#include "mqtt_protocol.h"


namespace phxrpc {


enum class ReturnCode;

class BaseTcpStream;


}  // namespace phxrpc


namespace phxqueue_phxrpc {

namespace mqttbroker {


class MqttMessage;

class MqttMessageHandler : public phxrpc::BaseMessageHandler {
  public:
    MqttMessageHandler() = default;
    virtual ~MqttMessageHandler() override = default;

    virtual int RecvRequest(phxrpc::BaseTcpStream &socket,
                            phxrpc::BaseRequest *&req) override;
    virtual int RecvResponse(phxrpc::BaseTcpStream &socket,
                             phxrpc::BaseResponse *&resp) override;

    virtual int GenRequest(phxrpc::BaseRequest *&req) override;
    virtual int GenResponse(phxrpc::BaseResponse *&resp) override;

    virtual bool keep_alive() const override;

    int GenConnect(const std::string &remaining_buffer, phxrpc::BaseRequest *&req);
    int GenConnack(const std::string &remaining_buffer, phxrpc::BaseResponse *&resp);
    int GenPublish(const std::string &remaining_buffer, phxrpc::BaseRequest *&req);
    int GenPublish(const std::string &remaining_buffer, phxrpc::BaseResponse *&resp);
    int GenPuback(const std::string &remaining_buffer, phxrpc::BaseRequest *&req);
    int GenPuback(const std::string &remaining_buffer, phxrpc::BaseResponse *&resp);
    int GenSubscribe(const std::string &remaining_buffer, phxrpc::BaseRequest *&req);
    int GenSuback(const std::string &remaining_buffer, phxrpc::BaseResponse *&resp);
    int GenUnsubscribe(const std::string &remaining_buffer, phxrpc::BaseRequest *&req);
    int GenUnsuback(const std::string &remaining_buffer, phxrpc::BaseResponse *&resp);
    int GenPingreq(const std::string &remaining_buffer, phxrpc::BaseRequest *&req);
    int GenPingresp(const std::string &remaining_buffer, phxrpc::BaseResponse *&resp);
    int GenDisconnect(const std::string &remaining_buffer, phxrpc::BaseRequest *&req);

  private:
    static int RecvRemainingLength(phxrpc::BaseTcpStream &in_stream,
                                   int &remaining_length);

    int RecvFixedHeaderAndRemainingBuffer(
            phxrpc::BaseTcpStream &in_stream, std::string &remaining_buffer);

    void Reset();

    void DecodeFixedHeader(const uint8_t fixed_header_byte);

    MqttProtocol::ControlPacketType control_packet_type_{MqttProtocol::ControlPacketType::FAKE_NONE};
    uint8_t flags_{0x00};
};


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

