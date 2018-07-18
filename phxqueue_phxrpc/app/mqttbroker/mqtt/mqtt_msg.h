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

#include <vector>
#include <string>

#include "phxrpc/msg.h"

#include "../mqttbroker.pb.h"
#include "mqtt_protocol.h"


namespace phxqueue_phxrpc {

namespace mqttbroker {


class MqttMessage : virtual public phxrpc::BaseMessage {
  public:
    static const char SampleFixedHeader[];

    // remaining length
    static int SendRemainingLength(phxrpc::BaseTcpStream &out_stream,
                                                  const int remaining_length);

    MqttMessage(google::protobuf::Message &base_pb);
    virtual ~MqttMessage() override;

    virtual int ToPb(google::protobuf::Message *const message) const override;
    virtual int FromPb(const google::protobuf::Message &message) override;

    virtual int Send(phxrpc::BaseTcpStream &socket) const override;

    virtual uint8_t EncodeFixedHeader() const;

    virtual int SendVariableHeader(std::ostringstream &out_stream) const = 0;
    virtual int RecvVariableHeader(std::istringstream &in_stream) = 0;

    virtual int SendPayload(std::ostringstream &out_stream) const = 0;
    virtual int RecvPayload(std::istringstream &in_stream) = 0;

    virtual size_t size() const override;

    // control packet type and flags
    int SendFixedHeaderAndRemainingBuffer(
            phxrpc::BaseTcpStream &out_stream, const std::string &remaining_buffer) const;

    int SendRemaining(std::ostringstream &out_stream) const;
    int RecvRemaining(std::istringstream &in_stream);

    MqttProtocol::ControlPacketType control_packet_type() const { return control_packet_type_; }

    void set_control_packet_type(const MqttProtocol::ControlPacketType control_packet_type) {
        control_packet_type_ = control_packet_type;
    }

    int remaining_length() const { return remaining_length_; }

    void set_remaining_length(const int remaining_length) {
        remaining_length_ = remaining_length;
    }

  private:
    MqttProtocol::ControlPacketType control_packet_type_{MqttProtocol::ControlPacketType::FAKE_NONE};
    int remaining_length_{0};
    google::protobuf::Message &base_pb_;
};


class MqttRequest : public virtual MqttMessage, public phxrpc::BaseRequest {
  public:
    MqttRequest(google::protobuf::Message &base_pb);
    virtual ~MqttRequest() = default;

    virtual bool keep_alive() const override { return true; };
    virtual void set_keep_alive(const bool) override {};
};


class MqttResponse : public virtual MqttMessage, public phxrpc::BaseResponse {
  public:
    MqttResponse(google::protobuf::Message &base_pb);
    virtual ~MqttResponse() = default;


    virtual void SetFake(FakeReason reason) override;

    virtual int Modify(const bool keep_alive, const std::string &version) override;

    virtual int result() override { return 0; }
    virtual void set_result(const int) override {}
};


class MqttFakeResponse final : public MqttResponse {
  public:
    MqttFakeResponse();
    virtual ~MqttFakeResponse() = default;

    virtual int SendVariableHeader(std::ostringstream &out_stream) const override {
        return 0;
    }
    virtual int RecvVariableHeader(std::istringstream &in_stream) override {
        return 0;
    }

    virtual int SendPayload(std::ostringstream &out_stream) const override {
        return 0;
    }
    virtual int RecvPayload(std::istringstream &in_stream) override {
        return 0;
    }

  private:
    google::protobuf::Empty pb_;
};


class MqttConnect final : public MqttRequest {
  public:
    MqttConnect();
    virtual ~MqttConnect() = default;

    virtual phxrpc::BaseResponse *GenResponse() const override;

    virtual int SendVariableHeader(std::ostringstream &out_stream) const override;
    virtual int RecvVariableHeader(std::istringstream &in_stream) override;

    virtual int SendPayload(std::ostringstream &out_stream) const override;
    virtual int RecvPayload(std::istringstream &in_stream) override;

    const MqttConnectPb &pb() const;

  private:
    MqttConnectPb pb_;
    bool user_name_flag_{false};
    bool password_flag_{false};
};


class MqttConnack final : public MqttResponse {
  public:
    MqttConnack();
    virtual ~MqttConnack() = default;

    virtual int SendVariableHeader(std::ostringstream &out_stream) const override;
    virtual int RecvVariableHeader(std::istringstream &in_stream) override;

    virtual int SendPayload(std::ostringstream &out_stream) const override {
        return 0;
    }
    virtual int RecvPayload(std::istringstream &in_stream) override {
        return 0;
    }

    const MqttConnackPb &pb() const;

  private:
    MqttConnackPb pb_;
};


class MqttPublish final : public MqttRequest, public MqttResponse {
  public:
    MqttPublish();
    virtual ~MqttPublish() = default;

    virtual phxrpc::BaseResponse *GenResponse() const override;

    virtual uint8_t EncodeFixedHeader() const override;

    virtual int SendVariableHeader(std::ostringstream &out_stream) const override;
    virtual int RecvVariableHeader(std::istringstream &in_stream) override;

    virtual int SendPayload(std::ostringstream &out_stream) const override;
    virtual int RecvPayload(std::istringstream &in_stream) override;

    virtual size_t size() const override;

    void SetFlags(const uint8_t flags);

    const MqttPublishPb &pb() const;

  private:
    MqttPublishPb pb_;
};


class MqttPuback final : public MqttRequest, public MqttResponse {
  public:
    MqttPuback();
    virtual ~MqttPuback() = default;

    virtual phxrpc::BaseResponse *GenResponse() const override;

    virtual int SendVariableHeader(std::ostringstream &out_stream) const override;
    virtual int RecvVariableHeader(std::istringstream &in_stream) override;
    virtual int SendPayload(std::ostringstream &out_stream) const override {
        return 0;
    }
    virtual int RecvPayload(std::istringstream &in_stream) override {
        return 0;
    }

    const MqttPubackPb &pb() const;

  private:
    MqttPubackPb pb_;
};


class MqttPubrec final : public MqttRequest, public MqttResponse {
  public:
    MqttPubrec();
    virtual ~MqttPubrec() = default;

    virtual phxrpc::BaseResponse *GenResponse() const override;

    virtual int SendVariableHeader(std::ostringstream &out_stream) const override;
    virtual int RecvVariableHeader(std::istringstream &in_stream) override;

    virtual int SendPayload(std::ostringstream &out_stream) const override;
    virtual int RecvPayload(std::istringstream &in_stream) override;

  private:
    MqttPubrecPb pb_;
};


class MqttPubrel final : public MqttRequest, public MqttResponse {
  public:
    MqttPubrel();
    virtual ~MqttPubrel() = default;

    virtual phxrpc::BaseResponse *GenResponse() const override;

    virtual int SendVariableHeader(std::ostringstream &out_stream) const override;
    virtual int RecvVariableHeader(std::istringstream &in_stream) override;

    virtual int SendPayload(std::ostringstream &out_stream) const override;
    virtual int RecvPayload(std::istringstream &in_stream) override;

  private:
    MqttPubrelPb pb_;
};


class MqttPubcomp final : public MqttRequest, public MqttResponse {
  public:
    MqttPubcomp();
    virtual ~MqttPubcomp() = default;

    virtual phxrpc::BaseResponse *GenResponse() const override;

    virtual int SendVariableHeader(std::ostringstream &out_stream) const override;
    virtual int RecvVariableHeader(std::istringstream &in_stream) override;

    virtual int SendPayload(std::ostringstream &out_stream) const override;
    virtual int RecvPayload(std::istringstream &in_stream) override;

  private:
    MqttPubcompPb pb_;
};


class MqttSubscribe final : public MqttRequest {
  public:
    MqttSubscribe();
    virtual ~MqttSubscribe() = default;

    virtual phxrpc::BaseResponse *GenResponse() const override;

    virtual int SendVariableHeader(std::ostringstream &out_stream) const override;
    virtual int RecvVariableHeader(std::istringstream &in_stream) override;
    virtual int SendPayload(std::ostringstream &out_stream) const override;
    virtual int RecvPayload(std::istringstream &in_stream) override;

    const MqttSubscribePb &pb() const;

  private:
    MqttSubscribePb pb_;
};


class MqttSuback final : public MqttResponse {
  public:
    MqttSuback();
    virtual ~MqttSuback() = default;

    virtual int SendVariableHeader(std::ostringstream &out_stream) const override;
    virtual int RecvVariableHeader(std::istringstream &in_stream) override;
    virtual int SendPayload(std::ostringstream &out_stream) const override;
    virtual int RecvPayload(std::istringstream &in_stream) override;

    const MqttSubackPb &pb() const;

  private:
    MqttSubackPb pb_;
};


class MqttUnsubscribe final : public MqttRequest {
  public:
    MqttUnsubscribe();
    virtual ~MqttUnsubscribe() = default;

    virtual phxrpc::BaseResponse *GenResponse() const override;

    virtual int SendVariableHeader(std::ostringstream &out_stream) const override;
    virtual int RecvVariableHeader(std::istringstream &in_stream) override;
    virtual int SendPayload(std::ostringstream &out_stream) const override;
    virtual int RecvPayload(std::istringstream &in_stream) override;

    const MqttUnsubscribePb &pb() const;

  private:
    MqttUnsubscribePb pb_;
};


class MqttUnsuback final : public MqttResponse {
  public:
    MqttUnsuback();
    virtual ~MqttUnsuback() = default;

    virtual int SendVariableHeader(std::ostringstream &out_stream) const override;
    virtual int RecvVariableHeader(std::istringstream &in_stream) override;

    virtual int SendPayload(std::ostringstream &out_stream) const override {
        return 0;
    }
    virtual int RecvPayload(std::istringstream &in_stream) override {
        return 0;
    }

    const MqttUnsubackPb &pb() const;

  private:
    MqttUnsubackPb pb_;
};


class MqttPingreq final : public MqttRequest {
  public:
    MqttPingreq();
    virtual ~MqttPingreq() = default;

    virtual phxrpc::BaseResponse *GenResponse() const override;

    virtual int SendVariableHeader(std::ostringstream &out_stream) const override {
        return 0;
    }
    virtual int RecvVariableHeader(std::istringstream &in_stream) override {
        return 0;
    }

    virtual int SendPayload(std::ostringstream &out_stream) const override {
        return 0;
    }
    virtual int RecvPayload(std::istringstream &in_stream) override {
        return 0;
    }

    const MqttPingreqPb &pb() const;

  private:
    MqttPingreqPb pb_;
};


class MqttPingresp final : public MqttResponse {
  public:
    MqttPingresp();
    virtual ~MqttPingresp() = default;

    virtual int SendVariableHeader(std::ostringstream &out_stream) const override {
        return 0;
    }
    virtual int RecvVariableHeader(std::istringstream &in_stream) override {
        return 0;
    }

    virtual int SendPayload(std::ostringstream &out_stream) const override {
        return 0;
    }
    virtual int RecvPayload(std::istringstream &in_stream) override {
        return 0;
    }

    const MqttPingrespPb &pb() const;

  private:
    MqttPingrespPb pb_;
};


class MqttDisconnect final : public MqttRequest {
  public:
    MqttDisconnect();
    virtual ~MqttDisconnect() = default;

    virtual phxrpc::BaseResponse *GenResponse() const override;

    virtual int SendVariableHeader(std::ostringstream &out_stream) const override {
        return 0;
    }
    virtual int RecvVariableHeader(std::istringstream &in_stream) override {
        return 0;
    }

    virtual int SendPayload(std::ostringstream &out_stream) const override {
        return 0;
    }
    virtual int RecvPayload(std::istringstream &in_stream) override {
        return 0;
    }

    const MqttDisconnectPb &pb() const;

  private:
    MqttDisconnectPb pb_;
};


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

