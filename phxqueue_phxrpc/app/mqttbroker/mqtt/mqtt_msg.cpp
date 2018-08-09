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

#include "mqtt_msg.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sstream>

#include "phxrpc/file/log_utils.h"
#include "phxrpc/network/socket_stream_base.h"
#include "phxrpc/rpc/phxrpc.pb.h"


namespace phxqueue_phxrpc {

namespace mqttbroker {


using namespace logic::mqtt;
using namespace std;


const char MqttMessage::SampleFixedHeader[]{
    '\x00',  // FAKE_NONE
    '\x10',  // CONNECT
    '\x20',  // CONNACK
    '\x32',  // PUBLISH QoS 1
    '\x40',  // PUBACK
    '\x50',  // PUBREC
    '\x62',  // PUBREL
    '\x70',  // PUBCOMP
    '\x82',  // SUBSCRIBE
    '\x90',  // SUBACK
    '\xa2',  // UNSUBSCRIBE
    '\xb0',  // UNSUBACK
    '\xc0',  // PINGREQ
    '\xd0',  // PINGRESP
    '\xe0',  // DISCONNECT
    '\xf0',  // FAKE_DISCONNACK
};


MqttMessage::MqttMessage(google::protobuf::Message &base_pb) : base_pb_(base_pb) {
}

MqttMessage::~MqttMessage() {}

int MqttMessage::SendRemainingLength(phxrpc::BaseTcpStream &out_stream,
                                                    const int remaining_length) {
    char temp{0x0};
    char continue_bit{0x0};
    uint32_t temp_remaining_length{static_cast<uint32_t>(remaining_length)};

    for (int i{0}; 4 > i; ++i) {
        temp = (temp_remaining_length & 0x7f);
        temp_remaining_length >>= 8;
        continue_bit = (temp_remaining_length > 0) ? 0x80 : 0x0;
        out_stream.put(temp | continue_bit);
        if (0x0 == continue_bit) {
            return 0;
        }
    }

    return 0;
}

int MqttMessage::ToPb(google::protobuf::Message *const message) const {
    try {
        message->CopyFrom(base_pb_);
    } catch (exception) {
        return -1;
    }

    return 0;
}

int MqttMessage::FromPb(const google::protobuf::Message &message) {
    try {
        base_pb_.CopyFrom(message);
    } catch (exception) {
        return -1;
    }

    return 0;
}

int MqttMessage::Send(phxrpc::BaseTcpStream &socket) const {
    ostringstream ss;
    int ret{SendRemaining(ss)};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendRemaining err %d", static_cast<int>(ret));

        return ret;
    }

    ret = SendFixedHeaderAndRemainingBuffer(socket, ss.str());
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendFixedHeaderAndRemainingBuffer err %d", static_cast<int>(ret));

        return ret;
    }

    if (!socket.flush().good()) {
        phxrpc::log(LOG_ERR, "socket err %d", socket.LastError());

        return static_cast<int>(socket.LastError());
    }

    return ret;
}

uint8_t MqttMessage::EncodeFixedHeader() const {
    const int control_packet_type_int{static_cast<int>(control_packet_type_)};
    const char fixed_header_char{SampleFixedHeader[control_packet_type_int]};
    uint8_t fixed_header_byte{static_cast<uint8_t>(fixed_header_char)};

    return fixed_header_byte;
}

size_t MqttMessage::size() const {
    // TODO: add header size
    return 0;
}

int MqttMessage::SendFixedHeaderAndRemainingBuffer(
        phxrpc::BaseTcpStream &out_stream, const string &remaining_buffer) const {
    uint8_t fixed_header_byte{EncodeFixedHeader()};
    int ret{MqttProtocol::SendChar(out_stream, static_cast<char>(fixed_header_byte))};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendChar err %d", static_cast<int>(ret));

        return ret;
    } else {
        phxrpc::log(LOG_DEBUG, "SendChar type %d fixed_header_byte %u",
                    static_cast<int>(control_packet_type_),
                    static_cast<uint8_t>(fixed_header_byte));
    }

    const int remaining_length{static_cast<const int>(remaining_buffer.size())};
    ret = SendRemainingLength(out_stream, remaining_length);
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendRemainingLength err %d", static_cast<int>(ret));

        return ret;
    }

    ret = MqttProtocol::SendChars(out_stream, remaining_buffer.data(),
                                  remaining_buffer.size());
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendChars err %d", static_cast<int>(ret));

        return ret;
    }

    return 0;
}

int MqttMessage::SendRemaining(ostringstream &out_stream) const {
    int ret{SendVariableHeader(out_stream)};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendVariableHeader err %d", static_cast<int>(ret));

        return ret;
    }
        // TODO: remove
        //string remaining_buffer(out_stream.str());
        //printf("remaining_buffer %zu\n", remaining_buffer.size());
        //for (int i{0}; remaining_buffer.size() > i; ++i) {
        //printf("%d\t", remaining_buffer.at(i));
        //}
        //printf("\n");
        //for (int i{0}; remaining_buffer.size() > i; ++i) {
        //printf("%c\t", remaining_buffer.at(i));
        //}
        //printf("\n");

    ret = SendPayload(out_stream);
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendPayload err %d", static_cast<int>(ret));

        return ret;
    }
        // TODO: remove
        //remaining_buffer = out_stream.str();
        //printf("remaining_buffer %zu\n", remaining_buffer.size());
        //for (int i{0}; remaining_buffer.size() > i; ++i) {
        //printf("%d\t", remaining_buffer.at(i));
        //}
        //printf("\n");
        //for (int i{0}; remaining_buffer.size() > i; ++i) {
        //printf("%c\t", remaining_buffer.at(i));
        //}
        //printf("\n");

    return ret;
}

int MqttMessage::RecvRemaining(istringstream &in_stream) {
    int ret{RecvVariableHeader(in_stream)};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "RecvVariableHeader err %d", static_cast<int>(ret));

        return ret;
    }

    ret = RecvPayload(in_stream);
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "RecvPayload err %d", static_cast<int>(ret));

        return ret;
    }
    //phxrpc::log(LOG_DEBUG, "RecvPayload \"%s\"", data_.c_str());

    return ret;
}

void MqttResponse::SetFake(FakeReason reason) {}

int MqttResponse::Modify(const bool keep_alive, const string &version) {
    return 0;
}


MqttRequest::MqttRequest(google::protobuf::Message &base_pb) : MqttMessage(base_pb) {
}


MqttResponse::MqttResponse(google::protobuf::Message &base_pb) : MqttMessage(base_pb) {
}


MqttFakeResponse::MqttFakeResponse() : MqttMessage(pb_), MqttResponse(pb_) {
    set_control_packet_type(MqttProtocol::ControlPacketType::FAKE_NONE);
    set_fake(true);
}


MqttConnect::MqttConnect() : MqttMessage(pb_), MqttRequest(pb_) {
    set_uri("/phxqueue_phxrpc/mqttbroker/MqttConnect");
    set_control_packet_type(MqttProtocol::ControlPacketType::CONNECT);
    pb_.set_proto_name("MQTT");
    pb_.set_proto_level(4);
}

phxrpc::BaseResponse *MqttConnect::GenResponse() const { return new MqttConnack; }

int MqttConnect::SendVariableHeader(ostringstream &out_stream) const {
    int ret{MqttProtocol::SendUnicode(out_stream, pb_.proto_name())};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

        return ret;
    }

    ret = MqttProtocol::SendChar(out_stream, pb_.proto_level());
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendChar err %d", static_cast<int>(ret));

        return ret;
    }

    uint8_t connect_flags{0x0};
    connect_flags |= ((pb_.clean_session() ? 0x1 : 0x0) << 1);
    connect_flags |= ((pb_.will_flag() ? 0x1 : 0x0) << 2);
    connect_flags |= (pb_.will_qos() << 3);
    connect_flags |= ((pb_.will_retain() ? 0x1 : 0x0) << 5);
    connect_flags |= ((user_name_flag_ ? 0x0 : 0x1) << 6);
    connect_flags |= ((password_flag_ ? 0x0 : 0x1) << 7);
    ret = MqttProtocol::SendChar(out_stream, static_cast<char>(connect_flags));
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendChar err %d", static_cast<int>(ret));

        return ret;
    }

    ret = MqttProtocol::SendUint16(out_stream, pb_.keep_alive());
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendUint16 err %d", static_cast<int>(ret));

        return ret;
    }

    return ret;
}

int MqttConnect::RecvVariableHeader(istringstream &in_stream) {
    int ret{MqttProtocol::RecvUnicode(in_stream, *pb_.mutable_proto_name())};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

        return ret;
    }

    // MQTT 3.1.1: "MQTT"
    // MQTT 3.1: "MQIsdp"
    if (pb_.proto_name() != "MQTT") {
        phxrpc::log(LOG_ERR, "violate mqtt protocol");

        return -401;
    }

    char proto_level{'\0'};
    ret = MqttProtocol::RecvChar(in_stream, proto_level);
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "RecvChar err %d", static_cast<int>(ret));

        return ret;
    }
    pb_.set_proto_level(proto_level);

    char connect_flags{0x0};
    ret = MqttProtocol::RecvChar(in_stream, connect_flags);
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "RecvChar err %d", static_cast<int>(ret));

        return ret;
    }
    pb_.set_clean_session(0x0 != (connect_flags & 0x2));
    pb_.set_will_flag(0x0 != (connect_flags & 0x4));
    uint32_t will_qos{0};
    will_qos = ((connect_flags & 0x8) >> 3);
    will_qos |= ((connect_flags & 0x10) >> 3);
    pb_.set_will_qos(will_qos);
    pb_.set_will_retain(0x0 != (connect_flags & 0x20));
    user_name_flag_ = (0x0 != (connect_flags & 0x40));
    password_flag_ = (0x0 != (connect_flags & 0x80));

    uint16_t keep_alive{0};
    ret = MqttProtocol::RecvUint16(in_stream, keep_alive);
    pb_.set_keep_alive(keep_alive);
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "RecvUint16 err %d", static_cast<int>(ret));

        return ret;
    }

    return ret;
}

int MqttConnect::SendPayload(ostringstream &out_stream) const {
    int ret{MqttProtocol::SendUnicode(out_stream, pb_.client_identifier())};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

        return ret;
    }

    if (pb_.will_flag()) {
        ret = MqttProtocol::SendUnicode(out_stream, pb_.will_topic());
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

            return ret;
        }

        ret = MqttProtocol::SendUnicode(out_stream, pb_.will_message());
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

            return ret;
        }
    }

    if (user_name_flag_) {
        ret = MqttProtocol::SendUnicode(out_stream, pb_.user_name());
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

            return ret;
        }
    }

    if (password_flag_) {
        ret = MqttProtocol::SendUnicode(out_stream, pb_.password());
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

            return ret;
        }
    }

    return ret;
}

int MqttConnect::RecvPayload(istringstream &in_stream) {
    int ret{MqttProtocol::RecvUnicode(in_stream, *pb_.mutable_client_identifier())};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

        return ret;
    }

    if (pb_.will_flag()) {
        ret = MqttProtocol::RecvUnicode(in_stream, *pb_.mutable_will_topic());
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

            return ret;
        }

        ret = MqttProtocol::RecvUnicode(in_stream, *pb_.mutable_will_message());
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

            return ret;
        }
    }

    if (user_name_flag_) {
        ret = MqttProtocol::RecvUnicode(in_stream, *pb_.mutable_user_name());
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

            return ret;
        }
    }

    if (password_flag_) {
        ret = MqttProtocol::RecvUnicode(in_stream, *pb_.mutable_password());
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

            return ret;
        }
    }

    return ret;
}

const MqttConnectPb &MqttConnect::pb() const { return pb_; }


MqttConnack::MqttConnack() : MqttMessage(pb_), MqttResponse(pb_) {
    set_control_packet_type(MqttProtocol::ControlPacketType::CONNACK);
}

int MqttConnack::SendVariableHeader(ostringstream &out_stream) const {
    int ret{MqttProtocol::SendChar(out_stream, pb_.session_present() ? 0x1 : 0x0)};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendChar err %d", static_cast<int>(ret));

        return ret;
    }

    ret = MqttProtocol::SendChar(out_stream, pb_.connect_return_code());
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendChar err %d", static_cast<int>(ret));

        return ret;
    }

    return 0;
}

int MqttConnack::RecvVariableHeader(istringstream &in_stream) {
    char connect_acknowledge_flags{0x0};
    int ret{MqttProtocol::RecvChar(in_stream, connect_acknowledge_flags)};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "RecvChar err %d", static_cast<int>(ret));

        return ret;
    }
    pb_.set_session_present(0x1 == (connect_acknowledge_flags & 0x1));

    char connect_return_code{0x0};
    ret = MqttProtocol::RecvChar(in_stream, connect_return_code);
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "RecvChar err %d", static_cast<int>(ret));

        return ret;
    }
    pb_.set_connect_return_code(connect_return_code);

    return 0;
}

const MqttConnackPb &MqttConnack::pb() const { return pb_; }


MqttPublish::MqttPublish() : MqttMessage(pb_), MqttRequest(pb_), MqttResponse(pb_) {
    set_uri("/phxqueue_phxrpc/mqttbroker/MqttPublish");
    set_control_packet_type(MqttProtocol::ControlPacketType::PUBLISH);
}

phxrpc::BaseResponse *MqttPublish::GenResponse() const { return new MqttFakeResponse; }

uint8_t MqttPublish::EncodeFixedHeader() const {
    const int control_packet_type_int{static_cast<int>(control_packet_type())};
    const char fixed_header_char{SampleFixedHeader[control_packet_type_int]};
    uint8_t fixed_header_byte{static_cast<uint8_t>(fixed_header_char)};

    pb_.dup() ? (fixed_header_byte |= 0x8) : (fixed_header_byte &= ~0x8);
    fixed_header_byte &= ~0x6;
    fixed_header_byte |= (static_cast<uint8_t>(pb_.qos()) << 1);
    pb_.retain() ? (fixed_header_byte |= 0x1) : (fixed_header_byte &= ~0x1);

    return fixed_header_byte;
}

int MqttPublish::SendVariableHeader(ostringstream &out_stream) const {
    int ret{MqttProtocol::SendUnicode(out_stream, pb_.topic_name())};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

        return ret;
    }

    if (0 < pb_.qos()) {
        ret = MqttProtocol::SendUint16(out_stream, pb_.packet_identifier());
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "SendUint16 err %d", static_cast<int>(ret));

            return ret;
        }
    }

    return ret;
}

int MqttPublish::RecvVariableHeader(istringstream &in_stream) {
    int ret{MqttProtocol::RecvUnicode(in_stream, *pb_.mutable_topic_name())};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

        return ret;
    }

    if (0 < pb_.qos()) {
        uint16_t packet_identifier{0};
        ret = MqttProtocol::RecvUint16(in_stream, packet_identifier);
        pb_.set_packet_identifier(packet_identifier);
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "RecvUint16 err %d", static_cast<int>(ret));

            return ret;
        }
    }

    return ret;
}

int MqttPublish::SendPayload(ostringstream &out_stream) const {
    return MqttProtocol::SendChars(out_stream, pb_.data().data(), pb_.data().size());
}

int MqttPublish::RecvPayload(istringstream &in_stream) {
    int variable_header_length{static_cast<int>(pb_.topic_name().length()) + 2};
    if (0 < pb_.qos()) {
        variable_header_length += 2;
    }
    const int payload_length{remaining_length() - variable_header_length};
    if (0 == payload_length)
      return 0;
    if (0 > payload_length)
      return -104;

    string payload_buffer;
    payload_buffer.resize(payload_length);
    int ret{MqttProtocol::RecvChars(in_stream, &payload_buffer[0], payload_length)};
    if (0 == ret) {
        pb_.set_data(payload_buffer);
    }

    return ret;
}

size_t MqttPublish::size() const {
    // TODO: add header size
    return pb_.data().size();
}

void MqttPublish::SetFlags(const uint8_t flags) {
    pb_.set_dup(static_cast<bool>((flags >> 3) & 0x01));
    pb_.set_qos(static_cast<int>((flags >> 1) & 0x03));
    pb_.set_retain(static_cast<bool>(flags & 0x01));
}

const MqttPublishPb &MqttPublish::pb() const { return pb_; }


MqttPuback::MqttPuback() : MqttMessage(pb_), MqttRequest(pb_), MqttResponse(pb_) {
    set_uri("/phxqueue_phxrpc/mqttbroker/MqttPuback");
    set_control_packet_type(MqttProtocol::ControlPacketType::PUBACK);
}

phxrpc::BaseResponse *MqttPuback::GenResponse() const { return new MqttFakeResponse; }

int MqttPuback::SendVariableHeader(ostringstream &out_stream) const {
    return MqttProtocol::SendUint16(out_stream, pb_.packet_identifier());
}

int MqttPuback::RecvVariableHeader(istringstream &in_stream) {
    uint16_t packet_identifier{0};
    int ret{MqttProtocol::RecvUint16(in_stream, packet_identifier)};
    if (0 == ret)
        pb_.set_packet_identifier(packet_identifier);

    return ret;
}

const MqttPubackPb &MqttPuback::pb() const { return pb_; }


MqttPubrec::MqttPubrec() : MqttMessage(pb_), MqttRequest(pb_), MqttResponse(pb_) {
    set_uri("/phxqueue_phxrpc/mqttbroker/MqttPubrec");
    set_control_packet_type(MqttProtocol::ControlPacketType::PUBREC);
}

phxrpc::BaseResponse *MqttPubrec::GenResponse() const { return new MqttFakeResponse; }

int MqttPubrec::SendVariableHeader(ostringstream &out_stream) const {
    return -101;
}

int MqttPubrec::RecvVariableHeader(istringstream &in_stream) {
    return -101;
}

int MqttPubrec::SendPayload(ostringstream &out_stream) const {
    return -101;
}

int MqttPubrec::RecvPayload(istringstream &in_stream) {
    return -101;
}


MqttPubrel::MqttPubrel() : MqttMessage(pb_), MqttRequest(pb_), MqttResponse(pb_) {
    set_uri("/phxqueue_phxrpc/mqttbroker/MqttPubrel");
    set_control_packet_type(MqttProtocol::ControlPacketType::PUBREL);
}

phxrpc::BaseResponse *MqttPubrel::GenResponse() const { return new MqttFakeResponse; }

int MqttPubrel::SendVariableHeader(ostringstream &out_stream) const {
    return -101;
}

int MqttPubrel::RecvVariableHeader(istringstream &in_stream) {
    return -101;
}

int MqttPubrel::SendPayload(ostringstream &out_stream) const {
    return -101;
}

int MqttPubrel::RecvPayload(istringstream &in_stream) {
    return -101;
}


MqttPubcomp::MqttPubcomp() : MqttMessage(pb_), MqttRequest(pb_), MqttResponse(pb_) {
    set_uri("/phxqueue_phxrpc/mqttbroker/MqttPubcomp");
    set_control_packet_type(MqttProtocol::ControlPacketType::PUBCOMP);
}

phxrpc::BaseResponse *MqttPubcomp::GenResponse() const { return new MqttFakeResponse; }

int MqttPubcomp::SendVariableHeader(ostringstream &out_stream) const {
    return -101;
}

int MqttPubcomp::RecvVariableHeader(istringstream &in_stream) {
    return -101;
}

int MqttPubcomp::SendPayload(ostringstream &out_stream) const {
    return -101;
}

int MqttPubcomp::RecvPayload(istringstream &in_stream) {
    return -101;
}


MqttSubscribe::MqttSubscribe() : MqttMessage(pb_), MqttRequest(pb_) {
    set_uri("/phxqueue_phxrpc/mqttbroker/MqttSubscribe");
    set_control_packet_type(MqttProtocol::ControlPacketType::SUBSCRIBE);
}

phxrpc::BaseResponse *MqttSubscribe::GenResponse() const { return new MqttSuback; }

int MqttSubscribe::SendVariableHeader(ostringstream &out_stream) const {
    return MqttProtocol::SendUint16(out_stream, pb_.packet_identifier());
}

int MqttSubscribe::RecvVariableHeader(istringstream &in_stream) {
    uint16_t packet_identifier{0};
    int ret{MqttProtocol::RecvUint16(in_stream, packet_identifier)};
    if (0 == ret)
        pb_.set_packet_identifier(packet_identifier);

    return ret;
}

int MqttSubscribe::SendPayload(ostringstream &out_stream) const {
    for (int i{0}; pb_.topic_filters_size() > i; ++i) {
        int ret{MqttProtocol::SendUnicode(out_stream, pb_.topic_filters(i))};
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

            return ret;
        }

        if (pb_.qoss_size() > i) {
            ret = MqttProtocol::SendChar(out_stream, pb_.qoss(i));
        } else {
            ret = MqttProtocol::SendChar(out_stream, 0x0);
        }
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "SendChar err %d", static_cast<int>(ret));

            return ret;
        }
    }

    return 0;
}

int MqttSubscribe::RecvPayload(istringstream &in_stream) {
    const int variable_header_length{2};
    const int payload_length{remaining_length() - variable_header_length};
    if (0 == payload_length)
      return 0;
    if (0 > payload_length)
      return -104;

    int used_length{0};
    vector<string> topic_filters;
    vector<uint32_t> qoss;
    while (used_length < payload_length && EOF != in_stream.peek()) {
        string topic_filter;
        int ret{MqttProtocol::RecvUnicode(in_stream, topic_filter)};
        if (-105 == ret) {
            return 0;
        }

        if (0 != ret) {
            phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

            return ret;
        }

        char requested_qos{0x0};
        ret = MqttProtocol::RecvChar(in_stream, requested_qos);
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "RecvChar err %d", static_cast<int>(ret));

            return ret;
        }

        topic_filters.emplace_back(topic_filter);
        qoss.emplace_back(requested_qos);

        used_length += topic_filter.length() + 3;
    }
    google::protobuf::RepeatedPtrField<string> tmp_topic_filters(
            topic_filters.begin(), topic_filters.end());
    pb_.mutable_topic_filters()->Swap(&tmp_topic_filters);
    google::protobuf::RepeatedField<uint32_t> tmp_qoss(qoss.begin(), qoss.end());
    pb_.mutable_qoss()->Swap(&tmp_qoss);

    return 0;
}

const MqttSubscribePb &MqttSubscribe::pb() const { return pb_; }


MqttSuback::MqttSuback() : MqttMessage(pb_), MqttResponse(pb_) {
    set_control_packet_type(MqttProtocol::ControlPacketType::SUBACK);
}

int MqttSuback::SendVariableHeader(ostringstream &out_stream) const {
    return MqttProtocol::SendUint16(out_stream, pb_.packet_identifier());
}

int MqttSuback::RecvVariableHeader(istringstream &in_stream) {
    uint16_t packet_identifier{0};
    int ret{MqttProtocol::RecvUint16(in_stream, packet_identifier)};
    if (0 == ret)
        pb_.set_packet_identifier(packet_identifier);

    return ret;
}

int MqttSuback::SendPayload(ostringstream &out_stream) const {
    for (int i{0}; pb_.return_codes_size() > i; ++i) {
        int ret{MqttProtocol::SendChar(out_stream, pb_.return_codes(i))};
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "SendChar err %d", static_cast<int>(ret));

            return ret;
        }
    }

    return 0;
}

int MqttSuback::RecvPayload(istringstream &in_stream) {
    vector<uint32_t> return_codes;
    while (EOF != in_stream.peek()) {
        char return_code{0x0};
        int ret{MqttProtocol::RecvChar(in_stream, return_code)};
        if (-105 == ret) {
            return 0;
        }

        if (0 != ret) {
            phxrpc::log(LOG_ERR, "RecvChar err %d", static_cast<int>(ret));

            return ret;
        }

        return_codes.emplace_back(static_cast<uint32_t>(return_code));
    }
    google::protobuf::RepeatedField<uint32_t> tmp_return_codes(
            return_codes.begin(), return_codes.end());
    pb_.mutable_return_codes()->Swap(&tmp_return_codes);

    return 0;
}

const MqttSubackPb &MqttSuback::pb() const { return pb_; }


MqttUnsubscribe::MqttUnsubscribe() : MqttMessage(pb_), MqttRequest(pb_) {
    set_uri("/phxqueue_phxrpc/mqttbroker/MqttUnsubscribe");
    set_control_packet_type(MqttProtocol::ControlPacketType::UNSUBSCRIBE);
}

phxrpc::BaseResponse *MqttUnsubscribe::GenResponse() const { return new MqttUnsuback; }

int
MqttUnsubscribe::SendVariableHeader(ostringstream &out_stream) const {
    return MqttProtocol::SendUint16(out_stream, pb_.packet_identifier());
}

int
MqttUnsubscribe::RecvVariableHeader(istringstream &in_stream) {
    uint16_t packet_identifier{0};
    int ret{MqttProtocol::RecvUint16(in_stream, packet_identifier)};
    if (0 == ret)
        pb_.set_packet_identifier(packet_identifier);

    return ret;
}

int
MqttUnsubscribe::SendPayload(ostringstream &out_stream) const {
    for (int i{0}; pb_.topic_filters_size() > i; ++i) {
        int ret{MqttProtocol::SendUnicode(out_stream, pb_.topic_filters(i))};
        if (0 != ret) {
            phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

            return ret;
        }
    }

    return 0;
}

int
MqttUnsubscribe::RecvPayload(istringstream &in_stream) {
    const int variable_header_length{2};
    const int payload_length{remaining_length() - variable_header_length};
    if (0 == payload_length)
      return 0;
    if (0 > payload_length)
      return -104;

    int used_length{0};
    vector<string> topic_filters;
    while (used_length < payload_length && EOF != in_stream.peek()){
        string topic_filter;
        int ret{MqttProtocol::RecvUnicode(in_stream, topic_filter)};
        if (-105 == ret) {
            return 0;
        }

        if (0 != ret) {
            phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

            return ret;
        }

        topic_filters.emplace_back(topic_filter);

        used_length += topic_filter.length() + 2;
    }
    google::protobuf::RepeatedPtrField<string> tmp_topic_filters(
            topic_filters.begin(), topic_filters.end());
    pb_.mutable_topic_filters()->Swap(&tmp_topic_filters);

    return 0;
}

const MqttUnsubscribePb &MqttUnsubscribe::pb() const { return pb_; }


MqttUnsuback::MqttUnsuback() : MqttMessage(pb_), MqttResponse(pb_) {
    set_control_packet_type(MqttProtocol::ControlPacketType::UNSUBACK);
}

int MqttUnsuback::SendVariableHeader(ostringstream &out_stream) const {
    return MqttProtocol::SendUint16(out_stream, pb_.packet_identifier());
}

int MqttUnsuback::RecvVariableHeader(istringstream &in_stream) {
    uint16_t packet_identifier{0};
    int ret{MqttProtocol::RecvUint16(in_stream, packet_identifier)};
    if (0 == ret)
        pb_.set_packet_identifier(packet_identifier);

    return ret;
}

const MqttUnsubackPb &MqttUnsuback::pb() const { return pb_; }


MqttPingreq::MqttPingreq() : MqttMessage(pb_), MqttRequest(pb_) {
    set_uri("/phxqueue_phxrpc/mqttbroker/MqttPing");
    set_control_packet_type(MqttProtocol::ControlPacketType::PINGREQ);
}

phxrpc::BaseResponse *MqttPingreq::GenResponse() const { return new MqttPingresp; }

const MqttPingreqPb &MqttPingreq::pb() const { return pb_; }


MqttPingresp::MqttPingresp() : MqttMessage(pb_), MqttResponse(pb_) {
    set_control_packet_type(MqttProtocol::ControlPacketType::PINGRESP);
}

const MqttPingrespPb &MqttPingresp::pb() const { return pb_; }


MqttDisconnect::MqttDisconnect() : MqttMessage(pb_), MqttRequest(pb_) {
    set_uri("/phxqueue_phxrpc/mqttbroker/MqttDisconnect");
    set_control_packet_type(MqttProtocol::ControlPacketType::DISCONNECT);
}

phxrpc::BaseResponse *MqttDisconnect::GenResponse() const { return new MqttFakeResponse; }

const MqttDisconnectPb &MqttDisconnect::pb() const { return pb_; }


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

