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


namespace {


using namespace std;


int EncodeUint16(string &dest, const uint16_t src) {
    dest.clear();
    dest.resize(2);
    dest[0] = static_cast<uint8_t>(src >> 8);
    dest[1] = static_cast<uint8_t>(src);

    return 0;
}

int EncodeUint16(char *const dest, const size_t dest_size, const uint16_t src) {
    if (2 != dest_size)
        return -1;

    dest[0] = static_cast<uint8_t>(src >> 8);
    dest[1] = static_cast<uint8_t>(src);

    return 0;
}

int EncodeUnicode(string &dest, const string &src) {
    dest.clear();
    dest.resize(2 + src.size());
    uint16_t src_size{static_cast<uint16_t>(src.size())};
    dest[0] = static_cast<uint8_t>(src_size >> 8);
    dest[1] = static_cast<uint8_t>(src_size);
    for (int i{0}; src_size > i; ++i) {
        dest[i + 2] = src.at(i);
    }

    return 0;
}

int EncodeUnicode(char *const dest, const size_t dest_size, const string &src) {
    if (2 + src.size() != dest_size)
        return -1;

    uint16_t src_size{static_cast<uint16_t>(src.size())};
    dest[0] = static_cast<uint8_t>(src_size >> 8);
    dest[1] = static_cast<uint8_t>(src_size);
    for (int i{0}; src_size > i; ++i) {
        dest[i + 2] = src.at(i);
    }

    return 0;
}

phxrpc::ReturnCode SendChar(ostringstream &out_stream, const char &content) {
    out_stream.put(content);

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode RecvChar(istringstream &in_stream, char &content) {
    in_stream.get(content);

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode SendUint16(ostringstream &out_stream, const uint16_t content) {
    out_stream.put(static_cast<char>(content >> 8));
    out_stream.put(static_cast<char>(content));

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode RecvUint16(istringstream &in_stream, uint16_t &content) {
    char temp{'\0'};
    in_stream.get(temp);
    content = (static_cast<uint8_t>(temp) << 8);
    temp = '\0';
    in_stream.get(temp);
    content |= static_cast<uint8_t>(temp);

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode SendChars(ostringstream &out_stream, const char *const content,
                             const int content_length) {
    out_stream.write(content, content_length);

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode RecvChars(istringstream &in_stream, char *const content,
                             const int content_length) {
    in_stream.read(content, content_length);

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode SendUnicode(ostringstream &out_stream, const string &content) {
    uint16_t content_size{static_cast<uint16_t>(content.size())};
    phxrpc::ReturnCode ret{SendUint16(out_stream, content_size)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "SendUint16 err %d", static_cast<int>(ret));

        return ret;
    }

    out_stream.write(content.data(), content.size());

    return ret;
}

phxrpc::ReturnCode RecvUnicode(istringstream &in_stream, string &content) {
    uint16_t content_size{0};
    phxrpc::ReturnCode ret{RecvUint16(in_stream, content_size)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "RecvUint16 err %d", static_cast<int>(ret));

        return ret;
    }

    content.resize(content_size);
    in_stream.read(&content[0], content_size);

    return ret;
}


phxrpc::ReturnCode SendChar(phxrpc::BaseTcpStream &out_stream, const char &content) {
    out_stream.put(content);

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode RecvChar(phxrpc::BaseTcpStream &in_stream, char &content) {
    in_stream.get(content);

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode SendUint16(phxrpc::BaseTcpStream &out_stream, const uint16_t content) {
    out_stream.put(static_cast<char>(content >> 8));
    out_stream.put(static_cast<char>(content));

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode RecvUint16(phxrpc::BaseTcpStream &in_stream, uint16_t &content) {
    char temp{'\0'};
    in_stream.get(temp);
    content = (static_cast<uint8_t>(temp) << 8);
    temp = '\0';
    in_stream.get(temp);
    content |= static_cast<uint8_t>(temp);

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode SendChars(phxrpc::BaseTcpStream &out_stream, const char *const content,
                             const int content_length) {
    out_stream.write(content, content_length);

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode RecvChars(phxrpc::BaseTcpStream &in_stream, char *const content,
                             const int content_length) {
    in_stream.read(content, content_length);

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode SendUnicode(phxrpc::BaseTcpStream &out_stream, const string &content) {
    uint16_t content_size{static_cast<uint16_t>(content.size())};
    phxrpc::ReturnCode ret{SendUint16(out_stream, content_size)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "SendUint16 err %d", static_cast<int>(ret));

        return ret;
    }

    out_stream.write(content.data(), content.size());

    return ret;
}

phxrpc::ReturnCode RecvUnicode(phxrpc::BaseTcpStream &in_stream, string &content) {
    uint16_t content_size{0};
    phxrpc::ReturnCode ret{RecvUint16(in_stream, content_size)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "RecvUint16 err %d", static_cast<int>(ret));

        return ret;
    }

    content.resize(content_size);
    in_stream.read(&content[0], content_size);

    return ret;
}


}  // namespace


namespace phxqueue_phxrpc {

namespace mqttbroker {


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


MqttMessage::MqttMessage() {
}

MqttMessage::~MqttMessage() {}

uint8_t MqttMessage::EncodeFixedHeader(const FixedHeader &fixed_header) {
    const int control_packet_type_int{static_cast<int>(fixed_header.control_packet_type)};
    const char fixed_header_char{SampleFixedHeader[control_packet_type_int]};
    uint8_t fixed_header_byte{static_cast<uint8_t>(fixed_header_char)};
    if (ControlPacketType::PUBLISH == fixed_header.control_packet_type) {
        fixed_header.dup ? (fixed_header_byte |= 0x8) : (fixed_header_byte &= ~0x8);
        fixed_header_byte &= ~0x6;
        fixed_header_byte |= (static_cast<uint8_t>(fixed_header.qos) << 1);
        fixed_header.retain ? (fixed_header_byte |= 0x1) : (fixed_header_byte &= ~0x1);
    }

    return fixed_header_byte;
}

MqttMessage::FixedHeader MqttMessage::DecodeFixedHeader(const uint8_t fixed_header_byte) {
    FixedHeader fixed_header;

    fixed_header.dup = static_cast<bool>((fixed_header_byte >> 3) & 0x01);
    fixed_header.qos = static_cast<int>((fixed_header_byte >> 1) & 0x03);
    fixed_header.retain = static_cast<bool>(fixed_header_byte & 0x01);

    uint8_t temp{fixed_header_byte};
    temp >>= 4;
    temp &= 0x0f;
    // must convert to unsigned first
    fixed_header.control_packet_type = static_cast<ControlPacketType>(temp);

    return fixed_header;
}

phxrpc::ReturnCode MqttMessage::SendFixedHeaderAndRemainingBuffer(
        phxrpc::BaseTcpStream &out_stream, const FixedHeader &fixed_header,
        const string &remaining_buffer) {
    uint8_t fixed_header_byte{EncodeFixedHeader(fixed_header)};
    phxrpc::ReturnCode ret{SendChar(out_stream, static_cast<char>(fixed_header_byte))};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "SendChar err %d", static_cast<int>(ret));

        return ret;
    } else {
        phxrpc::log(LOG_DEBUG, "SendChar type %d fixed_header_byte %u",
                    static_cast<int>(fixed_header.control_packet_type),
                    static_cast<uint8_t>(fixed_header_byte));
    }

    const int remaining_length{static_cast<const int>(remaining_buffer.size())};
    ret = SendRemainingLength(out_stream, remaining_length);
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "SendRemainingLength err %d", static_cast<int>(ret));

        return ret;
    }

    ret = SendChars(out_stream, remaining_buffer.data(),
                    remaining_buffer.size());
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "SendChars err %d", static_cast<int>(ret));

        return ret;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttMessage::RecvFixedHeaderAndRemainingBuffer(
        phxrpc::BaseTcpStream &in_stream, FixedHeader &fixed_header,
        string &remaining_buffer) {
    char fixed_header_char{0x0};
    phxrpc::ReturnCode ret{RecvChar(in_stream, fixed_header_char)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "RecvChar err %d", static_cast<int>(ret));

        return ret;
    }

    fixed_header = DecodeFixedHeader(static_cast<uint8_t>(fixed_header_char));

    phxrpc::log(LOG_DEBUG, "RecvChar type %d fixed_header %x",
                static_cast<int>(fixed_header.control_packet_type),
                static_cast<uint8_t>(fixed_header_char));

    int remaining_length{0};
    ret = RecvRemainingLength(in_stream, remaining_length);

    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "RecvRemainingLength err %d", static_cast<int>(ret));

        return ret;
    }

    remaining_buffer.resize(remaining_length);
    ret = RecvChars(in_stream, &remaining_buffer[0], remaining_length);
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "RecvChars err %d", static_cast<int>(ret));

        return ret;
    }

    // TODO: remove
    //for (int i{0}; remaining_length > i; ++i) {
    //    printf("%d\t", i);
    //}
    //printf("\n");
    //for (int i{0}; remaining_length > i; ++i) {
    //    printf("%c\t", remaining_buffer[i]);
    //}
    //printf("\n");
    //for (int i{0}; remaining_length > i; ++i) {
    //    printf("%d\t", remaining_buffer[i]);
    //}
    //printf("\n");

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttMessage::SendRemainingLength(phxrpc::BaseTcpStream &out_stream,
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
            return phxrpc::ReturnCode::OK;
        }
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttMessage::RecvRemainingLength(phxrpc::BaseTcpStream &in_stream,
                                                    int &remaining_length) {
    uint32_t temp_remaining_length{0};

    char temp{0x0};
    in_stream.get(temp);
    temp_remaining_length = (static_cast<uint8_t>(temp) & 0x7f);

    if (!(static_cast<uint8_t>(temp) & 0x80)) {
        remaining_length = temp_remaining_length;

        return phxrpc::ReturnCode::OK;
    }

    temp = 0x0;
    in_stream.get(temp);
    temp_remaining_length |= (static_cast<uint8_t>(temp) & 0x7f) << 7;
    if (!(static_cast<uint8_t>(temp) & 0x80)) {
        remaining_length = temp_remaining_length;

        return phxrpc::ReturnCode::OK;
    }

    temp = 0x0;
    in_stream.get(temp);
    temp_remaining_length |= (static_cast<uint8_t>(temp) & 0x7f) << 14;
    if (!(static_cast<uint8_t>(temp) & 0x80)) {
        remaining_length = temp_remaining_length;

        return phxrpc::ReturnCode::OK;
    }

    temp = 0x0;
    in_stream.get(temp);
    temp_remaining_length |= (static_cast<uint8_t>(temp) & 0x7f) << 21;

    remaining_length = temp_remaining_length;

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttMessage::Send(phxrpc::BaseTcpStream &socket) const {
    ostringstream ss;
    phxrpc::ReturnCode ret{SendRemaining(ss)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "SendRemaining err %d", static_cast<int>(ret));

        return ret;
    }

    ret = SendFixedHeaderAndRemainingBuffer(socket, fixed_header(), ss.str());
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "SendFixedHeaderAndRemainingBuffer err %d", static_cast<int>(ret));

        return ret;
    }

    if (!socket.flush().good()) {
        phxrpc::log(LOG_ERR, "socket err %d", socket.LastError());

        return static_cast<phxrpc::ReturnCode>(socket.LastError());
    }

    return ret;
}

size_t MqttMessage::size() const {
    // TODO: add header size
    return data_.size();
}

phxrpc::ReturnCode MqttMessage::SendRemaining(ostringstream &out_stream) const {
    phxrpc::ReturnCode ret{SendVariableHeader(out_stream)};
    if (phxrpc::ReturnCode::OK != ret) {
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
    if (phxrpc::ReturnCode::OK != ret) {
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

phxrpc::ReturnCode MqttMessage::RecvRemaining(istringstream &in_stream) {
    phxrpc::ReturnCode ret{RecvVariableHeader(in_stream)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "RecvVariableHeader err %d", static_cast<int>(ret));

        return ret;
    }

    ret = RecvPayload(in_stream);
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "RecvPayload err %d", static_cast<int>(ret));

        return ret;
    }
    //phxrpc::log(LOG_DEBUG, "RecvPayload \"%s\"", data_.c_str());

    return ret;
}

phxrpc::ReturnCode MqttMessage::SendPacketIdentifier(ostringstream &out_stream) const {
    phxrpc::ReturnCode ret{SendUint16(out_stream, packet_identifier_)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "SendUint16 err %d", static_cast<int>(ret));

        return ret;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttMessage::RecvPacketIdentifier(istringstream &in_stream) {
    phxrpc::ReturnCode ret{RecvUint16(in_stream, packet_identifier_)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "RecvUint16 err %d", static_cast<int>(ret));

        return ret;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttResponse::ModifyResp(const bool keep_alive, const string &version) {
    return phxrpc::ReturnCode::OK;
}


MqttFakeResponse::MqttFakeResponse() {
    mutable_fixed_header().control_packet_type = ControlPacketType::FAKE_NONE;
    set_fake(true);
}


MqttConnect::MqttConnect() {
    set_uri("/phxqueue_phxrpc.mqttbroker/PhxMqttConnect");
    mutable_fixed_header().control_packet_type = ControlPacketType::CONNECT;
}

phxrpc::ReturnCode MqttConnect::ToPb(google::protobuf::Message *const message) const {
    MqttConnectPb connect;

    connect.set_client_identifier(client_identifier_);
    connect.set_proto_name(proto_name_);
    connect.set_proto_level(proto_level_);
    connect.set_clean_session(clean_session_);
    connect.set_keep_alive(keep_alive_);
    connect.set_user_name(user_name_);
    connect.set_password(password_);
    connect.set_will_flag(will_flag_);
    connect.set_will_qos(will_qos_);
    connect.set_will_retain(will_retain_);
    connect.set_will_topic(will_topic_);
    connect.set_will_message(will_message_);


    // TODO: remove
    //printf("%s client_id %s\n", __func__, connect.client_identifier().c_str());

    try {
        message->CopyFrom(connect);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttConnect::FromPb(const google::protobuf::Message &message) {
    MqttConnectPb connect;

    try {
        connect.CopyFrom(message);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    client_identifier_ = connect.client_identifier();
    proto_name_ = connect.proto_name();
    proto_level_ = connect.proto_level();
    clean_session_ = connect.clean_session();
    keep_alive_ = connect.keep_alive();
    user_name_ = connect.user_name();
    password_ = connect.password();
    will_flag_ = connect.will_flag();
    will_qos_ = connect.will_qos();
    will_retain_ = connect.will_retain();
    will_topic_ = connect.will_topic();
    will_message_ = connect.will_message();

    return phxrpc::ReturnCode::OK;
}

phxrpc::BaseResponse *MqttConnect::GenResponse() const { return new MqttConnack; }

phxrpc::ReturnCode MqttConnect::SendVariableHeader(ostringstream &out_stream) const {
    phxrpc::ReturnCode ret{SendUnicode(out_stream, proto_name_)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

        return ret;
    }

    ret = SendChar(out_stream, proto_level_);
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "SendChar err %d", static_cast<int>(ret));

        return ret;
    }

    uint8_t connect_flags{0x0};
    connect_flags |= ((clean_session_ ? 0x1 : 0x0) << 1);
    connect_flags |= ((will_flag_ ? 0x1 : 0x0) << 2);
    connect_flags |= (will_qos_ << 3);
    connect_flags |= ((will_retain_ ? 0x1 : 0x0) << 5);
    connect_flags |= ((user_name_flag_ ? 0x0 : 0x1) << 6);
    connect_flags |= ((password_flag_ ? 0x0 : 0x1) << 7);
    ret = SendChar(out_stream, static_cast<char>(connect_flags));
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "SendChar err %d", static_cast<int>(ret));

        return ret;
    }

    ret = SendUint16(out_stream, keep_alive_);
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "SendUint16 err %d", static_cast<int>(ret));

        return ret;
    }

    return ret;
}

phxrpc::ReturnCode MqttConnect::RecvVariableHeader(istringstream &in_stream) {
    phxrpc::ReturnCode ret{RecvUnicode(in_stream, proto_name_)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

        return ret;
    }

    // MQTT 3.1.1: "MQTT"
    // MQTT 3.1: "MQIsdp"
    if (proto_name_ != "MQTT") {
        phxrpc::log(LOG_ERR, "violate mqtt protocol");

        return phxrpc::ReturnCode::ERROR_VIOLATE_PROTOCOL;
    }

    char proto_level{'\0'};
    ret = RecvChar(in_stream, proto_level);
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "RecvChar err %d", static_cast<int>(ret));

        return ret;
    }
    proto_level_ = proto_level;

    char connect_flags{0x0};
    ret = RecvChar(in_stream, connect_flags);
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "RecvChar err %d", static_cast<int>(ret));

        return ret;
    }
    clean_session_ = (0x0 != (connect_flags & 0x2));
    will_flag_ = (0x0 != (connect_flags & 0x4));
    will_qos_ = ((connect_flags & 0x8) >> 3);
    will_qos_ |= ((connect_flags & 0x10) >> 3);
    will_retain_ = (0x0 != (connect_flags & 0x20));
    user_name_flag_ = (0x0 != (connect_flags & 0x40));
    password_flag_ = (0x0 != (connect_flags & 0x80));

    ret = RecvUint16(in_stream, keep_alive_);
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "RecvUint16 err %d", static_cast<int>(ret));

        return ret;
    }

    return ret;
}

phxrpc::ReturnCode MqttConnect::SendPayload(ostringstream &out_stream) const {
    phxrpc::ReturnCode ret{SendUnicode(out_stream, client_identifier_)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

        return ret;
    }

    if (will_flag_) {
        ret = SendUnicode(out_stream, will_topic_);
        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

            return ret;
        }

        ret = SendUnicode(out_stream, will_message_);
        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

            return ret;
        }
    }

    if (user_name_flag_) {
        ret = SendUnicode(out_stream, user_name_);
        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

            return ret;
        }
    }

    if (password_flag_) {
        ret = SendUnicode(out_stream, password_);
        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

            return ret;
        }
    }

    return ret;
}

phxrpc::ReturnCode MqttConnect::RecvPayload(istringstream &in_stream) {
    phxrpc::ReturnCode ret{RecvUnicode(in_stream, client_identifier_)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

        return ret;
    }
    // TODO: remove
    //printf("client_id %s\n", client_identifier_.c_str());

    if (will_flag_) {
        ret = RecvUnicode(in_stream, will_topic_);
        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

            return ret;
        }

        ret = RecvUnicode(in_stream, will_message_);
        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

            return ret;
        }
    }

    if (user_name_flag_) {
        ret = RecvUnicode(in_stream, user_name_);
        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

            return ret;
        }
    }

    if (password_flag_) {
        ret = RecvUnicode(in_stream, password_);
        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

            return ret;
        }
    }

    return ret;
}


MqttConnack::MqttConnack() {
    mutable_fixed_header().control_packet_type = ControlPacketType::CONNACK;
}

phxrpc::ReturnCode MqttConnack::ToPb(google::protobuf::Message *const message) const {
    MqttConnackPb connack;

    connack.set_session_present(session_present_);
    connack.set_connect_return_code(connect_return_code_);

    try {
        message->CopyFrom(connack);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttConnack::FromPb(const google::protobuf::Message &message) {
    MqttConnackPb connack;

    try {
        connack.CopyFrom(message);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    session_present_ = connack.session_present();
    connect_return_code_ = connack.connect_return_code();

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttConnack::SendVariableHeader(ostringstream &out_stream) const {
    phxrpc::ReturnCode ret{SendChar(out_stream, session_present_ ? 0x1 : 0x0)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "SendChar err %d", static_cast<int>(ret));

        return ret;
    }

    ret = SendChar(out_stream, connect_return_code_);
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "SendChar err %d", static_cast<int>(ret));

        return ret;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttConnack::RecvVariableHeader(istringstream &in_stream) {
    char connect_acknowledge_flags{0x0};
    phxrpc::ReturnCode ret{RecvChar(in_stream, connect_acknowledge_flags)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "RecvChar err %d", static_cast<int>(ret));

        return ret;
    }
    session_present_ = (0x1 == (connect_acknowledge_flags & 0x1));

    ret = RecvChar(in_stream, connect_return_code_);
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "RecvChar err %d", static_cast<int>(ret));

        return ret;
    }

    return phxrpc::ReturnCode::OK;
}


MqttPublish::MqttPublish() {
    set_uri("/phxqueue_phxrpc.mqttbroker/PhxMqttPublish");
    mutable_fixed_header().control_packet_type = ControlPacketType::PUBLISH;
}

phxrpc::ReturnCode MqttPublish::ToPb(google::protobuf::Message *const message) const {
    MqttPublishPb publish;

    publish.set_dup(fixed_header().dup);
    publish.set_qos(fixed_header().qos);
    publish.set_retain(fixed_header().retain);

    publish.set_topic_name(topic_name_);
    publish.set_content(data_);
    publish.set_packet_identifier(packet_identifier());

    try {
        message->CopyFrom(publish);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttPublish::FromPb(const google::protobuf::Message &message) {
    MqttPublishPb publish;

    try {
        publish.CopyFrom(message);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    FixedHeader &fixed_header(mutable_fixed_header());
    fixed_header.dup = publish.dup();
    fixed_header.qos = publish.qos();
    fixed_header.retain = publish.retain();

    topic_name_ = publish.topic_name();
    data_ = publish.content();
    set_packet_identifier(publish.packet_identifier());

    return phxrpc::ReturnCode::OK;
}

phxrpc::BaseResponse *MqttPublish::GenResponse() const { return new MqttFakeResponse; }

phxrpc::ReturnCode MqttPublish::SendVariableHeader(ostringstream &out_stream) const {
    phxrpc::ReturnCode ret{SendUnicode(out_stream, topic_name_)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

        return ret;
    }

    if (0 < fixed_header().qos) {
        ret = SendPacketIdentifier(out_stream);
        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "SendPacketIdentifier err %d", static_cast<int>(ret));

            return ret;
        }
    }

    return ret;
}

phxrpc::ReturnCode MqttPublish::RecvVariableHeader(istringstream &in_stream) {
    phxrpc::ReturnCode ret{RecvUnicode(in_stream, topic_name_)};
    if (phxrpc::ReturnCode::OK != ret) {
        phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

        return ret;
    }

    if (0 < fixed_header().qos) {
        ret = RecvPacketIdentifier(in_stream);
        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "RecvPacketIdentifier err %d", static_cast<int>(ret));

            return ret;
        }
    }

    return ret;
}

phxrpc::ReturnCode MqttPublish::SendPayload(ostringstream &out_stream) const {
    return SendChars(out_stream, data_.data(), data_.size());
}

phxrpc::ReturnCode MqttPublish::RecvPayload(istringstream &in_stream) {
    int variable_header_length{static_cast<int>(topic_name_.length()) + 2};
    if (0 < fixed_header().qos) {
        variable_header_length += 2;
    }
    const int payload_length{remaining_length() - variable_header_length};
    if (0 == payload_length)
      return phxrpc::ReturnCode::OK;
    if (0 > payload_length)
      return phxrpc::ReturnCode::ERROR_LENGTH_UNDERFLOW;

    string &payload_buffer(data_);
    payload_buffer.resize(payload_length);
    return RecvChars(in_stream, &payload_buffer[0], payload_length);
}


MqttPuback::MqttPuback() {
    set_uri("/phxqueue_phxrpc.mqttbroker/PhxMqttPuback");
    mutable_fixed_header().control_packet_type = ControlPacketType::PUBACK;
}

phxrpc::ReturnCode MqttPuback::ToPb(google::protobuf::Message *const message) const {
    MqttPubackPb puback;

    puback.set_packet_identifier(packet_identifier());

    try {
        message->CopyFrom(puback);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttPuback::FromPb(const google::protobuf::Message &message) {
    MqttPubackPb puback;

    try {
        puback.CopyFrom(message);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    set_packet_identifier(puback.packet_identifier());

    return phxrpc::ReturnCode::OK;
}

phxrpc::BaseResponse *MqttPuback::GenResponse() const { return new MqttFakeResponse; }

phxrpc::ReturnCode MqttPuback::SendVariableHeader(ostringstream &out_stream) const {
    return SendPacketIdentifier(out_stream);
}

phxrpc::ReturnCode MqttPuback::RecvVariableHeader(istringstream &in_stream) {
    return RecvPacketIdentifier(in_stream);
}


MqttPubrec::MqttPubrec() {
    set_uri("/phxqueue_phxrpc.mqttbroker/PhxMqttPubrec");
    mutable_fixed_header().control_packet_type = ControlPacketType::PUBREC;
}

phxrpc::ReturnCode MqttPubrec::ToPb(google::protobuf::Message *const message) const {
    MqttPubrecPb pubrec;

    try {
        message->CopyFrom(pubrec);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttPubrec::FromPb(const google::protobuf::Message &message) {
    MqttPubrecPb pubrec;

    try {
        pubrec.CopyFrom(message);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::BaseResponse *MqttPubrec::GenResponse() const { return new MqttFakeResponse; }

phxrpc::ReturnCode MqttPubrec::SendVariableHeader(ostringstream &out_stream) const {
    return phxrpc::ReturnCode::ERROR_UNIMPLEMENT;
}

phxrpc::ReturnCode MqttPubrec::RecvVariableHeader(istringstream &in_stream) {
    return phxrpc::ReturnCode::ERROR_UNIMPLEMENT;
}

phxrpc::ReturnCode MqttPubrec::SendPayload(ostringstream &out_stream) const {
    return phxrpc::ReturnCode::ERROR_UNIMPLEMENT;
}

phxrpc::ReturnCode MqttPubrec::RecvPayload(istringstream &in_stream) {
    return phxrpc::ReturnCode::ERROR_UNIMPLEMENT;
}


MqttPubrel::MqttPubrel() {
    set_uri("/phxqueue_phxrpc.mqttbroker/PhxMqttPubrel");
    mutable_fixed_header().control_packet_type = ControlPacketType::PUBREL;
}

phxrpc::ReturnCode MqttPubrel::ToPb(google::protobuf::Message *const message) const {
    MqttPubrelPb pubrel;

    try {
        message->CopyFrom(pubrel);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttPubrel::FromPb(const google::protobuf::Message &message) {
    MqttPubrelPb pubrel;

    try {
        pubrel.CopyFrom(message);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::BaseResponse *MqttPubrel::GenResponse() const { return new MqttFakeResponse; }

phxrpc::ReturnCode MqttPubrel::SendVariableHeader(ostringstream &out_stream) const {
    return phxrpc::ReturnCode::ERROR_UNIMPLEMENT;
}

phxrpc::ReturnCode MqttPubrel::RecvVariableHeader(istringstream &in_stream) {
    return phxrpc::ReturnCode::ERROR_UNIMPLEMENT;
}

phxrpc::ReturnCode MqttPubrel::SendPayload(ostringstream &out_stream) const {
    return phxrpc::ReturnCode::ERROR_UNIMPLEMENT;
}

phxrpc::ReturnCode MqttPubrel::RecvPayload(istringstream &in_stream) {
    return phxrpc::ReturnCode::ERROR_UNIMPLEMENT;
}


MqttPubcomp::MqttPubcomp() {
    set_uri("/phxqueue_phxrpc.mqttbroker/PhxMqttPubcomp");
    mutable_fixed_header().control_packet_type = ControlPacketType::PUBCOMP;
}

phxrpc::ReturnCode MqttPubcomp::ToPb(google::protobuf::Message *const message) const {
    MqttPubcompPb pubcomp;

    try {
        message->CopyFrom(pubcomp);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttPubcomp::FromPb(const google::protobuf::Message &message) {
    MqttPubcompPb pubcomp;

    try {
        pubcomp.CopyFrom(message);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::BaseResponse *MqttPubcomp::GenResponse() const { return new MqttFakeResponse; }

phxrpc::ReturnCode MqttPubcomp::SendVariableHeader(ostringstream &out_stream) const {
    return phxrpc::ReturnCode::ERROR_UNIMPLEMENT;
}

phxrpc::ReturnCode MqttPubcomp::RecvVariableHeader(istringstream &in_stream) {
    return phxrpc::ReturnCode::ERROR_UNIMPLEMENT;
}

phxrpc::ReturnCode MqttPubcomp::SendPayload(ostringstream &out_stream) const {
    return phxrpc::ReturnCode::ERROR_UNIMPLEMENT;
}

phxrpc::ReturnCode MqttPubcomp::RecvPayload(istringstream &in_stream) {
    return phxrpc::ReturnCode::ERROR_UNIMPLEMENT;
}


MqttSubscribe::MqttSubscribe() {
    set_uri("/phxqueue_phxrpc.mqttbroker/PhxMqttSubscribe");
    mutable_fixed_header().control_packet_type = ControlPacketType::SUBSCRIBE;
}

phxrpc::ReturnCode MqttSubscribe::ToPb(google::protobuf::Message *const message) const {
    MqttSubscribePb subscribe;

    subscribe.set_packet_identifier(packet_identifier());
    google::protobuf::RepeatedPtrField<string> temp_topic_filters(
            topic_filters_.begin(), topic_filters_.end());
    subscribe.mutable_topic_filters()->Swap(&temp_topic_filters);
    google::protobuf::RepeatedField<uint32_t> temp_qoss(
            qoss_.begin(), qoss_.end());
    subscribe.mutable_qoss()->Swap(&temp_qoss);

    try {
        message->CopyFrom(subscribe);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttSubscribe::FromPb(const google::protobuf::Message &message) {
    MqttSubscribePb subscribe;

    try {
        subscribe.CopyFrom(message);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    set_packet_identifier(subscribe.packet_identifier());
    copy(subscribe.topic_filters().begin(), subscribe.topic_filters().end(),
         back_inserter(topic_filters_));
    copy(subscribe.qoss().begin(), subscribe.qoss().end(),
         back_inserter(qoss_));

    return phxrpc::ReturnCode::OK;
}

phxrpc::BaseResponse *MqttSubscribe::GenResponse() const { return new MqttSuback; }

phxrpc::ReturnCode MqttSubscribe::SendVariableHeader(ostringstream &out_stream) const {
    return SendPacketIdentifier(out_stream);
}

phxrpc::ReturnCode MqttSubscribe::RecvVariableHeader(istringstream &in_stream) {
    return RecvPacketIdentifier(in_stream);
}

phxrpc::ReturnCode MqttSubscribe::SendPayload(ostringstream &out_stream) const {
    for (int i{0}; topic_filters_.size() > i; ++i) {
        phxrpc::ReturnCode ret{SendUnicode(out_stream, topic_filters_.at(i))};
        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

            return ret;
        }

        if (qoss_.size() > i) {
            ret = SendChar(out_stream, qoss_.at(i));
        } else {
            ret = SendChar(out_stream, 0x0);
        }
        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "SendChar err %d", static_cast<int>(ret));

            return ret;
        }
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttSubscribe::RecvPayload(istringstream &in_stream) {
    const int variable_header_length{2};
    const int payload_length{remaining_length() - variable_header_length};
    if (0 == payload_length)
      return phxrpc::ReturnCode::OK;
    if (0 > payload_length)
      return phxrpc::ReturnCode::ERROR_LENGTH_UNDERFLOW;

    int used_length{0};
    topic_filters_.clear();
    qoss_.clear();
    while (used_length < payload_length && EOF != in_stream.peek()) {
        string topic_filter;
        phxrpc::ReturnCode ret{RecvUnicode(in_stream, topic_filter)};
        if (phxrpc::ReturnCode::ERROR_LENGTH_OVERFLOW == ret) {
            return phxrpc::ReturnCode::OK;
        }

        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

            return ret;
        }

        char requested_qos{0x0};
        ret = RecvChar(in_stream, requested_qos);
        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "RecvChar err %d", static_cast<int>(ret));

            return ret;
        }

        topic_filters_.emplace_back(topic_filter);
        qoss_.emplace_back(requested_qos);

        used_length += topic_filter.length() + 3;
    }

    return phxrpc::ReturnCode::OK;
}


MqttSuback::MqttSuback() {
    mutable_fixed_header().control_packet_type = ControlPacketType::SUBACK;
}

phxrpc::ReturnCode MqttSuback::ToPb(google::protobuf::Message *const message) const {
    MqttSubackPb suback;

    suback.set_packet_identifier(packet_identifier());
    google::protobuf::RepeatedField<uint32_t> temp_return_codes(
            return_codes_.begin(), return_codes_.end());
    suback.mutable_return_codes()->Swap(&temp_return_codes);

    try {
        message->CopyFrom(suback);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttSuback::FromPb(const google::protobuf::Message &message) {
    MqttSubackPb suback;

    try {
        suback.CopyFrom(message);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    set_packet_identifier(suback.packet_identifier());
    copy(suback.return_codes().begin(), suback.return_codes().end(),
         back_inserter(return_codes_));

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttSuback::SendVariableHeader(ostringstream &out_stream) const {
    return SendPacketIdentifier(out_stream);
}

phxrpc::ReturnCode MqttSuback::RecvVariableHeader(istringstream &in_stream) {
    return RecvPacketIdentifier(in_stream);
}

phxrpc::ReturnCode MqttSuback::SendPayload(ostringstream &out_stream) const {
    for (int i{0}; return_codes_.size() > i; ++i) {
        phxrpc::ReturnCode ret{SendChar(out_stream, return_codes_.at(i))};
        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "SendChar err %d", static_cast<int>(ret));

            return ret;
        }
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttSuback::RecvPayload(istringstream &in_stream) {
    while (EOF != in_stream.peek()) {
        char return_code{0x0};
        phxrpc::ReturnCode ret{RecvChar(in_stream, return_code)};
        if (phxrpc::ReturnCode::ERROR_LENGTH_OVERFLOW == ret) {
            return phxrpc::ReturnCode::OK;
        }

        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "RecvChar err %d", static_cast<int>(ret));

            return ret;
        }

        return_codes_.resize(return_codes_.size() + 1);
        return_codes_[return_codes_.size()] = return_code;
    }

    return phxrpc::ReturnCode::OK;
}


MqttUnsubscribe::MqttUnsubscribe() {
    set_uri("/phxqueue_phxrpc.mqttbroker/PhxMqttUnsubscribe");
    mutable_fixed_header().control_packet_type = ControlPacketType::UNSUBSCRIBE;
}

phxrpc::ReturnCode MqttUnsubscribe::ToPb(google::protobuf::Message *const message) const {
    MqttUnsubscribePb unsubscribe;

    unsubscribe.set_packet_identifier(packet_identifier());
    google::protobuf::RepeatedPtrField<string> temp_topic_filters(
            topic_filters_.begin(), topic_filters_.end());
    unsubscribe.mutable_topic_filters()->Swap(&temp_topic_filters);

    try {
        message->CopyFrom(unsubscribe);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttUnsubscribe::FromPb(const google::protobuf::Message &message) {
    MqttUnsubscribePb unsubscribe;

    try {
        unsubscribe.CopyFrom(message);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    set_packet_identifier(unsubscribe.packet_identifier());
    copy(unsubscribe.topic_filters().begin(), unsubscribe.topic_filters().end(),
         back_inserter(topic_filters_));

    return phxrpc::ReturnCode::OK;
}

phxrpc::BaseResponse *MqttUnsubscribe::GenResponse() const { return new MqttUnsuback; }

phxrpc::ReturnCode
MqttUnsubscribe::SendVariableHeader(ostringstream &out_stream) const {
    return SendPacketIdentifier(out_stream);
}

phxrpc::ReturnCode
MqttUnsubscribe::RecvVariableHeader(istringstream &in_stream) {
    return RecvPacketIdentifier(in_stream);
}

phxrpc::ReturnCode
MqttUnsubscribe::SendPayload(ostringstream &out_stream) const {
    for (int i{0}; topic_filters_.size() > i; ++i) {
        phxrpc::ReturnCode ret{SendUnicode(out_stream, topic_filters_.at(i))};
        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "SendUnicode err %d", static_cast<int>(ret));

            return ret;
        }
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode
MqttUnsubscribe::RecvPayload(istringstream &in_stream) {
    const int variable_header_length{2};
    const int payload_length{remaining_length() - variable_header_length};
    if (0 == payload_length)
      return phxrpc::ReturnCode::OK;
    if (0 > payload_length)
      return phxrpc::ReturnCode::ERROR_LENGTH_UNDERFLOW;

    int used_length{0};
    topic_filters_.clear();
    while (used_length < payload_length && EOF != in_stream.peek()){
        string topic_filter;
        phxrpc::ReturnCode ret{RecvUnicode(in_stream, topic_filter)};
        if (phxrpc::ReturnCode::ERROR_LENGTH_OVERFLOW == ret) {
            return phxrpc::ReturnCode::OK;
        }

        if (phxrpc::ReturnCode::OK != ret) {
            phxrpc::log(LOG_ERR, "RecvUnicode err %d", static_cast<int>(ret));

            return ret;
        }

        topic_filters_.emplace_back(topic_filter);

        used_length += topic_filter.length() + 2;
    }

    return phxrpc::ReturnCode::OK;
}


MqttUnsuback::MqttUnsuback() {
    mutable_fixed_header().control_packet_type = ControlPacketType::UNSUBACK;
}

phxrpc::ReturnCode MqttUnsuback::ToPb(google::protobuf::Message *const message) const {
    MqttUnsubackPb unsuback;

    unsuback.set_packet_identifier(packet_identifier());

    try {
        message->CopyFrom(unsuback);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttUnsuback::FromPb(const google::protobuf::Message &message) {
    MqttUnsubackPb unsuback;

    try {
        unsuback.CopyFrom(message);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    set_packet_identifier(unsuback.packet_identifier());

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttUnsuback::SendVariableHeader(ostringstream &out_stream) const {
    return SendPacketIdentifier(out_stream);
}

phxrpc::ReturnCode MqttUnsuback::RecvVariableHeader(istringstream &in_stream) {
    return RecvPacketIdentifier(in_stream);
}


MqttPingreq::MqttPingreq() {
    set_uri("/phxqueue_phxrpc.mqttbroker/PhxMqttPing");
    mutable_fixed_header().control_packet_type = ControlPacketType::PINGREQ;
}

phxrpc::ReturnCode MqttPingreq::ToPb(google::protobuf::Message *const message) const {
    MqttPingreqPb pingreq;

    try {
        message->CopyFrom(pingreq);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttPingreq::FromPb(const google::protobuf::Message &message) {
    MqttPingreqPb pingreq;

    try {
        pingreq.CopyFrom(message);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::BaseResponse *MqttPingreq::GenResponse() const { return new MqttPingresp; }


MqttPingresp::MqttPingresp() {
    mutable_fixed_header().control_packet_type = ControlPacketType::PINGRESP;
}

phxrpc::ReturnCode MqttPingresp::ToPb(google::protobuf::Message *const message) const {
    MqttPingrespPb pingresp;

    try {
        message->CopyFrom(pingresp);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttPingresp::FromPb(const google::protobuf::Message &message) {
    MqttPingrespPb pingresp;

    try {
        pingresp.CopyFrom(message);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}


MqttDisconnect::MqttDisconnect() {
    set_uri("/phxqueue_phxrpc.mqttbroker/PhxMqttDisconnect");
    mutable_fixed_header().control_packet_type = ControlPacketType::DISCONNECT;
}

phxrpc::ReturnCode MqttDisconnect::ToPb(google::protobuf::Message *const message) const {
    MqttDisconnectPb disconnect;

    try {
        message->CopyFrom(disconnect);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::ReturnCode MqttDisconnect::FromPb(const google::protobuf::Message &message) {
    MqttDisconnectPb disconnect;

    try {
        disconnect.CopyFrom(message);
    } catch (exception) {
        return phxrpc::ReturnCode::ERROR;
    }

    return phxrpc::ReturnCode::OK;
}

phxrpc::BaseResponse *MqttDisconnect::GenResponse() const { return new MqttFakeResponse; }


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

