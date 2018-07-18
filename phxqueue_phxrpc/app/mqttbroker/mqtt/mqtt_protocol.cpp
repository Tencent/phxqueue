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

#include "mqtt_protocol.h"


namespace phxqueue_phxrpc {

namespace mqttbroker {


using namespace std;


int MqttProtocol::EncodeUint16(string &dest, const uint16_t src) {
    dest.clear();
    dest.resize(2);
    dest[0] = static_cast<uint8_t>(src >> 8);
    dest[1] = static_cast<uint8_t>(src);

    return 0;
}

int MqttProtocol::EncodeUint16(char *const dest, const size_t dest_size, const uint16_t src) {
    if (2 != dest_size)
        return -1;

    dest[0] = static_cast<uint8_t>(src >> 8);
    dest[1] = static_cast<uint8_t>(src);

    return 0;
}

int MqttProtocol::EncodeUnicode(string &dest, const string &src) {
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

int MqttProtocol::EncodeUnicode(char *const dest, const size_t dest_size, const string &src) {
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

int MqttProtocol::SendChar(ostringstream &out_stream, const char &content) {
    out_stream.put(content);

    return 0;
}

int MqttProtocol::RecvChar(istringstream &in_stream, char &content) {
    in_stream.get(content);

    return 0;
}

int MqttProtocol::SendUint16(ostringstream &out_stream, const uint16_t content) {
    out_stream.put(static_cast<char>(content >> 8));
    out_stream.put(static_cast<char>(content));

    return 0;
}

int MqttProtocol::RecvUint16(istringstream &in_stream, uint16_t &content) {
    char temp{'\0'};
    in_stream.get(temp);
    content = (static_cast<uint8_t>(temp) << 8);
    temp = '\0';
    in_stream.get(temp);
    content |= static_cast<uint8_t>(temp);

    return 0;
}

int MqttProtocol::SendChars(ostringstream &out_stream, const char *const content,
              const int content_length) {
    out_stream.write(content, content_length);

    return 0;
}

int MqttProtocol::RecvChars(istringstream &in_stream, char *const content,
              const int content_length) {
    in_stream.read(content, content_length);

    return 0;
}

int MqttProtocol::SendUnicode(ostringstream &out_stream, const string &content) {
    uint16_t content_size{static_cast<uint16_t>(content.size())};
    int ret{SendUint16(out_stream, content_size)};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendUint16 err %d", static_cast<int>(ret));

        return ret;
    }

    out_stream.write(content.data(), content.size());

    return ret;
}

int MqttProtocol::RecvUnicode(istringstream &in_stream, string &content) {
    uint16_t content_size{0};
    int ret{RecvUint16(in_stream, content_size)};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "RecvUint16 err %d", static_cast<int>(ret));

        return ret;
    }

    content.resize(content_size);
    in_stream.read(&content[0], content_size);

    return ret;
}


int MqttProtocol::SendChar(phxrpc::BaseTcpStream &out_stream, const char &content) {
    out_stream.put(content);

    return 0;
}

int MqttProtocol::RecvChar(phxrpc::BaseTcpStream &in_stream, char &content) {
    in_stream.get(content);

    return 0;
}

int MqttProtocol::SendUint16(phxrpc::BaseTcpStream &out_stream, const uint16_t content) {
    out_stream.put(static_cast<char>(content >> 8));
    out_stream.put(static_cast<char>(content));

    return 0;
}

int MqttProtocol::RecvUint16(phxrpc::BaseTcpStream &in_stream, uint16_t &content) {
    char temp{'\0'};
    in_stream.get(temp);
    content = (static_cast<uint8_t>(temp) << 8);
    temp = '\0';
    in_stream.get(temp);
    content |= static_cast<uint8_t>(temp);

    return 0;
}

int MqttProtocol::SendChars(phxrpc::BaseTcpStream &out_stream, const char *const content,
              const int content_length) {
    out_stream.write(content, content_length);

    return 0;
}

int MqttProtocol::RecvChars(phxrpc::BaseTcpStream &in_stream, char *const content,
              const int content_length) {
    in_stream.read(content, content_length);

    return 0;
}

int MqttProtocol::SendUnicode(phxrpc::BaseTcpStream &out_stream, const string &content) {
    uint16_t content_size{static_cast<uint16_t>(content.size())};
    int ret{SendUint16(out_stream, content_size)};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "SendUint16 err %d", static_cast<int>(ret));

        return ret;
    }

    out_stream.write(content.data(), content.size());

    return ret;
}

int MqttProtocol::RecvUnicode(phxrpc::BaseTcpStream &in_stream, string &content) {
    uint16_t content_size{0};
    int ret{RecvUint16(in_stream, content_size)};
    if (0 != ret) {
        phxrpc::log(LOG_ERR, "RecvUint16 err %d", static_cast<int>(ret));

        return ret;
    }

    content.resize(content_size);
    in_stream.read(&content[0], content_size);

    return ret;
}


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

