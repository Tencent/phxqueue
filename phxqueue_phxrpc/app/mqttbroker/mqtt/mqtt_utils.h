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


#include <sstream>

#include "phxrpc/file.h"
#include "phxrpc/network.h"


namespace phxqueue_phxrpc {

namespace mqttbroker {


enum class ControlPacketType {
    FAKE_NONE = 0,
    CONNECT = 1,
    CONNACK,
    PUBLISH,
    PUBACK,
    PUBREC,
    PUBREL,
    PUBCOMP,
    SUBSCRIBE,
    SUBACK,
    UNSUBSCRIBE,
    UNSUBACK,
    PINGREQ,
    PINGRESP,
    DISCONNECT,
    FAKE_MAX,
};


int EncodeUint16(std::string &dest, const uint16_t src);
int EncodeUint16(char *const dest, const size_t dest_size, const uint16_t src);

int EncodeUnicode(std::string &dest, const std::string &src);
int EncodeUnicode(char *const dest, const size_t dest_size, const std::string &src);

int SendChar(std::ostringstream &out_stream, const char &content);
int RecvChar(std::istringstream &in_stream, char &content);

int SendUint16(std::ostringstream &out_stream, const uint16_t content);
int RecvUint16(std::istringstream &in_stream, uint16_t &content);

int SendChars(std::ostringstream &out_stream, const char *const content,
              const int content_length);
int RecvChars(std::istringstream &in_stream, char *const content,
              const int content_length);

int SendUnicode(std::ostringstream &out_stream, const std::string &content);
int RecvUnicode(std::istringstream &in_stream, std::string &content);

int SendChar(phxrpc::BaseTcpStream &out_stream, const char &content);
int RecvChar(phxrpc::BaseTcpStream &in_stream, char &content);

int SendUint16(phxrpc::BaseTcpStream &out_stream, const uint16_t content);
int RecvUint16(phxrpc::BaseTcpStream &in_stream, uint16_t &content);

int SendChars(phxrpc::BaseTcpStream &out_stream, const char *const content,
              const int content_length);
int RecvChars(phxrpc::BaseTcpStream &in_stream, char *const content,
              const int content_length);

int SendUnicode(phxrpc::BaseTcpStream &out_stream, const std::string &content);
int RecvUnicode(phxrpc::BaseTcpStream &in_stream, std::string &content);


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

