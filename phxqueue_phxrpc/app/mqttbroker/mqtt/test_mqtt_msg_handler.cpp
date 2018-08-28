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

#include <cassert>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>

#include "phxqueue_phxrpc/app/logic/mqtt.h"
#include "phxrpc/file/file_utils.h"
#include "phxrpc/file/opt_map.h"
#include "phxrpc/network/socket_stream_block.h"

#include "mqtt_msg.h"
#include "mqtt_msg_handler.h"


using namespace phxqueue_phxrpc::logic::mqtt;
using namespace phxqueue_phxrpc::mqttbroker;
using namespace std;


void ShowUsage(const char *program) {
    printf("\n%s [-r CONNECT|PUBLISH|SUBSCRIBE|UNSUBSCRIBE|PING|DISCONNECT] [-f file] [-v]\n", program);

    printf("\t-r mqtt method, CONNECT|PUBLISH|SUBSCRIBE|UNSUBSCRIBE|PING|DISCONNECT\n");
    printf("\t-f the file for content\n");
    printf("\t-v show this usage\n");
    printf("\n");

    exit(0);
}

void TraceMsg(const MqttMessage &msg) {
    ostringstream ss_req;
    msg.SendRemaining(ss_req);
    const string &s_req(ss_req.str());
    cout << s_req.size() << ":" << endl;
    for (int i{0}; s_req.size() > i; ++i) {
        cout << static_cast<int>(s_req.data()[i]) << "\t";
    }
    cout << endl;
    for (int i{0}; s_req.size() > i; ++i) {
        if (isalnum(s_req.data()[i]) || '_' ==  s_req.data()[i])
            cout << s_req.data()[i] << "\t";
        else
            cout << '.' << "\t";
    }
    cout << endl;
}

int main(int argc, char *argv[]) {
    assert(sigset(SIGPIPE, SIG_IGN) != SIG_ERR);

    phxrpc::OptMap opt_map("r:f:v");

    if ((!opt_map.Parse(argc, argv)) || opt_map.Has('v'))
        ShowUsage(argv[0]);

    const char *method{opt_map.Get('r')};
    const char *file{opt_map.Get('f')};

    if (nullptr == method) {
        printf("\nPlease specify method!\n");
        ShowUsage(argv[0]);
    }

    if (0 == strcasecmp(method, "CONNECT")) {
        cout << "Req:" << endl;
        MqttConnectPb connect_pb;
        connect_pb.set_client_identifier("test_client_1");
        MqttConnect connect;
        connect.FromPb(connect_pb);
        TraceMsg(connect);

        cout << "Resp:" << endl;
        TraceMsg(MqttConnack());
    } else if (0 == strcasecmp(method, "PUBLISH")) {
        cout << "Req:" << endl;
        MqttPublishPb publish_pb;
        publish_pb.set_topic_name("test_topic_1");
        publish_pb.set_data("test_msg_1");
        publish_pb.set_packet_identifier(11);
        MqttPublish publish;
        publish.FromPb(publish_pb);
        TraceMsg(publish);

        cout << "Resp:" << endl;
        MqttPubackPb puback_pb;
        puback_pb.set_packet_identifier(11);
        MqttPuback puback;
        puback.FromPb(puback_pb);
        TraceMsg(puback);
    } else if (0 == strcasecmp(method, "SUBSCRIBE")) {
        cout << "Req:" << endl;
        TraceMsg(MqttSubscribe());
        cout << "Resp:" << endl;
        TraceMsg(MqttSuback());
    } else if (0 == strcasecmp(method, "UNSUBSCRIBE")) {
        cout << "Req:" << endl;
        TraceMsg(MqttUnsubscribe());
        cout << "Resp:" << endl;
        TraceMsg(MqttUnsuback());
    } else if (0 == strcasecmp(method, "PING")) {
        cout << "Req:" << endl;
        TraceMsg(MqttPingreq());
        cout << "Resp:" << endl;
        TraceMsg(MqttPingresp());
    } else if (0 == strcasecmp(method, "DISCONNECT")) {
        cout << "Req:" << endl;
        TraceMsg(MqttDisconnect());
    } else {
        printf("unsupport method %s\n", method);
    }

    return 0;
}

