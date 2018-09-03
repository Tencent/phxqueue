# MqttBroker

Run MQTT Broker

```sh
# [Server]
# BindIP = 127.0.0.1
# Port = 9100
# [EventLoopServer]
# BindIP = 172.17.0.2  # container ip
# Port = 1883
bin/mqttbroker_main -c etc/mqttbroker_server.conf
# or
phxqueue_phxrpc/app/mqttbroker/mqttbroker_main -c etc/mqttbroker_server.conf
```

Use mqtt-spy to connect 127.0.0.1:1883.

## Unit Test

### Connect

```sh
bin/lock_tool_main -c phxqueue_phxrpc/app/lock/lock_client.conf -f GetString -t 1000 -l 1 -k "__broker__:client2session:mqtt-spy"
```

### HttpPublish

Send publish from MQTT Broker

```sh
# IP = 127.0.0.1
# Port = 9100
cd phxqueue_phxrpc/app/mqttbroker/
./mqttbroker_tool_main -c mqttbroker_http_client.conf -f HttpPublish -q 1 -t "/mqtt-spy/test/" -p 37 -s "test_string_37" -x "test_pub_client" -y "mqtt-spy"
```

Send publish from tools

```sh
# IP = 172.17.0.2  # container ip
# Port = 1883
cd phxqueue_phxrpc/app/mqttbroker/
./mqttbroker_tool_main -c mqttbroker_client.conf -f MqttPublish -q 1 -t "/mqtt-tools/test/" -p 121 -s test_string_121 -l mqtt-tools
```

