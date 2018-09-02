# MqttBroker

Run MQTT Broker

```sh
bin/mqttbroker_main -c etc/mqttbroker_server.conf
```

Use mqtt-spy to connect.

## Unit Test

### Connect

```sh
lock_tool_main -c lock_client.conf -f GetString -t 1000 -l 1 -k "__broker__:client2session:mqtt-spy"
```

### HttpPublish

Send publish from MQTT Broker

```sh
mqttbroker_tool_main -c mqttbroker_http_client.conf -f HttpPublish -q 1 -t "/mqtt-spy/test/" -p 37 -s "test_string_22" -x "test_pub_client" -y "mqtt-spy"
```

Send publish from tools

```sh
mqttbroker_tool_main -c mqttbroker_client.conf -f MqttPublish -q 1 -t "/mqtt-tools/test/" -p 121 -s test_string_27 -l mqtt-tools
```

