# Sample

## Unit Test

### HttpPublish

1. Run MQTT Broker

	```sh
	./mqttbroker_main -c mqttbroker_server.conf
	```

2. Connect MQTT Client

	Use mqtt-spy to connect.

3. Send publish from MQTT Broker

	```sh
	./mqttbroker_tool_main -c mqttbroker_http_client.conf -f HttpPublish -q 1 -t "/mqtt-spy/test/" -p 37 -s "test_string_22" -x "test_pub_client" -y "mqtt-spy"
	```

4. Send publish from tools

	```sh
	./mqttbroker_tool_main -c mqttbroker_client.conf -f MqttPublish -q 1 -t "/mqtt-tools/test/" -p 121 -s test_string_27 -l mqtt-tools
	```

