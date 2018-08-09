# Sample

## Unit Test

### HttpPublish

1. Run MQTT Broker

	```sh
	./search_main -c search_server.conf
	```

2. Connect MQTT Client

	Use mqtt-spy to connect.

3. Send publish from MQTT Broker

	```sh
	./search_tool_main -c search_http_client.conf -f PhxHttpPublish -q 1 -t "/mqtt-spy/test/" -p 37 -s "test_string_22" -x "test_pub_client" -y "mqtt-spy"
	```

4. Send publish from tools

	```sh
	./search_tool_main -c search_client.conf -f PhxMqttPublish -q 1 -t "/mqtt-tools/test/" -p 121 -s test_string_27 -l mqtt-tools
	```

