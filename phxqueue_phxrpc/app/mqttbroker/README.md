# MqttBroker

1. `ln -s ../../../etc` and `ln -s ../../../log`
2. Use mqtt-spy to connect.

```sh
lock_tool_main -c lock_client.conf -f GetString -t 1000 -l 1 -k "__broker__:client2session:mqtt-spy"
```

