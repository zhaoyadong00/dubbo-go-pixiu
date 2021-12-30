
# 

## Development 

```shell
curl http://localhost:9127/hi
```
result
```shell
Hello Pixiu World! from org.springframework.cloud.zookeeper.serviceregistry.ServiceInstanceRegistration@63a45760
```

find service instance info:
```shell
curl http://localhost:9127/service/pixiu-springcloud-server
```

result
```shell
[
  {
    "serviceId": "pixiu-springcloud-server",
    "host": "127.0.0.1",
    "port": 9127,
    "secure": false,
    "uri": "http://127.0.0.1:9127",
    "metadata": {
      "instance_status": "UP"
    },
    "serviceInstance": {
      "name": "pixiu-springcloud-server",
      "id": "1a7b120a-e77a-4a8d-ad3b-ca75473288a4",
      "address": "127.0.0.1",
      "port": 9127,
      "sslPort": null,
      "payload": {
        "@class": "org.springframework.cloud.zookeeper.discovery.ZookeeperInstance",
        "id": "pixiu-springcloud-server",
        "name": "pixiu-springcloud-server",
        "metadata": {
          "instance_status": "UP"
        }
      },
      "registrationTimeUTC": 1640852269145,
      "serviceType": "DYNAMIC",
      "uriSpec": {
        "parts": [
          {
            "value": "scheme",
            "variable": true
          },
          {
            "value": "://",
            "variable": false
          },
          {
            "value": "address",
            "variable": true
          },
          {
            "value": ":",
            "variable": false
          },
          {
            "value": "port",
            "variable": true
          }
        ]
      },
      "enabled": true
    },
    "instanceId": "1a7b120a-e77a-4a8d-ad3b-ca75473288a4",
    "scheme": null
  }
]
```




