# PUBLISHER

Replace the key `{envri_publisher01}` with the one received by email.

The following command sends the content of the file `message-example-1.json` to the messaging service. The file can be found in the project root folder. Note that the message itself needs to follow a [determined structure](http://argoeu.github.io/messaging/v1/publisher/).

The proposal here is to include the sensor, feature, and property as attributes of the message. The `data` attribute contains the actual message, e.g. an atomic observation or an `InsertResult`. It needs to be base64 encoded.

```
curl -d '@message-example-1.json' -H "Content-Type: application/json" -X POST https://messaging-devel.argo.grnet.gr/v1/projects/ENVRI/topics/envri_topic_101:publish?key={envri_publisher01}
```

# CONSUMER

Replace the key `{envri_consumer01}` with the one received by email.

With the following command, the consumer can pull messages (the maximum number of messages can be specified).

```
curl -d '{"maxMessages": "1"}' -H "Content-Type: application/json" -X POST https://messaging-devel.argo.grnet.gr/v1/projects/ENVRI/subscriptions/envri_sub_101:pull?key={envri_consumer01}
```

Finally, the consumer should acknowledge receiving the messages with the following command. The `ackIds` attribute can take one or more ids. The ids are included in the received messages (`ackId` attribute). Note that there is a timeout for acknowledgement. Messages that have been acknowledged are sent again in subsequent requests.

Replace the `ackId` in the posted message accordingly.

```
curl -d '{"ackIds": [ "projects/ENVRI/subscriptions/envri_sub_101:0" ]}' -H "Content-Type: application/json" -X POST https://messaging-devel.argo.grnet.gr/v1/projects/ENVRI/subscriptions/envri_sub_101:acknowledge?key={envri_consumer01}
```
