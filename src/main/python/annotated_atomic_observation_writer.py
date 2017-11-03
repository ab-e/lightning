import xml.etree.ElementTree
import datetime
import json
import requests
import base64
import sys

annotated_atomic_observations_dir = '../../../annotated_atomic_observations'
envri_consumer01 = '99e7201ccbb29630266fb9fb3911c20055ac5d6e'

r = requests.post('https://messaging-devel.argo.grnet.gr/v1/projects/ENVRI/subscriptions/envri_sub_101:pull?key={}'.format(envri_consumer01), data='{"maxMessages": "1"}')

messages_json = r.json()

if not messages_json['receivedMessages']:
    print('No messages')
    sys.exit()

message_json = messages_json['receivedMessages'][0]['message']
message_attributes_json = message_json['attributes']
message_type = message_attributes_json['type']

if message_type != 'AnnotatedAtomicObservation':
    sys.exit()

ack_id = messages_json['receivedMessages'][0]['ackId']

ack_ids_json = {}
ack_ids_json['ackIds'] = []
ack_ids_json['ackIds'].append(ack_id)

requests.post('https://messaging-devel.argo.grnet.gr/v1/projects/ENVRI/subscriptions/envri_sub_101:acknowledge?key={}'.format(envri_consumer01), data=json.dumps(ack_ids_json))

message_data = message_json['data']
message_data_decoded = base64.b64decode(message_data).decode('utf-8', 'ignore')
observation_json = json.loads(message_data_decoded)
result_value_time = observation_json['resultTime']

with open('{}/{}.json'.format(annotated_atomic_observations_dir, result_value_time), 'w') as f:
    json.dump(observation_json, f, indent=4)
