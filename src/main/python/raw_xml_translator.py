import xml.etree.ElementTree
import datetime
import json
import requests
import base64
import sys

raw_atomic_observations_dir = '../../../raw_atomic_observations'
envri_consumer01 = '99e7201ccbb29630266fb9fb3911c20055ac5d6e'
envri_publisher01 = 'f50b7c315eefb3fe7fde05c00751be0115e685a6'

r = requests.post('https://messaging-devel.argo.grnet.gr/v1/projects/ENVRI/subscriptions/envri_sub_101:pull?key={}'.format(envri_consumer01), data='{"maxMessages": "1"}')

messages_json = r.json()

if not messages_json['receivedMessages']:
    print('No messages')
    sys.exit()

ack_id = messages_json['receivedMessages'][0]['ackId']

ack_ids_json = {}
ack_ids_json['ackIds'] = []
ack_ids_json['ackIds'].append(ack_id)

r = requests.post('https://messaging-devel.argo.grnet.gr/v1/projects/ENVRI/subscriptions/envri_sub_101:acknowledge?key={}'.format(envri_consumer01), data=json.dumps(ack_ids_json))

message_json = messages_json['receivedMessages'][0]['message']
message_attributes_json = message_json['attributes']
message_data = message_json['data']

sensor = message_attributes_json['madeBySensor']
feature_of_interest = message_attributes_json['hasFeatureOfInterest']
observed_property = message_attributes_json['observedProperty']

message_data_decoded = base64.b64decode(message_data).decode('utf-8', 'ignore')

e = xml.etree.ElementTree.fromstring(message_data_decoded)

for result_value in e.findall('{http://www.opengis.net/sos/2.0}resultValues'):
    result_value_text = result_value.text.strip()  # strip is necessary to remove the leading newline
    result_value_array = result_value_text.split('#')
    result_value_time = result_value_array[0]
    result_value_temperature = result_value_array[3]

result_time = datetime.datetime.strptime(result_value_time, "%d-%m-%y_%H:%M:%S")
result_time_iso = result_time.isoformat()

result_json = {}
result_json['numericValue'] = result_value_temperature

observation_json = {}

observation_json['madeBySensor'] = sensor
observation_json['hasFeatureOfInterest'] = feature_of_interest
observation_json['observedProperty'] = observed_property
observation_json['resultTime'] = result_time_iso
observation_json['hasResult'] = result_json

observation_json_data = json.dumps(observation_json)

attributes_json = {}
attributes_json['madeBySensor'] = sensor
attributes_json['hasFeatureOfInterest'] = feature_of_interest
attributes_json['observedProperty'] = observed_property

with open('{}/{}.json'.format(raw_atomic_observations_dir, result_time_iso), 'w') as f:
    json.dump(observation_json, f, indent=4)

url = 'https://messaging-devel.argo.grnet.gr/v1/projects/ENVRI/topics/envri_topic_101:publish?key={}'.format(envri_publisher01)

attributes_json = {}
attributes_json['madeBySensor'] = sensor
attributes_json['hasFeatureOfInterest'] = feature_of_interest
attributes_json['observedProperty'] = observed_property

message_data = base64.b64encode(bytes(json.dumps(observation_json), 'utf-8'))

message_json = {}
message_json['attributes'] = attributes_json
message_json['data'] = message_data.decode('utf-8')

messages_array_json = []
messages_array_json.append(message_json)

data_json = {}
data_json['messages'] = messages_array_json

url = 'https://messaging-devel.argo.grnet.gr/v1/projects/ENVRI/topics/envri_topic_101:publish?key={}'.format(envri_publisher01)

r = requests.post(url, data=json.dumps(data_json))

print('Request {} {}'.format(r.text, r.status_code))
