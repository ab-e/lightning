import json
import requests
import base64

xml_file = '../../../raw_xml/Wed Feb 22 02:57:47 UTC 2017.xml'
sensor = '291EC6DA-43F8-46D3-86F5-628FBF955261'
feature_of_interest = 'http://vocab.nerc.ac.uk/collection/L04/current/L04001/'
observed_property = 'http://vocab.nerc.ac.uk/collection/P02/current/TEMP/'
envri_publisher01 = 'f50b7c315eefb3fe7fde05c00751be0115e685a6'

with open(xml_file, 'r') as f:
    xml_file_data = f.read()

attributes_json = {}
attributes_json['madeBySensor'] = sensor
attributes_json['hasFeatureOfInterest'] = feature_of_interest
attributes_json['observedProperty'] = observed_property

message_data = base64.b64encode(bytes(xml_file_data, 'utf-8'))

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

