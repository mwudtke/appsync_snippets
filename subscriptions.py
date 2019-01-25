# an example of registering and subscribing to AppSync subscriptions using Python 3.7

import requests
import paho.mqtt.client as mqtt
from urllib.parse import urlparse

# the AppSync graphql endpoint for your API
apiurl = 'https://XXXXXXXXXXXX.appsync-api.us-east-1.amazonaws.com/graphql'

# replace X-Api-Key with a valid API key
postHeaders = {
    'Content-Type': 'application/json',
    'X-Api-Key': 'XXX-XXXXXXXXXXXXXXXXXXXXXXXXXX'
}

# a valid subscription type from your schema
payload = {"query": "subscription {\n  onCreatePost {\n    id\n    title\n    __typename\n  }\n}\n"}

# make the subscription request to the server and extract the presigned URL and topic information
r = requests.post(apiurl, headers=postHeaders, json=payload)

# grab the necessary items out of the "extentions" object returned by the POST request
client_id = r.json()['extensions']['subscription']['mqttConnections'][0]['client']
ws_url = r.json()['extensions']['subscription']['mqttConnections'][0]['url']
topic = r.json()['extensions']['subscription']['mqttConnections'][0]['topics'][0]

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(topic)

# parse the websockets presigned url
urlparts = urlparse(ws_url)

headers = {
    "Host": "{0:s}".format(urlparts.netloc),
}

client = mqtt.Client(client_id=client_id, transport="websockets")
client.on_connect = on_connect
client.on_message = on_message

client.ws_set_options(path="{}?{}".format(urlparts.path, urlparts.query), headers=headers)
client.tls_set()

print("trying to connect now....")
client.connect(urlparts.netloc, 443)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()

