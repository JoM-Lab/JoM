#!/usr/bin/env python3
import json
import requests
from getpass import getpass
from base64 import encodestring

auth = (input('username:').rstrip() + ':' + getpass('password:').rstrip()).encode('utf-8')

headers = {'authorization': 'x-td-basic ' + encodestring(auth).rstrip().decode('utf-8'),
           'X-TD-Authtype': 'twitter'}
data = requests.get('https://tweetdeck.twitter.com/login', headers=headers).json()
assert 'session' in data, "error:" + str(data)

headers = {'Authorization': 'X-TD-Session ' + data['session']}
data = requests.get('https://tweetdeck.twitter.com/accounts', headers=headers).json()
assert 'key' in data[0], "error:" + str(data)

json.dump(dict(
    ACCESS_SECRET=data[0]['secret'],
    ACCESS_KEY=data[0]['key'],
    CONSUMER_KEY="yT577ApRtZw51q4NPMPPOQ",
    CONSUMER_SECRET="3neq3XqN5fO3obqwZoajavGFCUrC42ZfbrLXy5sCv8"),
    open('secret.json', 'w'), indent=2)
