import json
from rauth import OAuth1Session

globals().update(json.load(open('secret.json')))

twitter = OAuth1Session(consumer_key=CONSUMER_KEY,
                        consumer_secret=CONSUMER_SECRET,
                        access_token=ACCESS_KEY,
                        access_token_secret=ACCESS_SECRET)

