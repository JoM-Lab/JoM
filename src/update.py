#!/usr/bin/env python3
# coding: utf-8
import sys
import json
import gevent
import opencc
from gevent import Greenlet
from datetime import datetime
from rauth import OAuth1Session
from db import Tweet, Follow, new_session
from format import Formatter
from sqlalchemy import func
import time
import os
import re
import socket

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 1)

globals().update(json.load(open('secret.json')))

twitter = OAuth1Session(consumer_key=CONSUMER_KEY,
                        consumer_secret=CONSUMER_SECRET,
                        access_token=ACCESS_KEY,
                        access_token_secret=ACCESS_SECRET)

params = dict(screen_name='', count=200,
              exclude_replies=False, include_rts=True)

fparams = dict(screen_name='', count=200, stringify_ids=True,
               include_user_entities=True, cursor=-1)

formatter = None
follow_dict = {}
keywords = json.load(open('keywords.json'))
notify_sock = None


def notify(msg):
    global notify_sock
    if notify_sock is None:
        try:
            notify_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            notify_sock.connect("/tmp/jom")
        except:
            notify_sock = None
            print("[socket error]: cannot send notification")
            return False
    notify_sock.send(msg.encode("utf8"))
    return True


def save_follow_dict():
    json.dump(follow_dict, open('follow.json', 'w'))
    # print('saved follow.json')


def time_to_stamp(time):
    ''' "Fri Aug 29 12:23:59 +0000 2014" '''
    return int(datetime.strptime(time, '%a %b %d %X %z %Y').timestamp())


def to_record(tweet):
    if 'retweeted_status' in tweet:
        typ = 'rt'
    elif tweet['in_reply_to_status_id']:
        typ = 'reply'
    elif tweet['is_quote_status']:
        typ = 'quote'
    else:
        typ = 'tweet'
    timestamp = time_to_stamp(tweet['created_at'])
    entities = tweet.get('entities', {})
    urls = entities.get('urls', [])
    media = entities.get('media', [])

    rep = {}
    for u in urls + media:
        idx = tuple(u['indices'])
        if idx not in rep:
            rep[idx] = []
        rep[idx].append(u.get('media_url', u['expanded_url']))

    text = list(opencc.convert(tweet['text']))
    for idx in sorted(rep.keys(), reverse=True):
        st, ed = idx
        text[st:ed] = ' '.join(rep[idx])
    text = ''.join(text)

    t = Tweet(id=int(tweet['id']), sender=tweet['user']['screen_name'].lower(),
              type=typ, timestamp=int(timestamp), tweet=json.dumps(tweet),
              text=text)
    return t


def f_record(user, action, target_id, target_name, timestamp):
    f = Follow(timestamp=int(timestamp), person=user,
               target_id=int(target_id), target_name=target_name,
               action=action)
    return f


def get_tweets(user, max_id=None):
    p = params.copy()
    p['screen_name'] = user
    if max_id is not None:
        p['max_id'] = max_id
    r = twitter.get('https://api.twitter.com/1.1/statuses/user_timeline.json',
                    params=p)
    return r.json()


def get_f(user, ftype):
    p = fparams.copy()
    p['screen_name'] = user
    f = []
    if ftype == 'follower':
        resource_uri = 'https://api.twitter.com/1.1/followers/list.json'
    elif ftype == 'following':
        resource_uri = 'https://api.twitter.com/1.1/friends/list.json'
    else:
        raise Exception('Unknown type: ' + ftype)
    while True:
        j = twitter.get(resource_uri, params=p).json()
        if 'errors' in j:
            raise Exception(j['errors'])
        f.extend([(str(u['id']), u['screen_name']) for u in j['users']])
        if j['next_cursor'] != 0:
            p['cursor'] = j['next_cursor']
        else:
            break
    return dict(f)


class FMonitor(Greenlet):

    def __init__(self, user, timelimit, session):
        Greenlet.__init__(self)
        self.user, self.timelimit = user, timelimit
        self.session = session
        self.follow = follow_dict[self.user]

    def diff(self, following, follower):
        last_following = self.follow['following']
        last_follower = self.follow['follower']
        unfo_ids = last_following.keys() - following.keys()
        fo_ids = following.keys() - last_following.keys()
        unfoed_ids = last_follower.keys() - follower.keys()
        foed_ids = follower.keys() - last_follower.keys()
        self.follow['following'] = following
        self.follow['follower'] = follower
        save_follow_dict()
        print("{}    unfo:{} fo:{} unfoed:{} foed:{}".format(datetime.now(),
              *map(len, [unfo_ids, fo_ids, unfoed_ids, foed_ids])))
        now = time.time()
        for uid in unfo_ids:
            self.session.add(f_record(self.user, 'unfo', uid,
                                      last_following[uid], now))
        for uid in fo_ids:
            self.session.add(f_record(self.user, 'fo', uid,
                                      following[uid], now))
        for uid in unfoed_ids:
            self.session.add(f_record(self.user, 'unfoed', uid,
                                      last_follower[uid], now))
        for uid in foed_ids:
            self.session.add(f_record(self.user, 'foed', uid,
                                      follower[uid], now))
        self.session.commit()

    def _run(self):
        while True:
            try:
                following = get_f(self.user, 'following')
                follower = get_f(self.user, 'follower')
                print("{} Updating {}'s following/follower status".
                      format(datetime.now(), self.user))
                self.diff(following=following, follower=follower)
            except Exception as e:
                print("Error in updating {}'s following/follower"
                      " status': {}".format(self.user, e.args))
            gevent.sleep(self.timelimit)

class TMonitor(Greenlet):

    def __init__(self, user, timelimit, session):
        Greenlet.__init__(self)
        self.user, self.timelimit = user, timelimit
        self.session = session

    def get_max_id(self):
        max_id = self.session.query(func.max(Tweet.id))\
                             .filter(Tweet.sender == self.user).first()[0]
        return max_id or 0

    @staticmethod
    def match_tweet(patterns, text):
        for i, pats in enumerate(patterns):
            matches = [re.search(pat, text) for pat in pats]
            if matches and all(matches):
                return True, i
        return False, -1

    def update(self):
        global formatter, keywords

        maxid = self.get_max_id()
        all_tweets = []
        tweets = get_tweets(self.user)
        if type(tweets) is dict and 'error' in tweets:
            raise Exception(tweets['error'])
        all_tweets.extend([t for t in tweets if t['id'] > maxid])
        while tweets and tweets[-1]['id'] > maxid:
            tweets = get_tweets(self.user, max_id=tweets[-1]['id'] - 1)
            all_tweets.extend([t for t in tweets if t['id'] > maxid])

        kws = keywords.get(self.user, [])
        patterns = [[re.compile(i) for i in l] for l in kws]
        for t in all_tweets:
            record = to_record(t)
            tweet_text = record.text
            match, index = self.match_tweet(patterns, tweet_text)
            if record.type != 'quote' and record.type != 'rt' and match:
                msg = formatter.format_kw_notif(kws[index], record)
                notify(msg)
            self.session.add(record)
        self.session.commit()

        print('{} Updated {}\'s {} tweets'.format(
                           datetime.now(), self.user, len(all_tweets)))

    def _run(self):
        while True:
            try:
                self.update()
            except Exception as e:
                print('Error in updating {}: {}'.format(self.user, e.args))
            gevent.sleep(self.timelimit)


class KeywordsWatcher(Greenlet):
    def __init(self):
        Greenlet.__init(self)

    def _run(self):
        global keywords
        last = os.path.getmtime("keywords.json")
        while True:
            try:
                cur = os.path.getmtime("keywords.json")
                if cur > last:
                    keywords.clear()
                    keywords.update(json.load(open("keywords.json")))
                    last = cur
                    print('{} Reloaded keywords.json'.format(datetime.now()))
            except Exception as e:
                print('Error in updating keywords.json: {}'.format(e.args))
            gevent.sleep(10)


class Updater:

    def __init__(self, config='config.json'):
        global formatter
        self.config = json.load(open(config))
        formatter = Formatter(self.config['victims'])
        self.session = new_session(self.config['db_file'])

    def run(self):
        global follow_dict
        tasks = []
        r = twitter.get('https://api.twitter.com/1.1/application/rate_limit_status.json')
        limit = r.json()['resources']['statuses']['/statuses/home_timeline']['limit']
        used = 0

        if os.path.exists('follow.json'):
            try:
                follow_dict = json.load(open('follow.json'))
            except ValueError:
                follow_dict = {}
            print("loaded follow.json")

        for u in self.config['victims']:
            t_itv = self.config['victims'][u]['interval']
            if t_itv:
                used += 15 * 60 / t_itv
                g = TMonitor(u, t_itv, self.session)
                tasks.append(g)
                g.start()

            f_itv = self.config['victims'][u]['f_interval']
            if f_itv:
                if u not in follow_dict:
                    print("initing {}'s following/follower status".format(u))
                    follow_dict[u] = dict(following=get_f(u, 'following'),
                                          follower=get_f(u, 'follower'))
                    print("initilized {}'s following/follower status".format(u))
                g = FMonitor(u, f_itv, self.session)
                tasks.append(g)
                g.start()

        print('API usage: {:.1f} / {}'.format(used, limit))
        kwdog = KeywordsWatcher()
        tasks.append(kwdog)
        kwdog.start()
        gevent.joinall(tasks)

if __name__ == '__main__':
    Updater().run()
