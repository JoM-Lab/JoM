#!/usr/bin/env python3
# coding: utf-8
import sys
import json
import gevent
import opencc
import random
from gevent import Greenlet
from datetime import datetime
from .db import Tweet, Follow, Bio, new_session, check_deleted
from .format import Formatter
from .twitter import get_ratelimit, get_tweets, get_f, get_user_info
from sqlalchemy import func
import time
import os
import re
import socket
import collections


formatter = None
follow_dict = {}
keywords = {}


def notify(msg):
    """Send message through socket. It is used to communicate with polling

    :param str msg: message to send through socket
    :return: if message is sent successfully
    :rtype: bool
    """
    try:
        notify_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        notify_sock.connect("/tmp/jom")
        notify_sock.send(msg.encode("utf8"))
        notify_sock.close()
        return True
    except:
        print("[socket error]: cannot send notification")
        return False


def save_follow_dict():
    """Save following/follower status to a local JSON file
    """
    json.dump(follow_dict, open('follow.json', 'w'))


def parse_time(time):
    """convert time string to datatime object

    :param str time: time representation
    :return: datetime.datetime
    :rtype: int
    """

    ''' "Fri Aug 29 12:23:59 +0000 2014" '''
    return datetime.strptime(time, '%a %b %d %X %z %Y')


def to_record(tweet):
    """Convert a tweet of type dict to Tweet database instance

    :param dict tweet: a tweet
    :return: Tweet database instance
    :rtype: Tweet
    """
    if 'retweeted_status' in tweet:
        typ = 'rt'
    elif tweet['in_reply_to_status_id']:
        typ = 'reply'
    elif tweet['is_quote_status']:
        typ = 'quote'
    else:
        typ = 'tweet'
    timestamp = int(parse_time(tweet['created_at']).timestamp())
    text = opencc.convert(tweet['text'])
    t = Tweet(id=int(tweet['id']), user_id=tweet['user']['id'],
              type=typ, timestamp=int(timestamp), tweet=json.dumps(tweet),
              text=text)
    return t


def f_record(user_id, action, target_id, target_name, timestamp):
    """Convert to Follow database record

    :param str user_id: user id
    :param str action: fo/unfo/foed/unfoed
    :param str target_id: target id
    :param str target_name: target name
    :param int timestamp: when
    :return: Follow database instance
    :rtype: Follow
    """
    f = Follow(timestamp=int(timestamp), user_id=int(user_id),
               target_id=int(target_id), target_name=target_name,
               action=action)
    return f


class BioMonitor(Greenlet):
    ''' Bio changes monitor. '''

    def __init__(self, user_id, name, all_fields, timelimit, notify_fields, session):
        Greenlet.__init__(self)
        self.all_fields = all_fields
        self.user_id, self.timelimit = user_id, timelimit
        self.name = name
        self.session = session
        self.notify_fields = set(notify_fields)

    def update_bio(self):
        """ Return True if some fields in bio are changed and recorded in database.

        :rtype: True | False
        """
        last = self.session.query(Bio.bio)\
                           .filter(Bio.bio['id'] == str(self.user_id))\
                           .order_by(Bio.timestamp.desc()).first()
        current = get_user_info(self.user_id)

        if last is not None:
            last = last[0]
            changes = []
            for field in self.all_fields:
                last_field = last.get(field, None)
                curr_field = current.get(field, None)
                if last_field != curr_field:
                    changes.append((datetime.now().timestamp(), field, last_field, curr_field))
            if not changes:
                return False
            # notify bio changes
            to_notify = [c for c in changes if c[1] in self.notify_fields]
            if to_notify:
                msg = formatter.format_bio(self.user_id, 'now', to_notify)
                msg = msg.replace(' in now', '', 1)  # ugly hack formatting
                notify(json.dumps(dict(message=msg)))

        self.session.add(Bio(timestamp=datetime.now(), bio=current))
        self.session.commit()
        return True

    def _run(self):
        while True:
            try:
                print("{} Inspecting {}'s bio".format(datetime.now(), self.name))
                if self.update_bio():
                    print("{} Updated {}'s bio".format(datetime.now(), self.name))
            except Exception as e:
                print("Error in updating {}'s bio: {}".format(self.name, e.args))
            gevent.sleep(self.timelimit * (1 + 0.15 * random.random()))


class FMonitor(Greenlet):
    ''' Following/followers status monitor. '''

    def __init__(self, user_id, name, timelimit, session):
        Greenlet.__init__(self)
        self.user_id, self.timelimit = user_id, timelimit
        self.name = name
        self.session = session
        self.follow = follow_dict[self.user_id]

    def diff(self, following, follower):
        """Calculate following/follower change using basic
        set difference operation

        unfo = last_following - current_following (people unfoed by target)
        fo =   current_following - last_following (people foed by target)
        unfoed = last_follower - current_follower (people who unfoed target)
        foed = current_follower - last_follower (people who foed target)

        :param following: following id
        :param follower: follower id
        """
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
            self.session.add(f_record(self.user_id, 'unfo', uid,
                                      last_following[uid], now))
        for uid in fo_ids:
            self.session.add(f_record(self.user_id, 'fo', uid,
                                      following[uid], now))
        for uid in unfoed_ids:
            self.session.add(f_record(self.user_id, 'unfoed', uid,
                                      last_follower[uid], now))
        for uid in foed_ids:
            self.session.add(f_record(self.user_id, 'foed', uid,
                                      follower[uid], now))
        self.session.commit()

    def get_timelimit(self):
        """add random sleep seconds to time limit to avoid burst API usage
        We choose to add 0%-15% more seconds to time limit to original time limit

        :return: time limit plus randomly more seconds
        :rtype: float
        """
        return self.timelimit + 0.15 * self.timelimit * random.random()

    def _run(self):
        while True:
            try:
                following = get_f(self.user_id, 'following')
                follower = get_f(self.user_id, 'follower')
                print("{} Updating {}'s following/follower status".
                      format(datetime.now(), self.name))
                self.diff(following=following, follower=follower)
            except Exception as e:
                print("Error in updating {}'s following/follower"
                      " status': {}".format(self.name, e.args))
            gevent.sleep(self.get_timelimit())


class TMonitor(Greenlet):
    ''' Tweets monitor. '''

    def __init__(self, user_id, name, timelimit, session):
        Greenlet.__init__(self)
        self.user_id, self.timelimit = user_id, timelimit
        self.name = name
        self.session = session

    def get_max_id(self):
        """Get maximum tweet id from a list of tweets

        :return: maximum id
        :rtype: int
        """
        max_id = self.session.query(func.max(Tweet.id))\
                             .filter(Tweet.user_id == int(self.user_id)).first()
        return max_id[0] or 0  # 0 for target's first time

    @staticmethod
    def match_tweet(patterns, text):
        """match tweets using patterns

        A list of pattern has form ["a", "-b", "-c"], which
        means matching tweet text containing "a", but not "b" or "c"

        :param patterns: List of patterns
        :type patterns: List[List[str]]
        :param str text: text to be matched
        :return: boolean flag if match exists and the corresponding pattern
        :rtype: bool * List[str]
        """
        for pats in patterns:
            positive = [p for p in pats if p[0] != '-']
            negative = [p[1:] for p in pats if p[0] == '-']
            if all(re.search(p, text, re.I) for p in positive) and\
                    not any(re.search(p, text, re.I) for p in negative):
                return True, pats
        return False, None

    def update(self):
        """Update tweets
        """
        global formatter, keywords

        maxid = self.get_max_id()
        all_tweets = []
        tweets = get_tweets(self.user_id)
        if type(tweets) is dict and 'error' in tweets:
            raise Exception(tweets['error'])
        check_deleted(self.session, tweets)
        all_tweets.extend([t for t in tweets if t['id'] > maxid])
        while tweets and tweets[-1]['id'] > maxid:
            tweets = get_tweets(self.user_id, max_id=tweets[-1]['id'] - 1)
            all_tweets.extend([t for t in tweets if t['id'] > maxid])

        kws = keywords.get(self.user_id, [])
        for t in all_tweets:
            record = to_record(t)
            tweet_text = record.text
            match, pats = self.match_tweet(kws, tweet_text)
            # notify polling when tweets match certain group of keyword
            if record.type != 'quote' and record.type != 'rt' and match:
                msg = formatter.format_kw_notif(pats, record)
                notify(json.dumps(dict(message=msg, markdown=True)))
            self.session.add(record)
        self.session.commit()

        print('{} Updated {}\'s {} tweets'.format(
            datetime.now(), self.name, len(all_tweets)))

    def get_timelimit(self):
        """add random sleep seconds to time limit to avoid burst API usage
        We choose to add 0%-15% more seconds to time limit to original time limit

        :return: time limit plus randomly more seconds
        :rtype: float
        """
        return self.timelimit + 0.15 * self.timelimit * random.random()

    def _run(self):
        while True:
            try:
                self.update()
            except Exception as e:
                print('Error in updating {}: {}'.format(self.name, e.args))
            gevent.sleep(self.get_timelimit())


class KeywordsWatcher(Greenlet):
    """ Constantly update keywords from keywords.json. """

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
    """ Update tweets and following/follower of all targets. """

    def __init__(self):
        global formatter, keywords, follow_dict
        self.config = json.load(open('config.json'))
        if not os.path.exists('keywords.json'):
            open('keywords.json', 'w').write('{}')
        keywords = json.load(open('keywords.json'))
        if not os.path.exists('follow.json'):
            open('follow.json', 'w').write('{}')
        follow_dict = json.load(open('follow.json'))
        formatter = Formatter(self.config['victims'])
        self.session = new_session()

    def run(self):
        tasks = []
        r = get_ratelimit()
        limit = r['resources']['statuses']['/statuses/home_timeline']['limit']
        bio_all_fields = self.config['bio_all_fields']
        used = collections.defaultdict(float)

        for user_id, cfg in self.config['victims'].items():
            name = cfg['ref_screen_name']

            t_itv = cfg.get('interval', 0)
            if t_itv:
                used['tweet'] += 15 * 60 / t_itv
                g = TMonitor(user_id, name, t_itv, self.session)
                tasks.append(g)
                g.start()

            bio_itv = cfg.get('bio_interval', 0)
            bio_notify = cfg.get('bio_notify', [])
            if bio_itv:
                used['bio'] += 15 * 60 / bio_itv
                g = BioMonitor(user_id, name, bio_all_fields,
                               bio_itv, bio_notify, self.session)
                tasks.append(g)
                g.start()

            f_itv = cfg.get('f_interval', 0)
            if f_itv:
                # estimate count of api calls
                if user_id in follow_dict:
                    cnt_calls = sum(len(v)/200.0 for v in follow_dict[user_id].values())
                else:
                    cnt_calls = 2
                used['follow'] += 15 * 60 / f_itv * cnt_calls
                try:
                    if user_id not in follow_dict:
                        print("initing {}'s following/follower status".format(name))
                        follow_dict[user_id] = dict(following=get_f(user_id, 'following'),
                                                    follower=get_f(user_id, 'follower'))
                        print("initilized {}'s following/follower status".format(name))
                    g = FMonitor(user_id, name, f_itv, self.session)
                    tasks.append(g)
                    g.start()
                except:
                    print('ERROR:', name, 'not authorized')

        print('API usage:')
        for k, v in used.items():
            print('  {}: {:.1f} / {}'.format(k, v, limit))

        kwdog = KeywordsWatcher()
        tasks.append(kwdog)
        kwdog.start()

        gevent.joinall(tasks)

if __name__ == '__main__':
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 1)  # no buffering
    Updater().run()
