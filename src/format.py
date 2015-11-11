# coding: utf-8
import json
from html import unescape
from datetime import datetime
from tabulate import tabulate
import pytz
from pytz import timezone, utc
import opencc


class Formatter:
    """Format strings, pretty-printer, etc"""

    def __init__(self, config):
        self.config = config

    @staticmethod
    def convert_time(timestamp, tz):
        """Convert time

        :param int timestamp: Unix timestamp
        :param tz: timezone info
        :type tz: string | timezone object
        :return: formatted string of time information
        :rtype: string
        """
        if isinstance(tz, str):
            tz = timezone(tz)
        dt = datetime.fromtimestamp(timestamp, tz)
        s = dt.strftime(('%Y/' if dt.year != datetime.now().year else '') +
                        '%m/%d %H:%M:%S')
        tz_offset = dt.strftime('%z')[:3]
        if tz_offset != '+00':
            s += '(' + tz_offset + ')'
        return s

    def format_config(self):
        """Format config. Used by /help

        :return: formatted string of config.
        :rtype: string
        """
        l = []
        for name in self.config:
            d = self.config[name]
            l.append([str(name), str(d["shortcuts"]), str(d["timezone"]), str(d["interval"])])
        return tabulate(l, headers=["Name", "Shortcuts", "Timezone", "Interval"])

    def format_rows(self, rows, *, with_index=False):
        """Format a list of tweets

        :param rows: a list of tweets
        :type rows: List[json|dict]
        :param bool with_index: boolean flag deciding if index is attached
        :return: formatted string
        :rtype: string
        """
        if with_index:
            return '\n\n'.join([self.format1(r, i) for i, r in enumerate(rows)])
        else:
            return '\n\n'.join(map(self.format1, rows))

    def format_search(self, rows, cnt):
        """Format results of search

        :param rows: a list of tweet
        :type rows: List[json|dict]
        :param int cnt: number of remaining matched tweets
        :return: formatted string
        :rtype: string
        """
        if not rows:
            return 'no results'
        cnt_msg = '\n\n' + '-' * 25 + '\n{} more tweets'.format(cnt)
        return self.format_rows(rows, with_index=True) + (cnt_msg if cnt else '')

    def format_rand(self, r):
        """Format result of /rand

        :param r: a tweet
        :type r: dict | json
        :return : formatted string
        :rtype: string
        """
        return self.format1(r)

    @staticmethod
    def format_count(senders, y, n, count):
        """Format result of /count

        :param senders: list of names of all senders
        :type senders: List[str]
        :param y: list of desired keywords
        :type y: List[str]
        :param n: list of excluded keywords
        :type n: List[str]
        :param int count: number of matched tweets
        :return: formatted string
        :rtype: string
        """
        msg = "Number of tweets by {} with words {}\n".format(
            ', '.join(senders), ', '.join(y))
        if n:
            msg += "but without words {}\n".format(', '.join(n))
        msg += "\n-> {}\n".format(count)
        return msg

    @staticmethod
    def E(chars):
        """Generates a function that escapes certain characters in string

        :param chars: list of characters to escape
        :type chars: List[char]
        :return: function that escapes a string
        :rtype: string
        """
        def func(s):
            """Escaping a given string

            :param string s: string needs to be escaped
            :return: escaped string
            :rtype: string
            """
            for c in chars:
                s = s.replace(c, '\\' + c)
            return s
        return func

    def format_tweet(self, tweet):
        """Format a single tweet

        :param dict tweet: a tweet object
        :return: formatted string of a tweet
        :rtype: string
        """
        rep = {}
        entities = tweet.get('entities', {})
        for u in entities.get('urls', []) + entities.get('media', []):
            idx = tuple(u['indices'])
            rep.setdefault(idx, []).append('[{}]({})'.format(
                u['display_url'], u.get('media_url', u['expanded_url'])))
        for u in entities.get('user_mentions', []):
            idx = tuple(u['indices'])
            rep.setdefault(idx, []).append('[{}]({})'.format(
                '@'+u['screen_name'], 'twitter.com/' + u['screen_name']))
        for u in entities.get('hashtags', []):
            idx = tuple(u['indices'])
            rep.setdefault(idx, []).append('[{}]({})'.format('#'+u['text'],
                'twitter.com/hashtag/{}?src=hash'.format(u['text'])))
        for u in entities.get('symbols', []):
            idx = tuple(u['indices'])
            rep.setdefault(idx, []).append('[{}]({})'.format('$'+u['text'],
                'twitter.com/search?q=${}&src=ctag'.format(u['text'])))

        text = list(opencc.convert(tweet['text']))
        last = len(text)
        for idx in sorted(rep.keys(), reverse=True):
            st, ed = idx
            if ed < last:  # escape other parts
                text[ed:last] = self.E('_*[')(unescape(''.join(text[ed:last])))
            text[st:ed] = ' '.join(rep[idx])
            last = st
        text[0:last] = self.E('_*[')(unescape(''.join(text[0:last])))
        return ''.join(text)

    def format1(self, row, index=None):
        """Format a tweet

        :param row: a tweet
        :type row: dict|json
        :param int index: index
        :return: formatted string of tweet
        :rtype: string
        """
        if isinstance(row, dict):
            tweet = row
            sender = tweet['user']['screen_name'].lower()
            tweet_type = 'rt' if 'retweeted_status' in tweet else\
                         'reply' if tweet['in_reply_to_status_id'] else\
                         'quote' if tweet['is_quote_status'] else 'tweet'
            ts = int(datetime.strptime(row['created_at'], '%a %b %d %X %z %Y').timestamp())
            if sender in self.config:
                time_zone = self.config[sender]['timezone']
            else:
                time_zone = pytz.FixedOffset((tweet['user']['utc_offset'] or 0)//60)
        else:
            tweet = json.loads(row.tweet)
            sender, tweet_type, ts = row.sender, row.type, row.timestamp
            time_zone = self.config[sender]['timezone']

        url = 'twitter.com/{}/status/{}'.format(sender, tweet['id_str'])
        if tweet_type == 'reply':
            to_url = 'twitter.com/{}/status/{}'.format(
                    tweet['in_reply_to_screen_name'],
                    tweet['in_reply_to_status_id_str'])
            ty = '[replies]({}) to [tweet]({})'.format(url, to_url)
            text = self.format_tweet(tweet)
        elif tweet_type == 'rt':
            rt_name = tweet['retweeted_status']['user']['screen_name']
            rt_url = 'twitter.com/{}/status/{}'.format(rt_name,
                    tweet['retweeted_status']['id_str'])
            ty = 'retweets [{}]({})'.format(rt_name, rt_url)
            text = self.format_tweet(tweet['retweeted_status'])
        else:
            ty = '[{}s]({})'.format(tweet_type, url)
            text = self.format_tweet(tweet)

        msg = '{index}{time}, {sender} {ty}:\n{text}'.format(
            index='' if index is None else chr(9312+index)+' ',
            time=self.convert_time(ts, time_zone),
            sender=self.E('_')(sender), ty=ty, text=text)
        return msg

    def format_stat(self, user_name, time_limit, details):
        """Format statistics of a user

        :param string user_name: username of type string
        :param int time_limit: time range
        :param details: various details
        :type details: List[int]
        :return: formatted string of statistics
        :rtype: string
        """
        msg = "{}'s statistic during past {}:\n".format(user_name, time_limit)
        tz = self.config[user_name]['timezone']
        details[6] = details[6] and self.convert_time(details[6], tz)
        msg += ("  Tweet: {}\n  Reply: {}\n  RT: {}\n"
                "  Quote: {}\n  Total: {}\n  Indexed: {}\n"
                "  Since: {}\n").format(*details)
        return msg

    @staticmethod
    def format_quote(results, cnt):
        """Format quote

        :param results: list of tweets
        :type results: List[json|dict]
        :param int cnt: remaining tweets
        :return: formatted string of tweets
        :rtype: string
        """
        res = ''
        for i, r in enumerate(results):
            res += '{}. “{}” —— {}\n'.format(i+1, r.text, r.person)
        if not res:
            return "no quations"
        if cnt:
            res += '-' * 25 + '\n{} more'.format(cnt)
        return res

    @staticmethod
    def format_username_simple(username):
        """Format user name to twitter profile page link

        :param string username: user name
        :return: formatted string in profile page URL
        :rtype: string
        """
        return '[{}]({})'.format(username, 'twitter.com/'+username)

    def format_ff(self, sender, time_limit, unfo, fo, unfoed, foed):
        """Format follower/following change

        :param string sender: name of sender of type string
        :param string time_limit: time range, e.g 1d, 7d, 2w
        :param List[str] unfo: people who are unfoed by sender
        :param List[str] fo: people who are foed by sender
        :param List[str] unfoed: people who unfoed sender
        :param List[str] foed: people who foed sender
        :return: formatting string
        :rtype: string
        """
        f = self.format_username_simple
        following = ', '.join(['+'+f(u.target_name) for u in fo] +
                              ['-'+f(u.target_name) for u in unfo])
        follower  = ', '.join(['+'+f(u.target_name) for u in foed] +
                              ['-'+f(u.target_name) for u in unfoed])
        return ("{sender}'s following/follower status during past {time_limit}:\n"
                "following: {following} \nfollower: {follower}\n").format(
                    sender=self.E('_')(sender), time_limit=time_limit,
                    following=following, follower=follower)

    def format_fff(self, _sender, _time_limit, records, more):
        """Format detailed follower/following change

        :param string _sender: name of sender
        :param string _time_limit: time range, e.g 1d, 7d, 2w
        :param List[Follow] records: instance of class Follow
        :param int more: remaining follower/following changes
        :return: formatted string
        :rtype: string
        """
        f = self.format_username_simple
        res = []
        action = {'unfo': 'unfo', 'fo': 'fo',
                  'unfoed': 'unfoed by', 'foed': 'foed by'}
        if not records:
            return 'No results in the given period'
        for r in records[:30]:
            time = datetime.fromtimestamp(r.timestamp, utc)
            res.append('{} {} {}'.format(time.strftime('%m/%d %H:%M'),
                                         action[r.action], f(r.target_name)))
        if more:
            res.append('\\_' * 25 + '\n{} more...'.format(more))
        return '\n'.join(res)

    def format_kw_notif(self, kws, tweet):
        """ Format monitored tweet

        :param List[str] kws: list of monitored keywords
        :param dict|json tweet: tweet
        :return: formatted string
        :rtype: string
        """
        return self.format1(tweet) +\
                '\n\ncontaining monitored pattern: ' + self.E('_*[')(", ".join(kws))

    @staticmethod
    def format_keywords(sender, keywords):
        """Format monitored keywords

        :param string sender: name of sender
        :param List[str] keywords: list of monitored keywords
        :return: formatted string
        :rtype: string
        """
        return 'keywords for {}:\n{}'.format(
            sender, ', '.join(' AND '.join(l) for l in keywords))

    @staticmethod
    def format_trend_x_label(t1, t2):
        """Format x-axis date label

        :param int t1: timestamp of start date
        :param int t2: timestamp of end date
        :return: formatted string
        :rtype: string
        """
        s1 = datetime.fromtimestamp(t1).strftime('%m/%d')
        s2 = datetime.fromtimestamp(t2).strftime('%m/%d')
        return "{}-{}".format(s1, s2)

    def format_thread(self, tweets):
        """Format thread

        :param List[dict|json] tweets: list of tweets in thread
        :return: formatted string
        :rtype: string
        """
        return 'Thread:\n\n' + self.format_rows(tweets)
