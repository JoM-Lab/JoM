# coding: utf-8
import json
from html import unescape
from datetime import datetime
from tabulate import tabulate
import pytz
from pytz import timezone, utc
import opencc
from itertools import takewhile
from .resp import Resp


class Formatter:
    """ Format strings, pretty-printer, etc. """

    def __init__(self, config):
        self.config = config
        self.ref = {}
        for uid, u in config.items():
            self.ref[int(uid)] = self.ref[uid] = u['ref_screen_name']

    @staticmethod
    def convert_time(timestamp, tz):
        """Convert time.

        :param int timestamp: Unix timestamp
        :param tz: timezone info
        :type tz: string | timezone object
        :return: formatted string of time information
        :rtype: string
        """
        if isinstance(tz, str):
            tz = timezone(tz)
        dt = datetime.fromtimestamp(timestamp, tz)
        diff = dt.replace(tzinfo=None) - datetime.utcnow()
        s = dt.strftime(('%Y/' if diff.days > 365 else '') +
                        '%m/%d %H:%M:%S')
        tz_offset = dt.strftime('%z')[:3]
        if tz_offset != '+00':
            s += '(' + tz_offset + ')'
        return s

    def format_config(self):
        """Format config. Used by `/help`.

        :return: formatted string of config.
        :rtype: string
        """
        l = []
        for uid in self.config:
            d = self.config[uid]
            l.append([d["ref_screen_name"], d["shortcuts"],
                      d["timezone"], str(d["interval"]), str(d["f_interval"])])
        s = tabulate(l, headers=["Name", "Abbr.", "Timezone", "Intv.", "FIntv."])
        return '```\n{}\n```'.format(self.E('`')(s))

    def format_rows(self, rows, *, with_index=False):
        """Format a list of tweets.

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
        """Format results of search.

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
        """Format result of /rand.

        :param r: a tweet
        :type r: dict | json
        :return: formatted string
        :rtype: string
        """
        return self.format1(r)

    def format_count(self, senders, y, n, count):
        """Format result of /count.

        :param senders: list of ids of all senders
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
            ', '.join(self.ref[s] for s in senders), ', '.join(y))
        if n:
            msg += "but without words {}\n".format(', '.join(n))
        msg += "\n-> {}\n".format(count)
        return msg

    @staticmethod
    def E(chars):
        """Generates a function that escapes certain characters in string.

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
        """Format a single tweet.

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
                text[ed:last] = self.E('_*[`')(unescape(''.join(text[ed:last])))
            text[st:ed] = ' '.join(rep[idx])
            last = st
        text[0:last] = self.E('_*[`')(unescape(''.join(text[0:last])))
        return ''.join(text)

    def format1(self, row, index=None):
        """Format a tweet.

        :param row: a tweet
        :type row: dict|json
        :param int index: index
        :return: formatted string of tweet
        :rtype: string
        """
        if isinstance(row, dict):
            tweet = row
            name = tweet['user']['screen_name'].lower()
            tweet_type = 'rt' if 'retweeted_status' in tweet else\
                         'reply' if tweet['in_reply_to_status_id'] else\
                         'quote' if tweet['is_quote_status'] else 'tweet'
            ts = int(datetime.strptime(row['created_at'], '%a %b %d %X %z %Y').timestamp())
            time_zone = pytz.FixedOffset((tweet['user']['utc_offset'] or 0)//60)
        else:
            tweet = json.loads(row.tweet)
            uid, tweet_type, ts = str(row.user_id), row.type, row.timestamp
            name = self.ref[uid]
            time_zone = self.config[uid]['timezone']

        url = 'twitter.com/statuses/{}'.format(tweet['id_str'])
        if tweet_type == 'reply':
            to_url = 'twitter.com/statuses/' + tweet['in_reply_to_status_id_str']
            ty = '[replies]({}) to [tweet]({})'.format(url, to_url)
            text = self.format_tweet(tweet)
        elif tweet_type == 'rt':
            rt_name = tweet['retweeted_status']['user']['screen_name']
            rt_url = 'twitter.com/statuses/' + tweet['retweeted_status']['id_str']
            ty = '`retweets` [{}]({})'.format(rt_name, rt_url)
            text = self.format_tweet(tweet['retweeted_status'])
        elif tweet_type == 'quote' and 'quoted_status' in tweet:
            quote = tweet['quoted_status']
            quote_name = quote['user']['screen_name']
            quote_url = 'twitter.com/statuses/' + tweet['quoted_status']['id_str']
            ty = '[quotes]({}) [{}]({})'.format(url, quote_name, quote_url)
            main_text = tweet['text']
            # CR mark: this way of removing trailing link is quite shaky
            if main_text.split(' ')[-1].startswith('http'):
                main_text = main_text[:main_text.rfind(' ')]
            text = '{} \n\n `{}`'.format(self.E('_*[`')(unescape(main_text)),
                                         self.E('`')(quote['text']))
        else:
            ty = '[{}s]({})'.format(tweet_type, url)
            text = self.format_tweet(tweet)

        msg = '{index}{time}, {name} {ty}:\n{text}'.format(
            index='' if index is None else chr(9312+index)+' ',
            time=self.convert_time(ts, time_zone),
            name=self.E('_')(name), ty=ty, text=text)
        return msg

    def format_stat(self, uid, time_limit, details):
        """Format statistics of a user.

        :param str uid: user id
        :param int time_limit: time range
        :param details: various details
        :type details: List[int]
        :return: formatted string of statistics
        :rtype: string
        """
        msg = "{}'s statistic during past {}:\n".format(self.ref[uid], time_limit)
        tz = self.config[uid]['timezone']
        details[-1] = details[-1] and self.convert_time(details[-1], tz)
        msg += ("  Tweet: {}\n  Reply: {}\n  RT: {}\n"
                "  Quote: {}\n  Deleted: {}\n  Total: {}\n  Indexed: {}\n"
                "  Since: {}\n").format(*details)
        return msg

    def format_quote(self, results, cnt):
        """Format quote.

        :param results: list of tweets
        :type results: List[json|dict]
        :param int cnt: remaining tweets
        :return: formatted string of tweets
        :rtype: string
        """
        res = ''
        for i, r in enumerate(results):
            res += '{}. “{}” —— {}\n'.format(i+1, r.text, self.ref[r.user_id])
        if not res:
            return "no quations"
        if cnt:
            res += '-' * 25 + '\n{} more'.format(cnt)
        return res

    @staticmethod
    def format_username_simple(username):
        """Format user name to twitter profile page link.

        :param string username: user name
        :return: formatted string in profile page URL
        :rtype: string
        """
        return '[{}]({})'.format(username, 'twitter.com/'+username)

    def format_ff(self, sender, time_limit, unfo, fo, unfoed, foed):
        """Format follower/following change.

        :param string sender: user id of sender of type string
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
                    sender=self.E('_')(self.ref[sender]), time_limit=time_limit,
                    following=following, follower=follower)

    def format_f(self, _sender, _time_limit, records, more):
        """Format detailed follower/following change.

        :param string _sender: user id of sender
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

    def format_fs(self, targets, changed, records):
        f = self.format_username_simple
        res = []
        action = {'unfo': 'unfo', 'fo': 'fo',
                  'unfoed': 'unfoed by', 'foed': 'foed by'}
        if not records:
            return 'No results in the given period'
        for r in records:
            time = datetime.fromtimestamp(r.timestamp, utc)
            res.append('{} {} {} {}'.format(time.strftime('%m/%d %H:%M'),
                                    str(r.user_id), action[r.action], f(r.target_name)))
        return '\n'.join(res)

    def format_kw_notif(self, kws, tweet):
        """ Format monitored tweet.

        :param List[str] kws: list of monitored keywords
        :param dict|json tweet: tweet
        :return: formatted string
        :rtype: string
        """
        return self.format1(tweet) +\
            '\n\ncontaining monitored pattern: ' +\
            self.E('_*[`')(", ".join(kws))

    def format_keywords(self, sender, keywords):
        """Format monitored keywords.

        :param string sender: user id of sender
        :param List[str] keywords: list of monitored keywords
        :return: formatted string
        :rtype: string
        """
        return 'keywords for {}:\n{}'.format(
            self.ref[sender], ', '.join(' AND '.join(l) for l in keywords))

    @staticmethod
    def format_trend_x_label(t1, t2):
        """Format x-axis date label.

        :param int t1: timestamp of start date
        :param int t2: timestamp of end date
        :return: formatted string
        :rtype: string
        """
        s1 = datetime.fromtimestamp(t1).strftime('%m/%d')
        s2 = datetime.fromtimestamp(t2).strftime('%m/%d')
        return "{}-{}".format(s1, s2)

    def format_thread(self, tweets):
        """Format thread.

        :param List[dict|json] tweets: list of tweets in thread
        :return: formatted string
        :rtype: string
        """
        return 'Thread:\n\n' + self.format_rows(tweets)

    @staticmethod
    def format_usage(docstring):
        """Format usage explanation

        :param str docstring: raw docstring of a function
        :return: formatted explanation
        :rtype: str
        """
        ls = [l.lstrip() for l in docstring.split('\n') if l]
        ls = list(takewhile(lambda s: not s.startswith(':'), ls))
        ei = ls.index('Example::')
        exp = ls[:ei]
        uls = ls[ei + 1:]
        for l in uls:
            if l.startswith('/'):
                exp.append('\n``` {}```'.format(l))
            else:
                exp.append('  {}'.format(l))
        return '\n'.join(exp)

    def format_ids(self, names, ids):
        """Format user names with associated user id

        :param List[str] : list of user screen names
        :param List[int] ids: list of user ids
        :return: formatted string
        :rtype: str
        """
        f = self.format_username_simple
        return '\n'.join(["{} : ```{}```".format(f(n), id)
                          for (n, id) in zip(names, ids)])

    def format_bio(self, uid, time_limit, changes):
        """Format user's bio changes.

        :param int uid: user's id
        :param str time_limit: time range in string
        :type changes: [(int, str, str, str)]
        :param changes: list of (timestamp, field, old, new)
        :return: formatted string
        :rtype: str
        """
        result = ["{}'s bio changes in {}:\n".format(self.ref[uid], time_limit)]
        tz = self.config[uid]['timezone']
        for dt, field, old, new in changes:
            if field.endswith('_url'):
                old = ''
            else:
                old = ' from 「' + old + '」'
                new = '「' + new + '」'
            result.append("{}: change {}{} to {}".format(
                self.convert_time(dt, tz), field, old, new))
        return '\n'.join(result)

    def format_inline(self, mid, twts, offset, more):
        """Format inline query result tweets.

        :param str mid: unique identifier for the answered query
        :param twts: tweets
        :type twts: List[dict]
        :param int offset: offset
        :param int more: cnt of more tweets
        """
        results = []
        for t in twts:
            tz = self.config[str(t.user_id)]['timezone']
            time_str = Formatter.convert_time(t.timestamp, tz)
            title = '{} {}:'.format(time_str, self.ref[t.user_id])
            img = json.loads(t.tweet)['user']['profile_image_url']
            text = self.format1(t)
            results.append(dict(type='article', id=str(t.id), parse_mode='Markdown',
                                title=title, description=t.text, message_text=text,
                                thumb_url=img, disable_web_page_preview=True))
        return Resp(inline=dict(inline_query_id=int(mid), results=json.dumps(results),
                                next_offset=str(offset + len(twts)) if more else '',
                                cache_time=60))
