# coding: utf-8

from datetime import datetime
import json
from tabulate import tabulate
from pytz import timezone, utc


class Formatter:
    """Format strings, pretty-printer, etc"""

    def __init__(self, config):
        self.config = config

    def convert_time(self, timestamp, tz):
        s = datetime.fromtimestamp(timestamp, timezone(tz))\
                    .strftime('%Y/%m/%d %H:%M:%S')
        return s[5:] if s[:4] == str(datetime.now().year) else s

    def format_config(self):
        l = []
        for name in self.config:
            d = self.config[name]
            l.append([str(name), str(d["shortcuts"]), str(d["timezone"]), str(d["interval"])])
        return tabulate(l, headers=["Name", "Shortcuts", "Timezone", "Interval"])

    def format_rows(self, rows):
        msg = ""
        for i, r in enumerate(rows):
            msg += "{:-^34}\n{}\n".format(i+1, self.format1(r))
        return msg

    def format_search(self, rows, cnt):
        if not rows:
            return 'no results'
        cnt_msg = '_' * 25 + '\n{} more tweets'.format(cnt)
        return self.format_rows(rows) + (cnt_msg if cnt else '')

    def format_rand(self, r):
        return self.format1(r)

    def format_count(self, sender, y, n, count):
        msg = "Number of tweets by {} with words {}\n".format(sender, ', '.join(y))
        if n:
            msg += "but without words {}\n".format(', '.join(n))
        msg += "\n-> {}\n".format(count)
        return msg

    def format1(self, row):
        sender, tweet_type, ts = row.sender, row.type, row.timestamp
        tweet, text = json.loads(row.tweet), row.text
        ty = 'replies' if tweet_type == 'reply' else tweet_type + 's'
        url = 'twitter.com/{}/status/{}'.format(sender, tweet["id_str"])
        tz = self.config[sender]['timezone']
        msg = '{}, {} {}:\n{}\n\n{}'.format(
            self.convert_time(ts, tz), sender, ty, text, url)
        return msg

    def format_stat(self, user_name, time_limit, details):
        msg = "{}'s statistic during past {}:\n".format(user_name, time_limit)
        tz = self.config[user_name]['timezone']
        details[6] = details[6] and self.convert_time(details[6], tz)
        msg += ("  Tweet: {}\n  Reply: {}\n  RT: {}\n"
                "  Quote: {}\n  Total: {}\n  Indexed: {}\n"
                "  Since: {}\n").format(*details)
        return msg

    def format_quote(self, results, cnt):
        res = ''
        for i, r in enumerate(results):
            res += '{}. “{}” —— {}\n'.format(i+1, r.text, r.person)
        if not res:
            return "no quations"
        if cnt:
            res += '_' * 25 + '\n{} more'.format(cnt)
        return res

    def format_ff(self, sender, time_limit, unfo, fo, unfoed, foed):
        following = ', '.join(['+'+u.target_name for u in fo] +
                              ['-'+u.target_name for u in unfo])
        follower = ', '.join(['+'+u.target_name for u in foed] +
                             ['-'+u.target_name for u in unfoed])
        return ("{}'s following/follower status during past {}:\n"
                "following: {} \nfollower: {}\n").format(
                    sender, time_limit, following, follower)

    def format_fff(self, _sender, _time_limit, records, more):
        res = []
        action = {'unfo': 'unfo', 'fo': 'fo',
                  'unfoed': 'unfoed by', 'foed': 'foed by'}
        for r in records[:30]:
            time = datetime.fromtimestamp(r.timestamp, utc)
            res.append('{} {} {}'.format(time.strftime('%m/%d %H:%M'),
                                         action[r.action], r.target_name))
        if more:
            res.append('_' * 25 + '\n{} more...'.format(more))
        return '\n'.join(res)

    def format_kw_notif(self, kws, tweet):
        sender, tweet_type, ts = tweet.sender, tweet.type, tweet.timestamp
        tweet, text = json.loads(tweet.tweet), tweet.text
        ty = 'replies' if tweet_type == 'reply' else tweet_type + 's'
        ty += '\ncontaining monitored pattern: {}'.format(", ".join(kws))
        url = 'twitter.com/{}/status/{}'.format(sender, tweet["id_str"])
        tz = self.config[sender]['timezone']
        msg = '{}, {} {}:\n\n{}\n\n{}'.format(
            self.convert_time(ts, tz), sender, ty, text, url)
        return msg
