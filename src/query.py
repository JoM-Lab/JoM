# coding: utf-8
import re
import json
import math
import operator
from pathlib import Path
from pytz import timezone
from sqlalchemy import func
from functools import reduce
from datetime import datetime, timedelta
from collections import namedtuple, defaultdict
from resp import Resp
from freq import freq
from parser import Parser
from format import Formatter
from db import new_session, Tweet, Quote, Follow
from visualize import gen_sleep_png, gen_freq, gen_word_cloud, plot_trend, plot_punchcard
from twitter import twitter


class Query:

    def __init__(self, config_file='config.json', *, debug=False):
        '''
        :param config_file: json config file
        :type config_file: Dict[str, Object]
        :param bool debug: whether throw exception
        '''
        if type(config_file) is str:
            self.config = json.load(open(config_file))
        else:
            self.config = config_file
        self.debug = debug
        # the twitter api object used to query threads
        self.twitter = twitter
        self.session = new_session(self.config['db_file'])
        self.return_limit = self.config['return_limit']
        self.victims = self.config['victims']
        self.fmt = Formatter(self.victims)
        self.parser = Parser()
        # shortcut to full name mapping
        self.shortcuts = {}
        for name in self.victims:
            if name.lower() != name:
                self.victims[name.lower()] = self.victims[name]
                del self.victims[name]
                name = name.lower()
            u = self.victims[name]
            for s in u['shortcuts'].split():
                self.shortcuts[s] = name
        # caching last sent tweets for each `chat_id`
        self.cache = defaultdict(list)

    def query(self, chat_id, msg):
        ''' Process message from `chat_id` and return `Resp` to it.

        :param int chat_id: id of message sender, need to keep its sent history
        :param str msg: message string
        :return: `Resp` object
        :rtype: Resp
        '''
        try:
            parse_result = self.parser.parse(msg)
            tp, args = parse_result
            if tp == 'stat':
                return self.get_stat(*args)
            if tp == 'freq':
                return self.get_freq(*args)
            elif tp == 'search':
                return self.search(*args, chat_id=chat_id)
            elif tp == 'search_original':
                return self.search(*args, orig_only=True, chat_id=chat_id)
            elif tp == 'watch':
                return self.watch(*args)
            elif tp == 'rand':
                return self.rand(args)
            elif tp == 'rand_original':
                return self.rand(args, orig_only=True)
            elif tp == 'sleep':
                return self.sleep_time(*args)
            elif tp == 'count':
                return self.count(*args)
            elif tp == 'end':
                return Resp(message='.')
            elif tp == 'config':
                return Resp(message=self.fmt.format_config())
            elif tp == 'quote':
                return self.search_quote(*args)
            elif tp == 'randq':
                return self.rand_quote(args)
            elif tp == 'remember':
                return self.remember(*args)
            elif tp == 'forget':
                return self.forget(*args)
            elif tp == 'ff':
                return self.ff_status(*args)
            elif tp == 'fff':
                return self.fff_status(*args)
            elif tp == 'help':
                cmdlist = Path(__file__).absolute().parent.parent / 'cmdlist.txt'
                resp = cmdlist.open().read() if cmdlist.exists() else 'baka!'
                return Resp(message=resp)
            elif tp == 'wordcloud':
                return self.wordcloud(args)
            elif tp == 'trend':
                return self.trend(*args)
            elif tp == 'thread':
                return self.thread(args, chat_id=chat_id)
            elif tp == 'punchcard':
                return self.punchcard(*args)
            else:
                raise Exception('no cmd ' + tp)
        except Exception as e:
            if self.debug:
                raise  # for debug
            return Resp(message=str(e))

    def watch(self, sender, action=None, *kws):
        ''' Manage watched keywords to notify.

        :param str sender: single sender
        :param action: None/+/-
        :type action: str | None
        :param kws: keywords to add or delete
        :type kws: List[str]
        :return: `Resp` object
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        keywords = json.load(open('keywords.json'))
        if sender not in keywords:
            keywords[sender] = []

        if action == '+':
            assert kws, "no keywords provided"
            keywords[sender].append(kws)
            # update local file in real time
            json.dump(keywords, open('keywords.json', 'w'))
        elif action == '-':
            assert kws, "no keywords provided"
            keywords[sender] = [k for k in keywords[sender]
                                if set(k) != set(kws)]
            # update local file in real time
            json.dump(keywords, open('keywords.json', 'w'))
        elif action is not None:
            raise Exception("the second parameter should be +/-")

        msg = self.fmt.format_keywords(sender, keywords[sender])
        return Resp(message=msg)

    def _to_sender(self, sender):
        ''' Helper function that convert a string to a list of senders.

        :param str sender: may be '*' or 'sender1|sender2'
        :return: list of senders' full names
        :rtype: List[str]
        '''
        if sender == '*':
            return [v for v in self.shortcuts.values()]
        senders = []
        for s in sender.split('|'):
            s = s.lower()
            if s in self.shortcuts:
                s = self.shortcuts[s]
            if s in self.victims:
                senders.append(s)
            else:
                raise Exception('user {} not found'.format(s))
        return senders

    def rand(self, sender, *, orig_only=False):
        ''' A random tweet from someone.

        :param str sender: single sender
        :param bool orig_only: original tweets only
        :return: `Resp` object
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        cond = Tweet.sender == sender
        if orig_only:
            cond = cond & (Tweet.type == 'tweet')
        one = self.session.query(Tweet).filter(cond)\
                          .order_by(func.random()).first()
        if one is None:
            return Resp(message="nothing indexed")
        cmd = '/randt' if orig_only else '/rand'
        keyboard = {"keyboard": [[cmd + ' ' + sender], ['/end']],
                    "selective": True, "resize_keyboard": True}
        return Resp(message=self.fmt.format_rand(one),
                    keyboard=keyboard, markdown=True)

    Cond = namedtuple('Cond', 'cond contains excludes limit page offset idx '
                              'nokbd desc')
    ''' Search conditions parsed by `gen_search_cond`.

    :param cond: combined `sqlalchemy` condition object
    :type cond: sqlalchemy.sql.elements.BinaryExpression
    :param contains: list of keyword strings should be contained
    :type contains: List[str]
    :param excludes: list of keyword strings should not be excluded
    :type excludes: List[str]
    :param int limit: the most number of tweets to return
    :param int page: page number, start from 1
    :param int offset: count of tweets skipped according to `page`
    :param int idx: index number used as option
    :param bool nokbd: whether hide the keyboard
    :param bool desc: whether in decreasing order
    '''

    def gen_search_cond(self, senders, queries, table, *, orig_only=False):
        '''
        :param str senders: senders to query
        :param queries: list of all keywords and configs
        :type queries: List[str]
        :param table: `sqlalchemy` table class
        :type table: sqlalchemy.ext.declarative.api.DeclarativeMeta
        :param bool orig_only: original tweets only
        :return: `Resp` object
        :rtype: Resp
        '''
        excludes = [par[1:] for par in queries if par[0] == '-']
        contains = [par for par in queries if par[0] != '-' and par[0] != '!']
        cfg = [par[1:] for par in queries if par[0] == '!']

        # default condition values
        limit = self.return_limit
        page, idx, nokbd, desc = 1, None, False, True

        for c in cfg:
            if not c:
                continue
            if c[0] == 'c':
                assert c[1:].isdigit(), 'count is not a number'
                limit = int(c[1:])
            elif c[0] == 'p':
                assert c[1:].isdigit(), 'page is not a number'
                page = int(c[1:])
            elif c[0] == 'i':
                assert c[1:].isdigit(), 'index is not a number'
                idx = int(c[1:])
            elif c[0] == '!':
                nokbd = True
            elif c[0] == '<':
                desc = False

        limit = min(10, max(limit, 1))
        page = max(page, 1)
        offset = (page - 1) * limit

        author = table.sender if hasattr(table, 'sender') else table.person
        cond = reduce(operator.or_, [author == s for s in senders])

        for w in contains:
            cond = cond & table.text.op('regexp')(w)
        for w in excludes:
            cond = cond & ~table.text.op('regexp')(w)
        if orig_only and hasattr(table, 'type'):
            cond = cond & (table.type == 'tweet')

        return self.Cond(cond=cond, contains=contains, excludes=excludes,
                         limit=limit, page=page, offset=offset, idx=idx,
                         nokbd=nokbd, desc=desc)

    def search(self, senders, *paras, orig_only=False, chat_id=None):
        '''
        :param str senders: senders to query
        :param paras: keywords and configs
        :type paras: List[str]
        :param bool orig_only: original tweets only
        :param int chat_id: id to cache sent tweets
        :return: `Resp` object
        :rtype: Resp
        '''
        _senders = senders
        senders = self._to_sender(senders)
        c = self.gen_search_cond(senders, paras, table=Tweet,
                                 orig_only=orig_only)
        base = self.session.query(Tweet).filter(c.cond)

        if c.desc:
            base = base.order_by(Tweet.timestamp.desc())
        else:
            base = base.order_by(Tweet.timestamp.asc())

        base = base.offset(c.offset)
        rows = list(base.limit(c.limit))
        cnt = base.count() - len(rows)

        cmd = '/st' if orig_only else '/s'
        arg = ' '.join(c.contains + ['-' + i for i in c.excludes])
        opt = '{} {} {} !c{}'.format(cmd, _senders, arg, c.limit)
        options = []
        if c.page != 1:
            options.append(['{} !p{}'.format(opt, c.page - 1)])
        if cnt:
            options.append(['{} !p{}'.format(opt, c.page + 1)])
        if not c.nokbd and options:
            keyboard = {"keyboard": options + [['/end']],
                        "selective": True, "resize_keyboard": True}
        else:
            keyboard = None
        msg = self.fmt.format_search(rows, cnt)

        # cache sent tweets
        if chat_id:
            self.cache[chat_id] = rows

        # turn on preview if only one tweet
        preview = len(rows) == 1  # and 'twimg' in rows[0].text
        return Resp(message=msg, preview=preview, keyboard=keyboard,
                    markdown=True)

    def count(self, senders, *paras):
        ''' Count of keywords mentioned in all time.

        :param str senders: senders
        :param paras: keywords
        :type paras: List[str]
        :return: `Resp` object
        :rtype: Resp
        '''
        senders = self._to_sender(senders)
        c = self.gen_search_cond(senders, paras, Tweet)
        cnt = self.session.query(Tweet).filter(c.cond).count()
        msg = self.fmt.format_count(senders, c.contains, c.excludes, cnt)
        return Resp(message=msg)

    def get_freq(self, sender, time_limit, timestamp):
        ''' Visualization of daily tweet frequency.

        :param str sender: single sender
        :param str time_limit: time range in string
        :param int timestamp: timestamp to start
        :return: image file in `Resp.fileobj`
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        tz = self.victims[sender]['timezone']
        cond = (Tweet.sender == sender) & (Tweet.timestamp >= timestamp)
        rows = self.session.query(Tweet).filter(cond).all()
        if not rows:
            return Resp(message='no data')
        cnt = defaultdict(int)
        for r in rows:
            t = datetime.fromtimestamp(r.timestamp, timezone(tz))
            cnt[(t.year, t.month, t.day)] += 1
        i = datetime(*min(cnt.keys()))
        mx = datetime(*max(cnt.keys()))
        while i != mx:
            cnt[(i.year, i.month, i.day)]  # make cnt of days w/o data just 0
            i += timedelta(days=1)
        res = sorted(cnt.items(), key=lambda i: i[0])
        gen_freq(res)
        return Resp(fileobj=open('freq.png', 'rb'))

    def get_stat(self, sender, time_limit, timestamp):
        ''' Tweets statistics in a time range.

        :param str sender: single sender
        :param str time_limit: time range in string
        :param int timestamp: timestamp to start
        :return: `Resp` object
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        cond = (Tweet.sender == sender) & (Tweet.timestamp >= timestamp)
        rows = self.session.query(Tweet).filter(cond)\
                           .order_by(Tweet.timestamp.desc())
        indexed = self.session.query(Tweet)\
                              .filter(Tweet.sender == sender).count()
        tweet_num, reply_num, rt_num, quote_num, total = 0, 0, 0, 0, 0
        since_ts = self.session.query(func.min(Tweet.timestamp))\
                               .filter(Tweet.sender == sender).first()[0]
        for r in rows:
            total += 1
            if r.type == 'tweet':
                tweet_num += 1
            if r.type == 'reply':
                reply_num += 1
            if r.type == 'rt':
                rt_num += 1
            if r.type == 'quote':
                quote_num += 1
        details = [tweet_num, reply_num, rt_num, quote_num,
                   total, indexed, since_ts]
        return Resp(message=self.fmt.format_stat(sender, time_limit, details))

    def sleep_time(self, sender, time_limit, timestamp):
        ''' Visualization of inferred sleep time in each day.

        :param str sender: single sender
        :param str time_limit: time range in string
        :param int timestamp: timestamp to start
        :return: image file in `Resp.fileobj`
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        tz = self.victims[sender]['timezone']
        cond = (Tweet.sender == sender) & (Tweet.timestamp >= timestamp)
        rows = self.session.query(Tweet).filter(cond)\
                           .order_by(Tweet.timestamp.asc())  # ascending order
        tt = namedtuple('TimeTweet', 'time text')  # `datetime` and text
        tts = [tt(datetime.fromtimestamp(r.timestamp, timezone(tz)), r.text)
               for r in rows]
        sleep_intervals = []
        last = None
        for this in tts:
            # if last tweet sent between 22 and 5
            if last and (22 <= last.time.hour or last.time.hour <= 5) and(
                    # this tweet sent between 3 and 12
                    (3 <= this.time.hour <= 12) and
                    # the range gap between 3.5h and 16h
                    (3.5 * 3600 <= (this.time - last.time).total_seconds() <= 16 * 3600) and
                    # wake up 10h after last wake-up
                    (not sleep_intervals or
                     (last.time - sleep_intervals[-1][1].time).total_seconds() >= 10 * 3600)):
                sleep_intervals.append((last, this))
            last = this
        res = [(sleep.time, wake.time) for (sleep, wake) in sleep_intervals]
        if not res:
            return Resp(message='no data')
        gen_sleep_png(res)
        return Resp(fileobj=open('sleep.png', 'rb'))

    def search_quote(self, _senders, *qs):
        '''
        :param str _senders: senders to query
        :param qs: keywords
        :type qs: List[str]
        :return: `Resp` object
        :rtype: Resp
        '''
        senders = self._to_sender(_senders)
        c = self.gen_search_cond(senders, qs, Quote)
        base = self.session.query(Quote).filter(c.cond)
        if c.desc:
            base = base.order_by(Quote.timestamp.desc()).offset(c.offset)
        else:
            base = base.order_by(Quote.timestamp.asc()).offset(c.offset)
        rows = list(base.limit(c.limit))
        cnt = base.count() - len(rows)
        arg = ' '.join(c.contains + ['-' + i for i in c.excludes])
        opt = '/q {} {} !c{}'.format(_senders, arg, c.limit)
        options = []
        if c.page != 1:
            options.append(['{} !p{}'.format(opt, c.page - 1)])
        if cnt:
            options.append(['{} !p{}'.format(opt, c.page + 1)])
        if not c.nokbd and options:
            keyboard = {"keyboard": options + [['/end']],
                        "selective": True, "resize_keyboard": True}
        else:
            keyboard = None
        return Resp(message=self.fmt.format_quote(rows, cnt), keyboard=keyboard)

    def rand_quote(self, sender):
        '''
        :param str sender: single sender
        :return: `Resp` object
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        person = sender[0]
        cond = Quote.person == person
        row = self.session.query(Quote).filter(cond)\
                          .order_by(func.random()).first()
        return Resp(message=self.fmt.format_quote([row], 0))

    def remember(self, sender, quote):
        ''' Add quotation of someone.

        :param str sender: single sender
        :param str quote: quotation text
        :return: `Resp` object
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        person = sender[0]
        record = Quote(timestamp=datetime.now().timestamp(),
                       person=person, text=quote)
        self.session.add(record)
        self.session.commit()
        return Resp(message="I remembered.")

    def forget(self, sender, *qs):
        ''' Delete quotation of someone.

        :param str sender: single sender
        :param qs: keywords of the quotation to be deleted
        :type qs: List[str]
        :return: `Resp` object
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        c = self.gen_search_cond(sender, qs, Quote)
        quos = self.session.query(Quote).filter(c.cond)\
                           .order_by(Quote.timestamp.desc())\
                           .offset(c.offset).limit(c.limit).all()
        if len(quos) == 0:
            return Resp(message="nothing found")
        if len(quos) > 1 and c.idx is None:
                msg = self.fmt.format_quote(quos, 0) + '-' * 25 +\
                      "\nuse !iX as the index of quotation to forget"
                return Resp(message=msg)
        else:  # len(quos) == 1 or (len(quos) > 1 and idx is not None)
            if len(quos) == 1:
                q = quos[0]
            else:
                assert 1 <= c.idx <= len(quos), "bad index"
                q = quos[c.idx - 1]
            self.session.delete(q)
            self.session.commit()
            msg = "deleted quotation of {}: “{}”".format(sender[0], q.text)
            return Resp(message=msg)

    def fff_status(self, sender, time_limit, timestamp, config):
        ''' Follower and following changes in details.

        :param str sender: single sender
        :param str time_limit: time range in string
        :param int timestamp: timestamp to start
        :param config: page number and some of fo/unfo/foed/unfoed
        :type config: Dict[str, int | bool]
        :return: `Resp` object
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        page = max(config.get('p', 1), 1)
        filter_op = [k for k in config if k != 'p']
        cond = (Follow.person == sender) & (Follow.timestamp >= timestamp)
        if filter_op:
            cond &= reduce(operator.or_, [Follow.action == f for f in filter_op])
        offset = (page - 1) * 20  # hard-coded limit...
        base = self.session.query(Follow).filter(cond)\
                           .order_by(Follow.timestamp.desc())\
                           .offset(offset)
        rows = base.limit(20).all()
        cnt = base.count() - len(rows)
        options = []
        filter_op_text = '' if len(filter_op) == 4\
                         else ' '.join(['!{}'.format(f) for f in filter_op])
        if page != 1:
            options.append(['/fff {} {} {} !p{}'.format(
                           sender, time_limit, filter_op_text, page - 1)])
        if cnt:
            options.append(['/fff {} {} {} !p{}'.format(
                           sender, time_limit, filter_op_text, page + 1)])
        if options:
            keyboard = {"keyboard": options + [['/end']],
                        "selective": True,
                        "resize_keyboard": True}
        else:
            keyboard = None
        return Resp(message=self.fmt.format_fff(sender, time_limit, rows, cnt),
                    keyboard=keyboard, markdown=True)

    def ff_status(self, sender, time_limit, timestamp, filter_op):
        ''' Follower and following changes in summary.

        :param str sender: single sender
        :param str time_limit: time range in string
        :param int timestamp: timestamp to start
        :param filter_op: some of fo/unfo/foed/unfoed
        :type filter_op: List[str]
        :return: `Resp` object
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        cond = (Follow.person == sender) & (Follow.timestamp >= timestamp)
        if filter_op:
            cond &= reduce(operator.or_, [Follow.action == f for f in filter_op])
        rows = self.session.query(Follow).filter(cond)\
                           .order_by(Follow.timestamp.desc())

        f = defaultdict(list)
        for r in rows:
            if r.action not in ['fo', 'unfo', 'foed', 'unfoed']:
                raise Exception("Unknown action: " + r.action)
            if not filter_op or r.action in filter_op:
                f[r.action].append(r)

        # eliminate +someone & -someone at the same time range
        unfo, fo, unfoed, foed = f['unfo'], f['fo'], f['unfoed'], f['foed']
        dup_fo_ids = {u.target_id for u in unfo} & {u.target_id for u in fo}
        fo = [u for u in fo if u.target_id not in dup_fo_ids]
        unfo = [u for u in unfo if u.target_id not in dup_fo_ids]
        dup_foed_ids = {u.target_id for u in unfoed} & {u.target_id for u in foed}
        foed = [u for u in foed if u.target_id not in dup_foed_ids]
        unfoed = [u for u in unfoed if u.target_id not in dup_foed_ids]

        msg = self.fmt.format_ff(sender, time_limit, unfo=unfo, fo=fo,
                                 unfoed=unfoed, foed=foed)
        return Resp(message=msg, markdown=True)

    def wordcloud(self, sender):
        '''
        :param str sender: single sender
        :return: image file in `Resp.fileobj`
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        texts = self.session.query(Tweet.text).filter(Tweet.sender == sender)
        gen_word_cloud(freq(i.text for i in texts))
        return Resp(fileobj=open('wordcloud.png', 'rb'))

    def trend(self, sender, time_range, time_interval, kws, time_raw):
        ''' Trending visualization of keywords.

        :param str sender: senders
        :param int time_range: time range in seconds
        :param int time_interval: time interval in seconds
        :param kws: keywords to trend
        :type kws: List[str]
        :param str time_raw: time range in raw string
        :return: image file in `Resp.fileobj`
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        time_size = math.ceil(time_range / time_interval)
        # starting timestamp
        time_start = int(datetime.now().timestamp()) - time_size * time_interval
        # keywords from different senders are summed up
        cond = reduce(operator.or_, (Tweet.sender == s for s in sender))
        all_tweets = self.session.query(Tweet)\
            .filter(cond & (Tweet.timestamp >= time_start))\
            .order_by(Tweet.timestamp.desc()).all()
        # keyword: [count in each period]
        kwd_freq = {k: [] for k in kws}
        # the total count in each period as norm factor
        norm_factors = []
        # ticks to show on x-axis
        ticks = []

        for i in range(time_size):
            t1 = time_start + i * time_interval
            t2 = time_start + (i + 1) * time_interval
            ticks.append(self.fmt.format_trend_x_label(t1, t2))

            tweets = []
            while all_tweets and all_tweets[-1].timestamp <= t2:
                tweets.append(all_tweets.pop())

            norm_factors.append(len(tweets))
            for k in kws:
                matched = [t for t in tweets if re.search(k, t.text, re.I)]
                kwd_freq[k].append(len(matched))

        metadata = [sender] + time_raw
        plot_trend(kws, [kwd_freq[kw] for kw in kws], norm_factors, ticks, metadata)
        return Resp(fileobj=open('trend.png', 'rb'))

    def fetch_conversation(self, sid):
        ''' Fetch conversation by tweet id via twitter api.

        :param sid: tweet id
        :type sid: str | int
        :return: list of tweets
        :rtype: List[Dict]
        '''
        threads = self.twitter.get('https://api.twitter.com/1.1/conversation/show.json',
                                   params=dict(id=sid, include_entities=1)).json()
        return [] if 'errors' in threads else threads

    def thread(self, id_str, *, chat_id=None):
        ''' Generate thread from conversation online and tweets in DB.

        :param str id_str: if <= 20 the id in sent history,
                           or the real tweet id or url
        :param chat_id: None for real tweet id, int for id in history
        :type chat_id: int | None
        :return: `Resp` object
        :rtype: Resp
        '''
        id_str = id_str.split('/')[-1]
        assert id_str.isdigit(), 'not ended with digits'
        _id = int(id_str)
        if _id <= 20:  # if it's index of history tweets
            assert chat_id and self.cache[chat_id], "no history tweets"
            cnt_hist = len(self.cache[chat_id])
            assert _id - 1 < cnt_hist, "only {} history tweets".format(cnt_hist)
            tweet = self.cache[chat_id][_id - 1]
            # may be object in DB or json
            status_id = tweet.id if isinstance(tweet, Tweet) else tweet['id']
        else:
            status_id = _id  # assume it is real status id
        sid = status_id

        threads = []  # in increasing order of timestamp
        while len(threads) < 11 and sid:  # at most 11
            t = self.session.query(Tweet).filter(Tweet.id == sid).first()
            if t:  # if in DB, go up in DB
                t = json.loads(t.tweet)
                threads.insert(0, t)
                sid = t['in_reply_to_status_id']
                continue
            # if not in DB, try conversation api
            ts = self.fetch_conversation(sid)
            if not ts:
                break
            if threads:
                threads = [t for t in ts if t['id'] < threads[0]['id']] + threads +\
                          [t for t in ts if t['id'] > threads[-1]['id']]
            else:
                threads = ts
            sid = threads[0]['in_reply_to_status_id']

        if not threads:
                return Resp(message='no data')

        # try conversation api to find more latter tweets
        latter = self.fetch_conversation(threads[-1]['id'])
        threads.extend([t for t in latter if t['id'] > threads[-1]['id']])

        # limit to 5 forwards and backwards tweets
        idx = [t['id'] for t in threads].index(status_id)
        if idx <= 5:
            threads = threads[:10]
        elif len(threads)-1-idx <= 5:
            threads = threads[-10:]
        else:
            threads = threads[idx-5:idx+6]

        return Resp(message=self.fmt.format_thread(threads), markdown=True)

    def punchcard(self, sender, time_limit, timestamp):
        ''' Visualization of tweets frequency in each hour of each week day.

        :param str sender: single sender
        :param str time_limit: time range in string
        :param int timestamp: timestamp to start
        :return: image file in `Resp.fileobj`
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, 'so much people'
        sender = sender[0]
        cond = (Tweet.sender == sender) & (Tweet.timestamp >= timestamp)
        rows = self.session.query(Tweet).filter(cond).order_by(Tweet.timestamp.desc())
        stamps = [r.timestamp for r in rows]
        tz = self.victims[sender]['timezone']
        if isinstance(tz, str):
            tz = timezone(tz)
        d = defaultdict(int)
        for s in stamps:
            s = datetime.fromtimestamp(s, tz)
            d[(s.weekday(), s.hour)] += 1
        plot_punchcard(d, sender, time_limit)
        return Resp(fileobj=open('pc.png', 'rb'))
