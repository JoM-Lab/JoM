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
from .resp import Resp
from .freq import freq
from .parser import Parser
from .format import Formatter
from .db import new_session, Tweet, Quote, Follow, Bio
from .visualize import gen_sleep_png, gen_freq, gen_word_cloud, plot_trend, plot_punchcard
from .twitter import fetch_conversation, names2ids
from sqlalchemy.exc import SQLAlchemyError


class Query:
    ''' Process queries. '''

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
        self.session = new_session()
        self.return_limit = self.config['return_limit']
        self.victims = self.config['victims']
        self.fmt = Formatter(self.victims)
        self.parser = Parser()
        # shortcut to full name mapping
        self.shortcuts = {}
        for uid in self.victims:
            u = self.victims[uid]
            self.shortcuts[u['ref_screen_name']] = uid
            for s in u['shortcuts'].split():
                self.shortcuts[s] = uid
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
                return Resp(message=self.fmt.format_config(),
                            markdown=True)
            elif tp == 'quote':
                return self.search_quote(*args)
            elif tp == 'randq':
                return self.rand_quote(args)
            elif tp == 'remember':
                return self.remember(*args)
            elif tp == 'forget':
                return self.forget(*args)
            elif tp == 'f':
                return self.f_status(*args)
            elif tp == 'fs':
                return self.fs(args)
            elif tp == 'help':
                return self.help(args)
            elif tp == 'wordcloud':
                return self.wordcloud(args)
            elif tp == 'trend':
                return self.trend(*args)
            elif tp == 'thread':
                return self.thread(args, chat_id=chat_id)
            elif tp == 'punchcard':
                return self.punchcard(*args)
            elif tp == 'deleted':
                return self.deleted(*args, chat_id=chat_id)
            elif tp == 'ids':
                return self.ids(args)
            elif tp == 'bio':
                return self.bio_changes(*args)
            else:
                raise Exception('no cmd ' + tp)
        except SQLAlchemyError as e:
            # roolback when bad thing happened
            # otherwise all further requests raise InternalError
            # TODO: better solution?
            self.session.rollback()
            return Resp(message=str(e).splitlines()[0])
        except Exception as e:
            if self.debug:
                raise  # for debug
            return Resp(message='ERROR: ' + str(e))

    def watch(self, sender, action=None, *kws):
        ''' Manage watched keywords to notify.

        Example::

            /watch j + good
                monitor j'tweets containing pattern "good"
            /watch j + a b
                monitor j'tweets containing both pattern "a" and "b"
            /watch j - a b
                stop monitoring j's tweets containing both pattern "a" and "b"

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

        Example::

            /rand j
                a random tweet from j

        :param str sender: single sender
        :param bool orig_only: original tweets only
        :return: `Resp` object
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        cond = Tweet.user_id == int(sender)
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

        cond = reduce(operator.or_, [table.user_id == int(s) for s in senders])

        for w in contains:
            cond = cond & table.text.op('~*')(w)
        for w in excludes:
            cond = cond & table.text.op('!~*')(w)
        if orig_only and hasattr(table, 'type'):
            cond = cond & (table.type == 'tweet')

        return self.Cond(cond=cond, contains=contains, excludes=excludes,
                         limit=limit, page=page, offset=offset, idx=idx,
                         nokbd=nokbd, desc=desc)

    def search(self, senders, *paras, orig_only=False, chat_id=None):
        '''Search tweet of a target

        Example::

            /s j apple good -bad
                search j's tweet containing pattern "apple" and "good",
                but not "bad", default at 10 tweets per page
            /st j apple
                search j's original tweet containing pattern "apple"
            /s j apple !!
                no keyboard
            /s j apple !<
                show results in reverse order
            /s j apple !c2 !p3
                show result starting at page 3,
                with each page containing two tweets

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
        rows = base.limit(c.limit).all()
        cnt = base.count() - len(rows)

        cmd = '/st' if orig_only else '/s'
        arg = ' '.join(c.contains + ['-' + i for i in c.excludes])
        opt = '{} {} {} !c{}'.format(cmd, _senders, arg, c.limit)
        if not c.desc:
            opt += ' !<'
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
        preview = False
        if len(rows) == 1:
            t = json.loads(rows[0].tweet)
            entities = t.get('entities', {})
            if entities.get('media', []):  # TODO: urls?
                if not t['user']['protected']:
                    preview = True
        return Resp(message=msg, preview=preview, keyboard=keyboard,
                    markdown=True)

    def count(self, senders, *paras):
        ''' Count of keywords mentioned in all time.

        Example::

            /cnt j apple good
                count tweets of j containing either "apple" or "good"

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

        Example::

            /freq j
                show j's frequency of tweet in 7 days
            /freq j 2w
                show j's frequency of tweet in 2 weeks

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
        cond = (Tweet.user_id == int(sender)) & (Tweet.timestamp >= timestamp)
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

        Example::

            /stat j
                show j's tweet statistics in 24 hours
            /stat j 2w
                show j's tweet statistics in two weeks

        :param str sender: single sender
        :param str time_limit: time range in string
        :param int timestamp: timestamp to start
        :return: `Resp` object
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        cond = (Tweet.user_id == int(sender)) & (Tweet.timestamp >= timestamp)
        rows = self.session.query(Tweet).filter(cond)\
                           .order_by(Tweet.timestamp.desc())
        indexed = self.session.query(Tweet)\
                              .filter(Tweet.user_id == int(sender)).count()
        tweet_num, reply_num, rt_num, quote_num, deleted, total = 0, 0, 0, 0, 0, 0
        since_ts = self.session.query(func.min(Tweet.timestamp))\
                               .filter(Tweet.user_id == int(sender)).first()[0]
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
            if r.deleted:
                deleted += 1
        details = [tweet_num, reply_num, rt_num, quote_num, deleted,
                   total, indexed, since_ts]
        return Resp(message=self.fmt.format_stat(sender, time_limit, details))

    def sleep_time(self, sender, time_limit, timestamp):
        ''' Visualization of inferred sleep time in each day.

        Example::

            /sleep j
                show j's sleep pattern in seven days
            /sleep j 2w
                show j's sleep pattern in two weeks

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
        cond = (Tweet.user_id == int(sender)) & (Tweet.timestamp >= timestamp)
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
        '''Search quote containing certain keywords from a target

        Example::

            /quote n ab cd
                search n's quote containing pattern "ab" and "cd
            /quote j
                list all quotes of j

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
        rows = base.limit(c.limit).all()
        cnt = base.count() - len(rows)
        arg = ' '.join(c.contains + ['-' + i for i in c.excludes])
        opt = '/q {} {} !c{}'.format(_senders, arg, c.limit)
        if not c.desc:
            opt += ' !<'
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
        '''Return a random quote for a target

        Example::

            /rand_quote j
                random quote of j

        :param str sender: single sender
        :return: `Resp` object
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        cond = Quote.user_id == int(sender[0])
        row = self.session.query(Quote).filter(cond)\
                          .order_by(func.random()).first()
        return Resp(message=self.fmt.format_quote([row], 0))

    def remember(self, sender, quote):
        ''' Add quotation of someone.

        Example::

            /remember j abc
                add quote "abc" for j
            /remember j a b c
                add quote "a b c" for j

        :param str sender: single sender
        :param str quote: quotation text
        :return: `Resp` object
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        record = Quote(timestamp=datetime.now().timestamp(),
                       user_id=int(sender[0]), text=quote)
        self.session.add(record)
        self.session.commit()
        return Resp(message="I remembered.")

    def forget(self, sender, *qs):
        ''' Delete quotation of someone.

        Example::

            /forget j abc
                forget j's quote of "abc",
                show a list if there are multiple matches
            /forget j a b c
                forget j's quote with words "a", "b", "c"
            /forget j a b !i2
                forget j's 2nd quote with words "a", "b"

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
            msg = "deleted quotation of {}: “{}”".format(
                self.victims[sender[0]]['ref_screen_name'], q.text)
            return Resp(message=msg)

    def f_status(self, sender, time_limit, timestamp, config):
        ''' Follower and following changes in details.

        Example::

            /f j 7d !unfo !fo
                follower and following change of j in 7 days, only showing
                results of unfo and fo only.

            /f j
                all follower and following change of j in 24 hours

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
        cond = (Follow.user_id == int(sender)) & (Follow.timestamp >= timestamp)
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
            options.append(['/f {} {} {} !p{}'.format(
                           sender, time_limit, filter_op_text, page - 1)])
        if cnt:
            options.append(['/f {} {} {} !p{}'.format(
                           sender, time_limit, filter_op_text, page + 1)])
        if options:
            keyboard = {"keyboard": options + [['/end']],
                        "selective": True,
                        "resize_keyboard": True}
        else:
            keyboard = None
        return Resp(message=self.fmt.format_f(sender, time_limit, rows, cnt),
                    keyboard=keyboard, markdown=True)

    def fs(self, args):
        '''Search following/follower change

        Example::

            /fs j|jd foo bar
                list changes of j or jd related to foo or bar

        :param args: List[str]
        :return: `Resp` object
        :rtype: Resp
        '''
        targets, changed = self._to_sender(args[0]), args[1:]
        cond = reduce(operator.or_, [Follow.user_id == int(f) for f in targets])
        cond &= reduce(operator.or_, [Follow.target_name.ilike('%{}%'.format(s)) for s in changed])
        rows = self.session.query(Follow).filter(cond)\
                           .order_by(Follow.timestamp.desc()).all()
        msg = self.fmt.format_fs(targets, changed, rows)
        return Resp(message=msg, markdown=True)

    def wordcloud(self, sender):
        '''Plot wordcloud of a target

        Example::

            /wordcloud j
                plot wordcloud of j

        :param str sender: single sender
        :return: image file in `Resp.fileobj`
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        texts = self.session.query(Tweet.text).filter(Tweet.user_id == int(sender))
        gen_word_cloud(freq(i.text for i in texts))
        return Resp(fileobj=open('wordcloud.png', 'rb'))

    def trend(self, senders, time_range, time_interval, kws, time_raw):
        ''' Trending visualization of keywords.

        Example::

            /trend j 2w 1w a b c
                "2w" is the time range we want to query,
                "1w" is the interval or granularity.
                "a", "b", "c" are keywords.
                This command means plotting graph showing trend
                of tweets containing "a", "b", "c" in the past two weeks with
                time interval of one week.
                In this case, two data points will be generated.
                If interval is '2d', then 7 data points are
                generated for seven 2-days for each pattern.

                Default time range and time interval are '3m' and '2w'.

        :param str sender: senders
        :param int time_range: time range in seconds
        :param int time_interval: time interval in seconds
        :param kws: keywords to trend
        :type kws: List[str]
        :param str time_raw: time range in raw string
        :return: image file in `Resp.fileobj`
        :rtype: Resp
        '''
        senders = self._to_sender(senders)
        time_size = math.ceil(time_range / time_interval)
        # starting timestamp
        time_start = int(datetime.now().timestamp()) - time_size * time_interval
        # keywords from different senders are summed up
        cond = reduce(operator.or_, (Tweet.user_id == int(s) for s in senders))
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

        metadata = ([self.victims[s]['ref_screen_name'] for s in senders], time_raw)
        plot_trend(kws, [kwd_freq[kw] for kw in kws], norm_factors, ticks, metadata)
        return Resp(fileobj=open('trend.png', 'rb'))

    def thread(self, id_str, *, chat_id=None):
        ''' Generate thread from conversation online and tweets in DB.

        Example::

            /thread 665031794572582912
                show conversation thread of tweet of id 665031794572582912
            /thread 4
                show conversation thread of the 4th tweet
                in the result of last query

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
            ts = fetch_conversation(sid)
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
        latter = fetch_conversation(threads[-1]['id'])
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

        Example::

            /pc j 4m
                punchcard for j with data collected in 4 months
            /punnchard st 3d
                punchcard for st with data collected in 3 days

        :param str sender: single sender
        :param str time_limit: time range in string
        :param int timestamp: timestamp to start
        :return: image file in `Resp.fileobj`
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, 'so much people'
        sender = sender[0]
        cond = (Tweet.user_id == int(sender)) & (Tweet.timestamp >= timestamp)
        rows = self.session.query(Tweet).filter(cond).order_by(Tweet.timestamp.desc())
        stamps = [r.timestamp for r in rows]
        tz = self.victims[sender]['timezone']
        if isinstance(tz, str):
            tz = timezone(tz)
        d = defaultdict(int)
        for s in stamps:
            s = datetime.fromtimestamp(s, tz)
            d[(s.weekday(), s.hour)] += 1
        plot_punchcard(d, self.victims[sender]['ref_screen_name'], time_limit)
        return Resp(fileobj=open('pc.png', 'rb'))

    def deleted(self, senders, *args, chat_id=None):
        """Show someone's deleted tweets. Usage is same as `/s[earch]`.
        """
        _senders = senders
        senders = self._to_sender(_senders)
        c = self.gen_search_cond(senders, args, Tweet)
        base = self.session.query(Tweet).filter(c.cond & Tweet.deleted)
        if c.desc:
            base = base.order_by(Tweet.timestamp.desc())
        else:
            base = base.order_by(Tweet.timestamp.asc())
        base = base.offset(c.offset)
        rows = base.limit(c.limit).all()
        cnt = base.count() - len(rows)
        arg = ' '.join(c.contains + ['-' + i for i in c.excludes])
        opt = '/deleted {} {} !c{}'.format(_senders, arg, c.limit)
        if not c.desc:
            opt += ' !<'
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
        if chat_id:
            self.cache[chat_id] = rows
        return Resp(message=msg, keyboard=keyboard, markdown=True)

    def ids(self, names):
        """Return ids for names

        :param names: List[str]
        :return: formatted message
        :rtype: string
        """
        ids = names2ids(names)
        msg = self.fmt.format_ids(names, ids)
        return Resp(message=msg, markdown=True)

    def help(self, cmd):
        '''Show usage of a command

        Example::

            /help trend
                show usage of command "trend"
            /help
                show all commands

        :param: str cmd: command name
        :return: usage explanation
        :rtype: Resp
        '''
        cmd2func = {
            'stat': self.get_stat,
            's': self.search, 'st': self.search,
            'watch': self.watch,
            'rand': self.rand, 'randt': self.rand,
            'sleep': self.sleep_time, 'cnt': self.count,
            'remember': self.remember, 'forget': self.forget,
            'f': self.f_status,
            'fs': self.fs,
            'quote': self.search_quote, 'randq': self.rand_quote,
            'freq': self.get_freq,
            'wordcloud': self.wordcloud,
            'help': self.help,
            'trend': self.trend,
            'thread': self.thread,
            'pc': self.punchcard, 'punchcard': self.punchcard,
            'deleted': self.deleted,
            'ids': self.ids,
            'bio': self.bio_changes
        }
        if cmd:
            try:
                msg = self.fmt.format_usage(cmd2func[cmd].__doc__)
                return Resp(message=msg, markdown=True)
            except KeyError:
                return Resp(message='No usage for {}'.format(cmd), markdown=True)
        else:
            cmdlist = Path(__file__).absolute().parent.parent / 'cmdlist.txt'
            resp = cmdlist.open().read() if cmdlist.exists() else 'baka!'
            return Resp(message=resp)

    def bio_changes(self, sender, time_limit, timestamp):
        ''' Bio changes in a time range.

        Example::

            /bio j
                show j's bio changes in 7 days
            /bio j 2w
                show j's bio changes in two weeks

        :param str sender: single sender
        :param str time_limit: time range in string
        :param int timestamp: timestamp to start
        :return: `Resp` object
        :rtype: Resp
        '''
        sender = self._to_sender(sender)
        assert len(sender) == 1, 'so much people'
        sender = sender[0]
        bios = self.session.query(Bio).filter(
            (Bio.bio['id'] == str(sender)) &
            (Bio.timestamp >= datetime.fromtimestamp(timestamp)))\
            .order_by(Bio.timestamp.desc()).all()

        # fetch one more bio to fit the time limit requirement of changes
        if bios:
            one_more = self.session.query(Bio).filter(
                (Bio.bio['id'] == str(sender)) &
                (Bio.timestamp < bios[-1].timestamp))\
                .order_by(Bio.timestamp.desc()).first()
            if one_more:
                bios.append(one_more)

        changes = []
        old = None
        bios.reverse()
        for b in bios:
            if old is None:
                old = b.bio
                continue
            new = b.bio
            for field in self.config['bio_all_fields']:
                if old[field] != new[field]:
                    changes.append((b.timestamp.timestamp(),
                                    field, old[field], new[field]))
            old = new
        changes.reverse()
        msg = self.fmt.format_bio(sender, time_limit, changes)
        return Resp(message=msg)

    def inline(self, mid, offset, query):
        ''' Inline search.

        :param str mid: unique identifier for the answered query
        :param str offset: offset in string
        :param str query: query string
        :return: `Resp` object
        :rtype: Resp
        '''
        try:
            senders, *qs = query.split()
            senders = self._to_sender(senders)
        except:
            return self.fmt.format_inline(mid, [], 0, 0)
        c = self.gen_search_cond(senders, qs, table=Tweet)
        base = self.session.query(Tweet).filter(c.cond)
        if c.desc:
            base = base.order_by(Tweet.timestamp.desc())
        else:
            base = base.order_by(Tweet.timestamp.asc())
        offset = int(offset)
        base = base.offset(offset)  # not c.offset
        twts = base.limit(c.limit).all()
        more = base.count() - len(twts)
        return self.fmt.format_inline(mid, twts, offset, more)
