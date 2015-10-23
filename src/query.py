# coding: utf-8
from sqlalchemy import func
from collections import namedtuple, defaultdict
from pytz import timezone
from datetime import datetime, timedelta
from db import new_session, Tweet, Quote, Follow
from format import Formatter
from freq import freq
from visualize import gen_sleep_png, gen_freq, gen_word_cloud
from parser import Parser
from functools import reduce
from pathlib import Path
import json
import operator
import requests


class Query:

    def __init__(self, config_file='config.json'):
        if type(config_file) is str:
            self.config = json.load(open(config_file))
        else:
            self.config = config_file
        self.session = new_session(self.config['db_file'])
        self.return_limit = self.config['return_limit']
        self.victims = self.config['victims']
        self.fmt = Formatter(self.victims)
        self.parser = Parser()
        self.shortcuts = {}
        for name in self.victims:
            u = self.victims[name]
            for s in u['shortcuts'].split():
                self.shortcuts[s] = name
        self.selective = False
        self.hide_keyboard = {"hide_keyboard": True,
                              "selective": self.selective}

    def query(self, msg):
        no_preview = True
        try:
            parse_result = self.parser.parse(msg)
            tp, args = parse_result
            if tp == 'stat':
                return self.get_stat(*args), self.hide_keyboard, no_preview
            if tp == 'freq':
                return self.get_freq(*args), self.hide_keyboard, no_preview
            elif tp == 'search':
                rows, cnt, keyboard = self.search(*args)
                no_preview = not (len(rows) == 1 and 'twimg' in rows[0].text)
                return self.fmt.format_search(rows, cnt), keyboard, no_preview
            elif tp == 'search_original':
                rows, cnt, keyboard = self.search(*args, orig_only=True)
                no_preview = not (len(rows) == 1 and 'twimg' in rows[0].text)
                return self.fmt.format_search(rows, cnt), keyboard, no_preview
            elif tp == 'watch':
                return self.watch(*args), self.hide_keyboard, no_preview
            elif tp == 'rand':
                l, keyboard = self.rand(args)
                return l, keyboard, False
            elif tp == 'rand_original':
                l, keyboard = self.rand(args, orig_only=True)
                return l, keyboard, False
            elif tp == 'sleep':
                return self.sleep_time(*args), self.hide_keyboard, no_preview
            elif tp == 'count':
                return self.count(*args), self.hide_keyboard, no_preview
            elif tp == 'end':
                return '.', self.hide_keyboard, no_preview
            elif tp == 'config':
                return self.fmt.format_config(), self.hide_keyboard, no_preview
            elif tp == 'say':
                self.gen_speech(args)
                return open('say.mp3', 'rb'), self.hide_keyboard, no_preview
            elif tp == 'quote':
                return self.search_quote(*args), self.hide_keyboard, no_preview
            elif tp == 'randq':
                return self.rand_quote(args), self.hide_keyboard, no_preview
            elif tp == 'remember':
                return self.remember(*args), self.hide_keyboard, no_preview
            elif tp == 'forget':
                return self.forget(*args), self.hide_keyboard, no_preview
            elif tp == 'ff':
                return self.ff_status(*args), self.hide_keyboard, no_preview
            elif tp == 'fff':
                resp, keyboard = self.fff_status(*args)
                return resp, keyboard, no_preview
            elif tp == 'help':
                cmdlist = Path(__file__).absolute().parent.parent / 'cmdlist.txt'
                resp = cmdlist.open().read() if cmdlist.exists() else 'baka!'
                return resp, self.hide_keyboard, no_preview
            elif tp == 'wordcloud':
                return self.wordcloud(args), self.hide_keyboard, no_preview
            else:
                raise Exception('no cmd ' + tp)
        except Exception as e:
            # raise  # for debug
            return str(e), self.hide_keyboard, True

    def watch(self, sender, action=None, *kws):
            sender = self._to_sender(sender)
            assert len(sender) == 1, "so many people"
            sender = sender[0]
            keywords = json.load(open('keywords.json'))
            if sender not in keywords:
                keywords[sender] = []

            if action == '+':
                assert kws, "no keywords provided"
                keywords[sender].append(kws)
                json.dump(keywords, open('keywords.json', 'w'))
            elif action == '-':
                assert kws, "no keywords provided"
                keywords[sender] = [k for k in keywords[sender]
                                    if set(k) != set(kws)]
                json.dump(keywords, open('keywords.json', 'w'))
            elif action != None:
                raise Exception("the second parameter should be +/-")

            return 'keywords for {}:\n{}'.format(
                sender, ', '.join('|'.join(l) for l in keywords[sender]))

    def gen_speech(self, sen):
        params = dict(tl='zh-CN', ie='UTF-8', q=sen)
        r = requests.get('https://translate.google.com/translate_tts',
                         params=params)
        with open('say.mp3', 'wb') as f:
            f.write(r.content)  # FIXME: maybe r.raw

    def _to_sender(self, sender):
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
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        cond = Tweet.sender == sender
        if orig_only:
            cond = cond & (Tweet.type == 'tweet')
        one = self.session.query(Tweet).filter(cond)\
                          .order_by(func.random()).first()
        if one is None:
            return "nothing indexed", self.hide_keyboard
        cmd = '/randt' if orig_only else '/rand'
        keyboard = {"keyboard": [[cmd + ' ' + sender], ['/end']],
                    "selective": self.selective,
                    "resize_keyboard": True}
        return self.fmt.format_rand(one), keyboard

    def gen_search_cond(self, senders, queries, table, *, orig_only=False):
        excludes = [par[1:] for par in queries if par[0] == '-']
        contains = [par for par in queries if par[0] != '-' and par[0] != '!']
        cfg = [par[1:] for par in queries if par[0] == '!']
        desc = True

        limit = self.return_limit
        page = 1
        idx = None
        nokbd = False

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

        return cond, (contains, excludes), (limit, page, offset, idx, nokbd, desc)

    def search(self, senders, *paras, orig_only=False, randone=False):
        _senders = senders
        senders = self._to_sender(senders)
        cond, args, cfg = self.gen_search_cond(senders, paras, table=Tweet,
                                               orig_only=orig_only)
        contains, excludes = args
        limit, page, offset, _, nokbd, desc = cfg
        base = self.session.query(Tweet).filter(cond)

        if desc:
            base = base.order_by(Tweet.timestamp.desc())
        else:
            base = base.order_by(Tweet.timestamp.asc())

        base = base.offset(offset)
        rows = list(base.limit(limit))
        cnt = base.count() - len(rows)

        cmd = '/st' if orig_only else '/s'
        arg = ' '.join(contains + ['-' + i for i in excludes])
        opt = '{} {} {} !c{}'.format(cmd, _senders, arg, limit)
        options = []
        if page != 1:
            options.append(['{} !p{}'.format(opt, page - 1)])
        if cnt:
            options.append(['{} !p{}'.format(opt, page + 1)])
        if not nokbd and options:
            keyboard = {"keyboard": options + [['/end']],
                        "selective": self.selective,
                        "resize_keyboard": True}
        else:
            keyboard = self.hide_keyboard
        return rows, cnt, keyboard

    def count(self, senders, *paras):
        senders = self._to_sender(senders)
        cond, args, cfg = self.gen_search_cond(senders, paras, Tweet)
        contains, excludes = args
        c = self.session.query(Tweet).filter(cond).count()
        return self.fmt.format_count(', '.join(senders), contains, excludes, c)

    def get_freq(self, sender, time_limit, timestamp):
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        tz = self.victims[sender]['timezone']
        cond = (Tweet.sender == sender) & (Tweet.timestamp >= timestamp)
        rows = self.session.query(Tweet).filter(cond).all()
        if not rows:
            return 'no data'
        cnt = defaultdict(int)
        for r in rows:
            t = datetime.fromtimestamp(r.timestamp, timezone(tz))
            cnt[(t.year, t.month, t.day)] += 1
        i = datetime(*min(cnt.keys()))
        mx = datetime(*max(cnt.keys()))
        while i != mx:
            cnt[(i.year, i.month, i.day)]
            i += timedelta(days=1)
        res = sorted(cnt.items(), key=lambda i: i[0])
        gen_freq(res)
        return open('freq.png', 'rb')

    def get_stat(self, sender, time_limit, timestamp):
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
            ttype = r.type
            if ttype == 'tweet':
                tweet_num += 1
            if ttype == 'reply':
                reply_num += 1
            if ttype == 'rt':
                rt_num += 1
            if ttype == 'quote':
                quote_num += 1
        details = [tweet_num, reply_num, rt_num, quote_num,
                   total, indexed, since_ts]
        return self.fmt.format_stat(sender, time_limit, details)

    def sleep_time(self, sender, time_limit, timestamp):
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        tz = self.victims[sender]['timezone']
        cond = (Tweet.sender == sender) & (Tweet.timestamp >= timestamp)
        rows = self.session.query(Tweet).filter(cond)\
                           .order_by(Tweet.timestamp.asc())
        tt = namedtuple('TimeTweet', 'time text')
        tts = [tt(datetime.fromtimestamp(r.timestamp, timezone(tz)), r.text)
               for r in rows]
        sleep_intervals = []
        last = None
        for this in tts:
            if last and (22 <= last.time.hour or last.time.hour <= 5) and\
                    (3 <= this.time.hour <= 12) and\
                    (3.5 * 3600 <= (this.time - last.time).total_seconds() <= 16 * 3600) and\
                    (not sleep_intervals or
                     (last.time - sleep_intervals[-1][1].time).total_seconds() >= 10 * 3600):
                sleep_intervals.append((last, this))
            last = this
        # FOR DEBUG
        # formatter = lambda t: t.strftime('%m/%d %H:%M:%S')
        # for (last, this) in sleep_intervals:
        #     print('sleep {} {}'.format(formatter(last.time), repr(last.text)))
        #     print('wake  {} {}\n'.format(formatter(this.time), repr(this.text)))
        res = [(sleep.time, wake.time) for (sleep, wake) in sleep_intervals]
        if not res:
            return 'no data'
        gen_sleep_png(res)
        return open('sleep.png', 'rb')

    def search_quote(self, sender, *qs):
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        cond, arg, cfg = self.gen_search_cond(sender, qs, Quote)
        limit, _, offset, _, _, desc = cfg
        base = self.session.query(Quote).filter(cond)
        if desc:
            base = base.order_by(Quote.timestamp.desc()).offset(offset)
        else:
            base = base.order_by(Quote.timestamp.asc()).offset(offset)
        rows = list(base.limit(limit))
        cnt = base.count() - len(rows)
        return self.fmt.format_quote(rows, cnt)

    def rand_quote(self, sender):
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        person = sender[0]
        cond = Quote.person == person
        row = self.session.query(Quote).filter(cond)\
                          .order_by(func.random()).first()
        return self.fmt.format_quote([row], 0)

    def remember(self, sender, quote):
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        person = sender[0]
        record = Quote(timestamp=datetime.utcnow().timestamp(),
                       person=person, text=quote)
        self.session.add(record)
        self.session.commit()
        return "I remembered."

    def forget(self, sender, *qs):
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        cond, arg, cfg = self.gen_search_cond(sender, qs, Quote)
        limit, _, offset, idx, _, _ = cfg
        quos = self.session.query(Quote).filter(cond)\
                           .order_by(Quote.timestamp.desc())\
                           .offset(offset).limit(limit).all()
        if len(quos) == 0:
            return "nothing found"
        if len(quos) > 1 and idx is None:
                return self.fmt.format_quote(quos, 0) + '-' * 25 +\
                       "\nuse !iX as the index of quotation to forget"
        else:  # len(quos) == 1 or (len(quos) > 1 and idx is not None)
            if len(quos) == 1:
                q = quos[0]
            else:
                assert 1 <= idx <= len(quos), "bad index"
                q = quos[idx - 1]
            self.session.delete(q)
            self.session.commit()
            return "deleted quotation of {}: “{}”".format(sender[0], q.text)

    def fff_status(self, sender, time_limit, timestamp, page):
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        cond = (Follow.person == sender) & (Follow.timestamp >= timestamp)
        page = max(page, 1)
        offset = (page - 1) * 30
        base = self.session.query(Follow).filter(cond)\
                           .order_by(Follow.timestamp.desc())\
                           .offset(offset)
        rows = base.limit(30).all()
        cnt = base.count() - len(rows)
        options = []
        if page != 1:
            options.append(['/fff {} {} !p{}'.format(
                           sender, time_limit, page - 1)])
        if cnt:
            options.append(['/fff {} {} !p{}'.format(
                           sender, time_limit, page + 1)])
        if options:
            keyboard = {"keyboard": options + [['/end']],
                        "selective": self.selective,
                        "resize_keyboard": True}
        else:
            keyboard = self.hide_keyboard
        return (self.fmt.format_fff(sender, time_limit, rows, cnt),
                keyboard)

    def ff_status(self, sender, time_limit, timestamp):
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        cond = (Follow.person == sender) & (Follow.timestamp >= timestamp)
        rows = self.session.query(Follow).filter(cond)\
                           .order_by(Follow.timestamp.desc())
        unfo, fo, unfoed, foed = [], [], [], []
        for r in rows:
            action = r.action
            if action == 'unfo':
                unfo.append(r)
            elif action == 'fo':
                fo.append(r)
            elif action == 'unfoed':
                unfoed.append(r)
            elif action == 'foed':
                foed.append(r)
            else:
                raise Exception("Unknown action: " + action)
        dup_fo_ids = {u.target_id for u in unfo} & {u.target_id for u in fo}
        fo = [u for u in fo if u.target_id not in dup_fo_ids]
        unfo = [u for u in unfo if u.target_id not in dup_fo_ids]
        dup_foed_ids = {u.target_id for u in unfoed} & {u.target_id for u in foed}
        foed = [u for u in foed if u.target_id not in dup_foed_ids]
        unfoed = [u for u in unfoed if u.target_id not in dup_foed_ids]
        return self.fmt.format_ff(sender, time_limit, unfo=unfo, fo=fo,
                                  unfoed=unfoed, foed=foed)

    def wordcloud(self, sender):
        sender = self._to_sender(sender)
        assert len(sender) == 1, "so many people"
        sender = sender[0]
        texts = self.session.query(Tweet.text).filter(Tweet.sender == sender)
        gen_word_cloud(freq(i.text for i in texts))
        return open('wordcloud.png', 'rb')
