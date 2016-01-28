import time
from itertools import takewhile


class Parser:
    """ Parse coming request. """

    def __init__(self):
        self.last_timestamp = 0

    def parse(self, msg):
        trunks = msg.split()
        if not trunks:
            raise Exception('no input')
        elif trunks[0] == 'stat':
            return 'stat', self.parse_timed(trunks, default='24h')
        elif trunks[0] == 's':
            return 'search', self.parse_n_more(2)(trunks)
        elif trunks[0] == 'st':
            return 'search_original', self.parse_n_more(2)(trunks)
        elif trunks[0] == 'watch':
            return 'watch', self.parse_n_more(2)(trunks)
        elif trunks[0] == 'rand':
            return 'rand', self.parse_n(2)(trunks)
        elif trunks[0] == 'randt':
            return 'rand_original', self.parse_n(2)(trunks)
        elif trunks[0] == 'sleep':
            return 'sleep', self.parse_timed(trunks, default='7d')
        elif trunks[0] == 'cnt':
            return 'count', self.parse_cnt(trunks)
        elif trunks[0] == 'end':
            return 'end', None
        elif trunks[0] == 'config':
            return 'config', None
        elif trunks[0] == 'remember' or trunks[0] == 'rem':
            author, *qs = self.parse_n_more(3)(trunks)
            return 'remember', (author, ' '.join(qs))
        elif trunks[0] == 'forget':
            return 'forget', self.parse_n_more(2)(trunks)
        elif trunks[0] == 'f':
            return 'f', self.parse_f(trunks, default='1d')
        elif trunks[0] == 'fs':
            return 'fs', self.parse_n_more(2)(trunks)
        elif trunks[0] == 'quote':
            return 'quote', self.parse_n_more(2)(trunks)
        elif trunks[0] == 'randq':
            return 'randq', self.parse_n(2)(trunks)
        elif trunks[0] == 'freq':
            return 'freq', self.parse_timed(trunks, default='7d')
        elif trunks[0] == 'wordcloud':
            return 'wordcloud', self.parse_n(2)(trunks)
        elif trunks[0] == 'help':
            return 'help', self.parse_help(trunks)
        elif trunks[0] == 'trend':
            return 'trend', self.parse_trend(trunks)
        elif trunks[0] == 'thread':
            return 'thread', self.parse_n(2)(trunks)
        elif trunks[0] == 'pc' or trunks[0] == 'punchcard':
            return 'punchcard', self.parse_timed(trunks, default='7d')
        elif trunks[0] == 'deleted':
            return 'deleted', self.parse_n_more(2)(trunks)
        elif trunks[0] == 'ids':
            return 'ids', self.parse_n_more(2)(trunks)
        elif trunks[0] == 'bio':
            return 'bio', self.parse_timed(trunks, default='7d')
        else:
            raise Exception('no cmd ' + trunks[0])

    @staticmethod
    def unit_to_seconds(unit):
        """Turn time unit to seconds

        :param string unit: 'h', 'd', 'w', or 'm', standing for
                            'hour', 'day', 'week', and 'month'.
        :return: seconds
        :rtype: int
        """
        if unit == 'h':
            return 3600
        elif unit == 'd':
            return 86400
        elif unit == 'w':
            return 604800
        elif unit == 'm':
            return 2592000
        else:
            raise Exception('invalid time unit')

    def to_seconds(self, time_limit):
        """ Turn a time limit into seconds

        :param string time_limit: time limit, e.g. '7d', '2w', '3m'
        :return: seconds
        :rtype: int
        """
        amt, unit = int(time_limit[:-1]), time_limit[-1]
        return amt * self.unit_to_seconds(unit)

    def parse_limit(self, time_limit):
        """Obtaining lower bound timestamp from time limit
        This is useful when one wants to get tweets in a certain period of time.
        For example, if ``time_limit`` is '1w', we get lower bound
        timestamp 1444541999. Desired time range is then [1444541999, now]

        :param string time_limit: time limit of type string, e.g. '7d', '2w', '3m'
        :return: lower bound timestamp of type int
        :rtype: int
        """
        timestamp = int(time.time())
        return timestamp - self.to_seconds(time_limit)

    @staticmethod
    def filter_config(trunks, allowed):
        """Separate regular command argument and config
        For example, for command "/f j 7d !unfo",
        "f", "j", "7d" are regular command arguments and
        "!unfo" is config. In this case, we only want result of unfo.


        :param List[str] trunks: list of trunks of command
        :param allowed: list of allowed config flag, e.g unfo, fo
        :type allowed: string|List[str]
        :return: list of regular command trunks, config detail dictionary
        :rtype: List[str] * dict
        """
        allowed = allowed.split() if isinstance(allowed, str) else allowed
        args = [t for t in trunks if t[0] != '!']
        cfgs = [t[1:] for t in trunks if t[0] == '!']
        cfg_dict = {}
        for cfg in cfgs:
            # ``cfg`` can have form of a pure character string
            # or a character string with some digits attached behind.
            # For example, 'unfo' and 'p7' are valid config
            name = ''.join(takewhile(lambda c: c.isalpha(), cfg))
            # config should be in allowed config list
            assert name in allowed, "config not allowed: " + name
            num = cfg[len(name):]
            # if no digit attached at the end, we treat it as boolean flag
            if not num:
                cfg_dict[name] = True
            # if there are digits attached, associate digits in integer with config
            elif num.isdigit():
                cfg_dict[name] = int(num)
            else:
                raise Exception("config `{}` is not number".format(name))
        return args, cfg_dict

    def parse_timed(self, trunks, *, default='24h'):
        """Parse command involving time range

        :param List[str] trunks: list of trunks of command
        :param string default: default time range, which is 24 hours
        :return: a tuple containing username of type string, time range of type string,
               timestamp of lower-bound time range
        :rtype: string * string * int
        """
        if len(trunks) == 2:
            return trunks[1], default, self.parse_limit(default)
        elif len(trunks) == 3:
            if trunks[2][:-1].isdigit() and trunks[2][-1] in 'hdwm':
                return trunks[1], trunks[2], self.parse_limit(trunks[2])
            else:
                raise Exception('bad time format')
        else:
            raise Exception('bad parameters')

    def parse_trend(self, trunks):
        """Parse trend command.
        For example, for command "/trend j 2w 1w a b c",
        "2w" is the time range we want to query,
        "1w" is the interval or granularity.
        "a", "b", "c" are keywords.
        This command means plotting graph showing trend
        of tweets containing "a", "b", "c" in the past two weeks with
        time interval of one week.
        In this case, two integers are associated with two weeks respectively
        for each pattern. If interval is '2d', then 7 integers are
        generated for seven 2-days for each pattern.

        Default time range and time interval are '3m' and '2w'.

        :param List[str] trunks: list of trunks of command
        :return: a tuple containing username of type string, time range
                 in seconds, time interval in seconds, list of keywords,
                 time range and interval of type string
        :rtype: string * int * int * List[str] * List[str]
        """
        try:
            if len(trunks) > 4:
                time_limit = self.parse_limit(trunks[2])
                time_interval = self.parse_limit(trunks[3])
                if time_interval < time_limit:
                    raise Exception("Bad time interval")
                else:
                    return trunks[1], \
                        self.to_seconds(trunks[2]), \
                        self.to_seconds(trunks[3]), \
                        trunks[4:], [trunks[2], trunks[3]]
            else:
                raise Exception('not enough parameters')
        except Exception:
            if len(trunks) > 2:
                return trunks[1], \
                    self.to_seconds('3m'), self.to_seconds('2w'), \
                    trunks[2:], ['3m', '2w']
            else:
                raise Exception('not enough parameters')

    def parse_f(self, trunks, *, default='24h'):
        """Parser f command
        A valid command can be "/f j 7d !unfo !fo !p2"

        :param List[str] trunks: list of trunk of command
        :param str default: default time range, which is 24 hours
        :return: a tuple of result of ``parse_timed`` concatenated with config dictionary
        :rtype: string * string * int * dict
        """
        trunks, cfg = self.filter_config(trunks, 'p fo unfo foed unfoed')
        res = self.parse_timed(trunks, default=default)
        return res + (cfg,)

    @staticmethod
    def parse_cnt(trunks):
        """Parse cnt command
        A valid command can be "/cnt j -kw_no kw1 kw2".
        Keywords starting with minus sign are excluded keywords

        :param List[str] trunks: list of trunks of command
        :return: list of included and excluded keywords
        :rtype: List[str]
        """
        if len(trunks) >= 3:
            if any(s[0] != '-' for s in trunks[2:]):
                return trunks[1:]
            else:
                raise Exception('no positive query term')
        else:
            raise Exception('not enough parameters')

    @staticmethod
    def parse_n(n):
        """Given a list of trunks of length n, return the trunk at index 1
        Usage: ``parse_n(2)(['a', 'b'])`` returns 'b'

        :param int n: desired length of trunks
        :return: a function that gets trunk at given index
        :rtype: function
        """
        def func(trunks):
            if len(trunks) == n:
                return trunks[1]
            else:
                raise Exception('need {} parameters'.format(n))
        return func

    @staticmethod
    def parse_n_more(n):
        """Given a list of length at least n, return trunks including and after index 1
        Usage: ``parse_n_more(3)(['a', 'b', 'c', 'd'])`` returns ['b', 'c', 'd'].

        :param int n: least length of list
        :return: trunks
        :rtype: List[str]
        """
        def func(trunks):
            if len(trunks) >= n:
                return trunks[1:]
            else:
                raise Exception('not {} and more parameters'.format(n))
        return func

    @staticmethod
    def parse_help(trunks):
        '''Parse help command
        A valid help could be "/help trend" or "/help"

        :param trunks: List[str] trunks: list of trunks of command
        :return: command name or None if user wants to list all commands
        :rtype: str | None
        '''
        if len(trunks) <= 2:
            if len(trunks) == 1:
                return None
            else:
                return trunks[1]
        else:
            raise Exception('too many parameters')
