import time


class Parser:
    """ Parse coming request """

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
        elif trunks[0] == 'say':
            return 'say', ' '.join(self.parse_n_more(2)(trunks))
        elif trunks[0] == 'remember':
            author, *qs = self.parse_n_more(3)(trunks)
            return 'remember', (author, ' '.join(qs))
        elif trunks[0] == 'forget':
            return 'forget', self.parse_n_more(2)(trunks)
        elif trunks[0] == 'ff':
            return 'ff', self.parse_timed(trunks, default='1d')
        elif trunks[0] == 'fff':
            return 'fff', self.parse_fff(trunks, default='1d')
        elif trunks[0] == 'quote':
            return 'quote', self.parse_n_more(2)(trunks)
        elif trunks[0] == 'randq':
            return 'randq', self.parse_n(2)(trunks)
        elif trunks[0] == 'freq':
            return 'freq', self.parse_timed(trunks, default='7d')
        elif trunks[0] == 'wordcloud':
            return 'wordcloud', self.parse_n(2)(trunks)
        elif trunks[0] == 'help':
            return 'help', None
        else:
            raise Exception('no cmd ' + trunks[0])

    @staticmethod
    def parse_limit(time_limit):
        amt, unit = int(time_limit[:-1]), time_limit[-1]
        timestamp = int(time.time())
        if unit == 'h':
            timestamp -= amt * 60 * 60
        elif unit == 'd':
            timestamp -= amt * 60 * 60 * 24
        elif unit == 'w':
            timestamp -= amt * 60 * 60 * 24 * 7
        elif unit == 'm':
            timestamp -= amt * 60 * 60 * 24 * 7 * 30
        else:
            raise Exception('wrong time unit')
        return timestamp

    def parse_timed(self, trunks, *, default='24h'):
        if len(trunks) == 2:
            return (trunks[1], default, self.parse_limit(default))
        elif len(trunks) == 3:
            if trunks[2][:-1].isdigit() and trunks[2][-1] in 'hdwm':
                return (trunks[1], trunks[2], self.parse_limit(trunks[2]))
            else:
                raise Exception('bad time format')
        else:
            raise Exception('bad parameters')

    def parse_fff(self, trunks, *, default='24h'):
        if len(trunks) == 2:
            return (trunks[1], default, self.parse_limit(default), 1)
        elif len(trunks) in (3, 4):
            if not (trunks[2][:-1].isdigit() and trunks[2][-1] in 'hdwm'):
                raise Exception('bad time format: {}'.format(trunks[2]))
            if len(trunks) == 3:
                pg = 1
            elif len(trunks) == 4:
                if not trunks[3][:2] == '!p' and trunks[3][2:].isdigit():
                    raise Exception('bad page config: {}'.format(trunks[3]))
                else:
                    pg = int(trunks[3][2:])
            return (trunks[1], trunks[2], self.parse_limit(trunks[2]), pg)
        else:
            raise Exception('bad parameters')

    def parse_cnt(self, trunks):
        if len(trunks) >= 3:
            if any(s[0] != '-' for s in trunks[2:]):
                return trunks[1:]
            else:
                raise Exception('no positive query term')
        else:
            raise Exception('not enough parameters')

    @staticmethod
    def parse_n(n):
        def func(trunks):
            if len(trunks) == n:
                return trunks[1]
            else:
                raise Exception('need {} parameters'.format(n))
        return func

    @staticmethod
    def parse_n_more(n):
        def func(trunks):
            if len(trunks) >= n:
                return trunks[1:]
            else:
                raise Exception('not {} and more parameters'.format(n))
        return func
