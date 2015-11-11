#!/usr/bin/env python3
import os
import re
import sys
import random
import string
import marshal
from collections import defaultdict
from db import Tweet, new_session

sep = set(string.ascii_letters + string.digits + '@#')


def init(k=4):
    from snownlp import SnowNLP

    all_tweets = []
    session = new_session('./data.db')
    for i in session.query(Tweet.text)\
            .filter(Tweet.sender == 'user')\
            .filter(Tweet.type == 'tweet'):
        text = i.text
        text = re.sub('\n', ' ', text)
        text = re.sub('"', ' ', text)
        # remove all links
        text = re.sub('https?://[^ ]*', '', text)
        text = text.strip()
        if not text:
            continue
        all_tweets.append(text)
    n = len(all_tweets)
    print('total number of tweets:', n)

    nxt = defaultdict(lambda: defaultdict(int))
    last = 0
    for cnt, t in enumerate(all_tweets):
        if cnt - last > n * 0.01:
            sys.stdout.write('\rprogress: {:.0f}%'.format(100.0 * cnt / n))
            last = cnt

        t = [(None, 'START')] + list(SnowNLP(t).tags) + [(None, 'END')]
        # less than k
        for i in range(1, min(k, len(t))):
            # tags = [w[1] for w in t[:i]]
            pre = [w[1] for w in t[:i-1]] + [t[i-1][0]]
            nxt[tuple(pre)][t[i]] += 1
        # every k continous words
        for i in range(len(t)-k):
            # previous k-1 tags, last word
            pre = [w[1] for w in t[i:i+k-1]] + [t[i+k-1][0]]
            nxt[tuple(pre)][t[i+k]] += 1

    for k in nxt:
        nxt[k] = list(nxt[k].items())
    return dict(nxt)


def weightedChoice(items_weights):
    selection = None
    total_weight = 0.0
    for item, weight in items_weights:
        total_weight += weight
        if random.random() * total_weight < weight:
            selection = item
    return selection


def gen(nxt, k=4):
    s = [(None, 'START')]
    while 1:
        tp = s[-k:]
        l = len(tp)
        # previous k-1 tags, last word
        pre = [w[1] for w in tp[:l-1]] + [tp[l-1][0]]
        pre = tuple(pre)
        if pre not in nxt:
            break
        w = weightedChoice(nxt[pre])
        if w[1] == 'END':
            break
        s.append(w)
    # sys.stderr.write(str(s[1:]))
    res = ''
    for w, _ in s[1:]:
        if res and res[-1] in sep and w[0] in sep:
            res += ' '
        res += w
    return res

if __name__ == '__main__':
    if not os.path.exists('nxt.marshal'):
        print('need 5 min to train...')
        marshal.dump(init(), open('nxt.marshal', 'wb'))
    nxt = marshal.load(open('nxt.marshal', 'rb'))
    while 1:
        print(gen(nxt))
