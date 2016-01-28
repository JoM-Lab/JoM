#!/usr/bin/env python3
# coding: utf-8
from __future__ import unicode_literals, print_function
from collections import Counter
from .db import new_session, Tweet
import string
import re

zh = re.compile(r"[\u4e00-\u9fa5]+")
other = re.compile(r"[a-zA-Z0-9_]+")
bad = ("_ 1 2 3 4 5 I O RT The a and are be bit co com for gt http https html in "
       "is it jpg ly me media my not of on org p pbs png r s status t that the this to "
       "twimg via www you").split()
alphanum = set(string.ascii_letters + string.digits)


def freq(texts, limit=400, p=12):
    '''Find the most frequent words and their frequencies in a set of texts.

    :param texts: a list of strings
    :type texts: List[str]
    :param int limit: a soft limit to cut off some words
    :param int p: longest word length
    :return: list of words and frequencies
    :rtype: List[(str, int)]
    '''
    cnt = Counter()
    cntE = Counter()
    for l in texts:
        # find all Chinese
        for w in zh.findall(l):
            saw = set()
            for ln in range(2, p+1):  # all length
                for i in range(len(w)-(ln-1)):  # all start pos
                    saw.add(w[i:i+ln].strip('的').lstrip('了'))
            for v in saw:
                cnt[v] += 1
        # English words and digits
        for w in other.findall(l):
            cntE[w] += 1
    for w in bad:
        cntE[w] = 0  # remove
    # find top `limit` ones
    freqs = cnt.most_common(limit)
    # initialize results as top 10 English words & digits
    results = list(cntE.most_common(10))
    # already in results
    filt = Counter()
    # from longest to shortest
    for ln in range(p, 1, -1):
        # find all with this length but a part of longer ones
        cur = [(k, v) for k, v in freqs if len(k) == ln]
        # filter some with `filt`
        cur = [(k, v) for k, v in cur if k not in filt or filt[k] * 2 < v]
        cur.sort(key=lambda t: t[1], reverse=True)
        results.extend(cur)
        # put all parts into `filt`
        for w, v in cur:
            for l in range(2, ln):
                for i in range(len(w)-(l-1)):
                    filt[w[i:i+l]] += v
    # print(results)
    return results

if __name__ == '__main__':
    session = new_session()
    texts = [i.text for i in session.query(Tweet.text).filter(Tweet.sender == 'masked')]
    cnt = freq(texts)
    print(sorted([(v, k) for k, v in cnt], reverse=True))
