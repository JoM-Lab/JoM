#!/usr/bin/env python3
import os
import matplotlib
matplotlib.use('agg')
from matplotlib.ticker import FuncFormatter
import matplotlib.pyplot as plt
import numpy as np
from scipy.ndimage import imread
from wordcloud import WordCloud


def gen_freq(ts):
    plt.xkcd()
    fig, ax = plt.subplots()
    dt, cnt = zip(*ts)
    labels = ['{1}-{2}'.format(*i) for i in dt]
    xs = np.arange(len(ts))
    ax.bar(xs-0.4, cnt, color='#ddffff')
    plt.xticks(xs, labels)
    for label in ax.get_xticklabels():
        label.set_rotation(90)
    tot = sum(cnt)
    avg = tot / len(cnt)
    plt.title('total: {}   avg: {:.2f}'.format(tot, avg))
    plt.savefig("freq.png")


def gen_sleep_png(ts):
    '''
    input: [(datetime, datetime)]
    output: sleep.png
    '''
    plt.xkcd()
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    ax1.yaxis.tick_right()
    ax2.yaxis.tick_left()
    x = [b.strftime("%m-%d") for a, b in ts]
    xi = np.arange(len(x))
    y1 = [(a.hour - 24 if a.hour >= 20 else a.hour) + a.minute / 60. for a, b in ts]
    y2 = [(b.hour - 24 if b.hour >= 20 else b.hour) + b.minute / 60. for a, b in ts]
    ax1.bar([i-0.4 for i in xi], [j-i for i, j in zip(y1, y2)], color='#eeffff')
    ax2.plot(xi, y1, 'o-b')
    ax2.plot(xi, y2, 'o-b')
    start, end = ax2.get_ylim()
    ax2.yaxis.set_ticks(np.arange(start, end, 1))
    ax2.yaxis.set_major_formatter(FuncFormatter(lambda y, pos: int(y if y >= 0 else y + 24)))
    plt.xticks(xi, x)
    plt.xlim(xi[0]-0.5, xi[-1]+0.5)
    for label in ax1.get_xticklabels():
        label.set_rotation(90)
    durs = [(b - a).total_seconds() for a, b in ts]
    last = durs[-1]/3600.0 if durs else float('NaN')
    avg = sum(durs)/len(durs)/3600.0 if durs else float('NaN')
    plt.title('last: {:.2f}h   avg: {:.2f}h'.format(last, avg))
    plt.savefig("sleep.png")


def gen_word_cloud(freqs):
    base_img = os.path.join(os.path.dirname(__file__), "uomi.png")
    wc = WordCloud(background_color="white", mask=imread(base_img),
                   font_path=os.path.join(os.path.dirname(__file__), "c.otf"),
                   max_words=150, max_font_size=30, scale=2  #, random_state=42
                   ).generate_from_frequencies(freqs)
    wc.to_file("wordcloud.png")

if __name__ == '__main__':
    import datetime
    ts = [(datetime.datetime(2015, 6, 21, 22, 55, 8), datetime.datetime(2015, 6, 22, 4, 21, 4)),
          (datetime.datetime(2015, 6, 23, 0, 10, 53), datetime.datetime(2015, 6, 23, 8, 33, 20)),
          (datetime.datetime(2015, 6, 24, 0, 58, 5), datetime.datetime(2015, 6, 24, 7, 26, 32)),
          (datetime.datetime(2015, 6, 25, 0, 16, 36), datetime.datetime(2015, 6, 25, 4, 48, 13)),
          (datetime.datetime(2015, 6, 26, 1, 42, 54), datetime.datetime(2015, 6, 26, 8, 1, 42)),
          (datetime.datetime(2015, 6, 27, 2, 43, 31), datetime.datetime(2015, 6, 27, 6, 57, 32))]
    gen_sleep_png(ts)
    gen_freq([((2015, 7, 12), 37), ((2015, 7, 13), 71), ((2015, 7, 14), 153),
              ((2015, 7, 15), 94), ((2015, 7, 16), 0), ((2015, 7, 17), 9)])
