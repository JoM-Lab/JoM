#!/usr/bin/env python3
import os
import matplotlib
matplotlib.use('agg')
from matplotlib.ticker import FuncFormatter, MaxNLocator
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import numpy as np
from scipy.ndimage import imread
from scipy.interpolate import spline
from wordcloud import WordCloud
import datetime

plt.xkcd()


def plot_trend(kws, cnts, norm_factors, xlbs, metadata):
    '''Trend of a set of keywords.

    :param List[str] kws: keywords
    :param cnts: counts of keywords
    :type cnts: List[List[int]]
    :param norm_factors: total number of tweets in each time period
    :type norm_factors: List[Int]
    :param List[str] xlbs: labels in x-axis
    :param metadata: authors and time range in str
    :type metadata: List[Object]
    '''
    fig, ax = plt.subplots()

    xs = np.arange(len(xlbs))
    plt.xticks(xs, xlbs)
    plt.xlim(xs[0]-0.1, xs[-1]+0.1)

    xlabels = ax.get_xticklabels()
    # limit the number of x-axis labels
    show_indices = set(range(len(xlabels))[::len(xlabels)//14+1])
    show_indices |= set([len(xlabels)-1])
    for i in range(len(xlabels)):
        if i not in show_indices:
            xlabels[i]._visible = False
        xlabels[i].set_rotation(45)

    # generate colors for each line
    colors = [plt.cm.jet(i) for i in np.linspace(0.1, 0.9, len(kws))]
    for kw, cnt, color in zip(kws, cnts, colors):
        xs_new = np.linspace(min(xs), max(xs), 100)
        cnt_normalized = [c / nf if nf != 0 else 0
                          for c, nf in zip(cnt, norm_factors)]
        # make it smooth
        cnt_smooth = spline(xs, cnt_normalized, xs_new)
        cnt_smooth[cnt_smooth < 0] = 0  # no negative
        # limit the word length in legend box
        kw = '{}...'.format(kw[:7]) if len(kw) > 10 else kw
        ax.plot(xs_new, cnt_smooth, '-', label=kw, color=color)

    # place the figure on the left
    box = ax.get_position()
    ax.set_position([box.x0 - 0.03, box.y0 + 0.15,
                     box.width * 0.8, box.height * 0.8])
    # place the legend on the right
    ax.legend(bbox_to_anchor=(1.05, 1.), loc=2, borderaxespad=0.,
              prop=fm.FontProperties(fname=fm.findfont('Source Han Sans CN')))
    plt.title('{} ngram in {}/{}'.format(', '.join(metadata[0]), *(metadata[1:])))
    fig.savefig("trend.png")


def gen_freq(ts):
    '''Histogram of each day's tweet count.

    :param ts: date time and count
    :type ts: List[(str, int)]
    '''
    fig, ax = plt.subplots()
    dt, cnt = zip(*ts)
    xs = np.arange(len(ts))
    ax.bar(xs-0.4, cnt, color='#ddffff')
    plt.xticks(xs, ['{1}-{2}'.format(*i) for i in dt])
    xlabels = ax.get_xticklabels()
    # limit the number of labels on x-axis
    show_indices = set(range(len(xlabels))[::len(xlabels)//14+1])
    show_indices |= set([len(xlabels)-1])
    for i in range(len(xlabels)):
        if i not in show_indices:
            xlabels[i]._visible = False
        xlabels[i].set_rotation(90)
        # make the weekend's label red
        weekday = datetime.datetime(*dt[i]).weekday()
        if weekday == 5 or weekday == 6:
            xlabels[i].set_color('red')
    tot = sum(cnt)
    avg = tot / len(cnt)
    plt.title('total: {}   avg: {:.2f}'.format(tot, avg))
    plt.savefig("freq.png")


def gen_sleep_png(ts):
    '''Histgram od sleep duration and trend of sleep/wake time.

    :param ts: wake and sleep time
    :type ts: List[(datetime, datetime)]
    '''
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    ax1.yaxis.tick_right()
    ax2.yaxis.tick_left()
    x = [b.strftime("%m-%d") for a, b in ts]
    xi = np.arange(len(x))
    # fix the order of time
    y1 = [(a.hour - 24 if a.hour >= 20 else a.hour) + a.minute / 60. for a, b in ts]
    y2 = [(b.hour - 24 if b.hour >= 20 else b.hour) + b.minute / 60. for a, b in ts]
    ax1.bar([i-0.4 for i in xi], [j-i for i, j in zip(y1, y2)], color='#eeffff')
    ax2.plot(xi, y1, 'o-b')
    ax2.plot(xi, y2, 'o-b')
    # custom the y-axis labels
    start, end = ax2.get_ylim()
    ax2.yaxis.set_ticks(np.arange(start, end, 1))
    ax2.yaxis.set_major_formatter(FuncFormatter(lambda y, pos: int(y if y >= 0 else y + 24)))
    plt.xticks(xi, x)
    plt.xlim(xi[0]-0.5, xi[-1]+0.5)
    labels = ax1.get_xticklabels()
    # limit the number of x-axis labels
    show_indices = set(range(len(labels))[::len(labels)//14+1])
    # always show the last one
    show_indices.add(len(labels)-1)
    for i in range(len(labels)):
        if i not in show_indices:
            labels[i]._visible = False
        labels[i].set_rotation(90)
        # paint weekend red
        weekday = ts[i][1].weekday()
        if weekday == 5 or weekday == 6:
            labels[i].set_color('red')
    durs = [(b - a).total_seconds() for a, b in ts]
    last = durs[-1]/3600.0 if durs else float('NaN')
    avg = sum(durs)/len(durs)/3600.0 if durs else float('NaN')
    plt.title('last: {:.2f}h   avg: {:.2f}h'.format(last, avg))
    plt.savefig("sleep.png")


def gen_word_cloud(freqs):
    '''Word cloud!

    :param freqs: word and frequency
    :type freqs: List[(str, cnt)]
    '''
    # bash to the background image
    base_img = os.path.join(os.path.dirname(__file__), "uomi.png")
    # path to otf file
    font_path = fm.findfont('Source Han Sans CN')\
                  .replace('Regular', 'ExtraLight')
    wc = WordCloud(background_color="white", mask=imread(base_img),
                   font_path=font_path,
                   max_words=150, max_font_size=30, scale=2  #, random_state=42
                   ).generate_from_frequencies(freqs)
    wc.to_file("wordcloud.png")


def plot_punchcard(infos, sender, time_limit):
    '''Tweet frequency in every hour in every week day.

    :param infos: dictionary from (weekday, hour) to count
    :type infos: Dict[(int, int), int]
    :param str sender: the author
    :param str time_limit: time range in raw string
    '''
    fig, ax = plt.subplots()

    # make a 2D array
    data = np.zeros((7, 24))
    for key in infos:
        data[key[0], key[1]] = infos[key]
    data = data / float(np.max(data))

    for y in range(7):
        for x in range(24):
            # add circles
            ax.add_artist(plt.Circle((x, y), data[y, x]/2.4, color='gray'))

    plt.ylim(-0.7, 6.7)
    plt.xlim(-1, 24)
    # custom the y-axis labels
    weekdays = 'Monday Tuesday Wednesday Thursday Friday Saturday Sunday'.split()
    plt.yticks(range(7), weekdays)
    plt.xticks(range(24))
    plt.xlabel('Hour')
    plt.ylabel('Weekday')
    ax.invert_yaxis()
    scale = 0.6
    fig.set_size_inches(25*scale, 7.4*scale, forward=True)
    plt.title('punckcard for {} in {}'.format(sender, time_limit))
    plt.savefig('pc.png')

if __name__ == '__main__':
    ts = [(datetime.datetime(2015, 6, 21, 22, 55, 8), datetime.datetime(2015, 6, 22, 4, 21, 4)),
          (datetime.datetime(2015, 6, 23, 0, 10, 53), datetime.datetime(2015, 6, 23, 8, 33, 20)),
          (datetime.datetime(2015, 6, 24, 0, 58, 5), datetime.datetime(2015, 6, 24, 7, 26, 32)),
          (datetime.datetime(2015, 6, 25, 0, 16, 36), datetime.datetime(2015, 6, 25, 4, 48, 13)),
          (datetime.datetime(2015, 6, 26, 1, 42, 54), datetime.datetime(2015, 6, 26, 8, 1, 42)),
          (datetime.datetime(2015, 6, 27, 2, 43, 31), datetime.datetime(2015, 6, 27, 6, 57, 32)),
          (datetime.datetime(2015, 6, 28, 2, 43, 31), datetime.datetime(2015, 6, 28, 6, 57, 32)),
          (datetime.datetime(2015, 6, 29, 2, 43, 31), datetime.datetime(2015, 6, 29, 6, 57, 32)),
          (datetime.datetime(2015, 6, 30, 2, 43, 31), datetime.datetime(2015, 6, 30, 6, 57, 32))]
    #gen_sleep_png(ts)
    gen_freq([((2015, 7, 11), 120), ((2015, 7, 12), 37), ((2015, 7, 13), 71), ((2015, 7, 14), 153),
              ((2015, 7, 15), 94), ((2015, 7, 16), 0), ((2015, 7, 17), 9)])
