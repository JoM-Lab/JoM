import json
from requests.exceptions import ConnectionError
from rauth import OAuth1Session

_twitter = None


def twitter():
    """Return a session to call twitter api.

    :return: a requests session
    :rtype: OAuth1Session
    """
    global _twitter
    if _twitter is None:
        globals().update(json.load(open('secret.json')))
        _twitter = OAuth1Session(consumer_key=CONSUMER_KEY,
                                 consumer_secret=CONSUMER_SECRET,
                                 access_token=ACCESS_KEY,
                                 access_token_secret=ACCESS_SECRET)
    return _twitter


def get_ratelimit():
    """Get twitter api rate limit status.

    :return: rate limit for each api
    :rtype: Dict
    """
    return twitter().get('https://api.twitter.com/1.1/'
                         'application/rate_limit_status.json').json()


def get_user_info(user_id):
    """Returns a variety of information about the user specified by `user_id`.

    :param str user: target's Twitter user id
    :rtype: Dict
    """
    return twitter().get('https://api.twitter.com/1.1/users/show.json', params=dict(user_id=user_id)).json()


def get_tweets(user_id, max_id=None):
    """Get tweets from one's timeline

    :param str user: target's Twitter user id
    :param max_id: the id of last tweet in range, defaults to be None
    :type max_id: int | None
    :return: result from API call, a list of tweets
    :rtype: List[Dict]
    """
    p = dict(user_id=user_id, count=200,
             exclude_replies=False, include_rts=True)
    if max_id is not None:
        p['max_id'] = max_id
    while 1:
        try:
            r = twitter().get('https://api.twitter.com/1.1'
                              '/statuses/user_timeline.json',
                              params=p)
            break
        except ConnectionError:
            pass
    return r.json()


def get_f(user_id, ftype):
    """Get one's follower/following

    :param str user_id: target's user id
    :param str ftype: follower or following
    :return: a mapping from follower/following id to screen name
    :rtype: Dict
    """
    p = dict(user_id=user_id, count=200, stringify_ids=True,
             include_user_entities=True, cursor=-1)
    f = []
    if ftype == 'follower':
        resource_uri = 'https://api.twitter.com/1.1/followers/list.json'
    elif ftype == 'following':
        resource_uri = 'https://api.twitter.com/1.1/friends/list.json'
    else:
        raise Exception('Unknown type: ' + ftype)
    while True:
        while 1:
            try:
                j = twitter().get(resource_uri, params=p).json()
                break
            except ConnectionError:
                pass
        if 'errors' in j:
            raise Exception(j['errors'])
        if 'error' in j:
            raise Exception(j['error'])
        f.extend([(str(u['id']), u['screen_name']) for u in j['users']])
        if j['next_cursor'] != 0:
            p['cursor'] = j['next_cursor']
        else:
            break
    return dict(f)


def fetch_conversation(sid):
    ''' Fetch conversation by tweet id via twitter api.

    :param sid: tweet id
    :type sid: str | int
    :return: list of tweets
    :rtype: List[Dict]
    '''
    threads = twitter().get('https://api.twitter.com/1.1/conversation/show.json',
                            params=dict(id=sid, include_entities=1)).json()
    return [] if 'errors' in threads else threads


def ids2names(ids):
    """Twitter user ids to screen names.

    :param List[int] ids: user ids
    :return: list of corresponding names
    :rtype: List[str]
    """
    users = twitter().get('https://api.twitter.com/1.1/friendships/lookup.json',
                          params=dict(user_id=','.join(map(str, ids)))).json()
    names = []
    i, n = 0, len(users)
    for _id in ids:
        if i < n and users[i]['id'] == _id:
            names.append(users[i]['screen_name'])
            i += 1
        else:
            names.append(None)
    return names


def names2ids(names):
    """Twitter screen names to user ids.

    :param List[str] ids: screen names
    :return: list of corresponding user ids
    :rtype: List[int]
    """
    users = twitter().get('https://api.twitter.com/1.1/friendships/lookup.json',
                          params=dict(screen_name=','.join(map(str, names)))).json()
    ids = []
    i, n = 0, len(users)
    for name in names:
        if i < n and users[i]['screen_name'] == name:
            ids.append(users[i]['id'])
            i += 1
        else:
            ids.append(None)
    return ids
