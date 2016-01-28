#!/usr/bin/env python3
import re
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, BigInteger, Text, Boolean, func
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB
from sqlalchemy.orm import sessionmaker
Base = declarative_base()


class Tweet(Base):
    """ Representation of Tweet instance in database. """
    __tablename__ = 'tweets'
    #: tweet numeric id (primary key)
    id = Column(BigInteger, primary_key=True)
    #: sender's user_id
    user_id = Column(BigInteger)
    #: type of tweet: tweet, reply, rt, quote
    type = Column(Text)
    #: timestamp when tweet
    timestamp = Column(BigInteger)
    #: json data
    tweet = Column(Text)
    #: main content of tweet
    text = Column(Text)
    #: whether is already deleted online
    deleted = Column(Boolean)


class Quote(Base):
    """ Representation of Quote instance in database. """
    __tablename__ = 'quote'
    #: tweet numeric id (primary key)
    id = Column(Integer, primary_key=True)
    #: timestamp of the quote
    timestamp = Column(BigInteger)
    #: user id of the person who made the quote
    user_id = Column(BigInteger)
    #: main content
    text = Column(Text)


class Follow(Base):
    """ Representation of following/follower change instance in database. """
    __tablename__ = 'follow'
    #: unique id
    id = Column(Integer, primary_key=True)
    #: timestamp when action happens
    timestamp = Column(BigInteger)
    #: user id of the actor
    user_id = Column(BigInteger)
    #: numeric id of target
    target_id = Column(BigInteger)
    #: screen_name of target
    target_name = Column(Text)
    #: type of action: unfo, fo, unfoed, foed
    action = Column(Text)


class Bio(Base):
    """ Representation of bio change instance in database. """
    __tablename__ = 'bio'
    #: unique id
    id = Column(Integer, primary_key=True)
    #: timestamp when bio changes
    timestamp = Column(TIMESTAMP)
    #: bio json data
    bio = Column(JSONB)


def re_fn(pat, item):
    """Customized search function.

    :param str pat: pattern
    :param str item: target
    :return: if item matches pattern
    :rtype: bool
    """
    try:
        reg = re.compile(pat, re.I)  # perform case-insensitive matching
        return reg.search(item) is not None
    except re.error:
        return False


def new_session(debug=False):
    """Create new database session.

    HOWTO init PostgreSQL database::

        sudo -i -u postgres
        initdb --locale en_US.UTF-8 -E UTF8 -D '/var/lib/postgres/data'
        sudo systemctl start postgresql.service
        createuser -s -e -d jom
        createdb jom -U jom

    :return: a database session
    :rtype: DBSession
    """
    engine = create_engine('postgresql://jom:@localhost/jom', echo=debug)
    Base.metadata.create_all(engine)
    DBSession = sessionmaker(engine)
    session = DBSession()
    return session


def check_deleted(session, tweets):
    """Check if there are tweets in DB are deleted online based on
       these existing tweets.

    :type tweets: Dict
    :param tweets: list of tweets of someone sorted by id descendingly
    """
    user_id = tweets[-1]['user']['id']
    since_id = tweets[-1]['id']
    tweets_in_db = session.query(Tweet)\
                          .filter((Tweet.user_id == user_id) &
                                  (Tweet.id >= since_id))\
                          .order_by(Tweet.id.desc()).all()
    dels = []
    tweets = tweets[:]  # make a copy
    # check backwards
    while tweets and tweets_in_db:
        if tweets_in_db[-1].id < tweets[-1]['id']:
            dels.append(tweets_in_db.pop())
        elif tweets_in_db[-1].id > tweets[-1]['id']:
            tweets.pop()
        elif tweets_in_db[-1].id == tweets[-1]['id']:
            tweets_in_db.pop()
            tweets.pop()
    # dels.extend(tweets_in_db)
    for t in dels:
        t.deleted = True
        session.add(t)
    session.commit()
    return dels


if __name__ == '__main__':
    import json
    victims = json.load(open('config.json'))['victims']
    name2id = {u['ref_screen_name']: _id for _id, u in victims.items()}
    session = new_session(debug=True)
    for i in session.query(Tweet.text)\
            .filter((Tweet.user_id == name2id['masked']) &
                    (Tweet.type == 'tweet'))\
            .filter(Tweet.text.op('~')('23*3'))\
            .order_by(func.random()).limit(5):
        print(i.text)
