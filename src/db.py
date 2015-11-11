#!/usr/bin/env python3
import re
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, Text, func
from sqlalchemy.event import listens_for
from sqlalchemy.orm import sessionmaker
Base = declarative_base()


class Tweet(Base):
    """Representation of Tweet instance in database

    Attributes:
        id (int): tweet numeric id (primary key)
        sender (string) : sender's name
        type (string): type of tweet: tweet, reply, rt, quote
        tweet (string) : tweet
        text (string) : main content of tweet
    """
    __tablename__ = 'tweets'
    id = Column(Integer, primary_key=True)
    sender = Column(Text)
    type = Column(Text)
    timestamp = Column(Integer)
    tweet = Column(Text)
    text = Column(Text)


class Quote(Base):
    """Representation of Quote instance in database

    Attributes:
        id (int): tweet numeric id (primary key)
        timestamp (int): timestamp of the quote
        person (string): name of the person who made the quote
        text (string) : main content
    """
    __tablename__ = 'quotes'
    id = Column(Integer, primary_key=True)
    timestamp = Column(Integer)
    person = Column(Text)
    text = Column(Text)


class Follow(Base):
    """Representation of following/follower change instance in database

    Attributes:
        id (int):
        timestamp (int): timestamp when action happens
        person (string): name of the actor
        target_id (id): numeric id of target
        target_name (string): string of person who made the quote
        action (string): type of action: unfo, fo, unfoed, foed
    """
    __tablename__ = 'follow'
    id = Column(Integer, primary_key=True)
    timestamp = Column(Integer)
    person = Column(Text)
    target_id = Column(Integer)
    target_name = Column(Text)
    action = Column(Text)


def re_fn(pat, item):
    """Customized search function

    :param string pat: pattern
    :param string item: target
    :return : if item matches pattern
    :rtype: bool
    """
    reg = re.compile(pat, re.I)  # perform case-insensitive matching
    return reg.search(item) is not None


def new_session(db):
    """Create new database session

    :param string db: name of database file. Database should be in the same directory as the directory
     in which program starts
    :return : a database session
    :rtype: DBSession
    """
    engine = create_engine('sqlite:///' + db, convert_unicode=True, echo=False)
    listens_for(engine, "begin")(
            lambda conn: conn.connection.create_function('regexp', 2, re_fn))
    Base.metadata.create_all(engine)
    DBSession = sessionmaker(engine)
    session = DBSession()
    return session

