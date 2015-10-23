#!/usr/bin/env python3
import re
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, Text, func
from sqlalchemy.event import listens_for
from sqlalchemy.orm import sessionmaker
Base = declarative_base()


class Tweet(Base):
    __tablename__ = 'tweets'
    id = Column(Integer, primary_key=True)
    sender = Column(Text)
    type = Column(Text)
    timestamp = Column(Integer)
    tweet = Column(Text)
    text = Column(Text)


class Quote(Base):
    __tablename__ = 'quotes'
    id = Column(Integer, primary_key=True)
    timestamp = Column(Integer)
    person = Column(Text)
    text = Column(Text)


class Follow(Base):
    __tablename__ = 'follow'
    id = Column(Integer, primary_key=True)
    timestamp = Column(Integer)
    person = Column(Text)
    target_id = Column(Integer)
    target_name = Column(Text)
    action = Column(Text)  # unfo, fo, unfoed, foed


def re_fn(expr, item):
    reg = re.compile(expr, re.I)
    return reg.search(item) is not None


def new_session(db):
    engine = create_engine('sqlite:///' + db, convert_unicode=True, echo=False)
    listens_for(engine, "begin")(
            lambda conn: conn.connection.create_function('regexp', 2, re_fn))
    Base.metadata.create_all(engine)
    DBSession = sessionmaker(engine)
    session = DBSession()
    return session