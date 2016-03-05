from datetime import datetime

from sqlalchemy import (Column, Integer, Float, String, Date, DateTime, ForeignKey,
                        Boolean, create_engine, UniqueConstraint)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref, sessionmaker

Base = declarative_base()


class Exchange(Base):
    __tablename__ = 'exchanges'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    symbol = Column(String, nullable=False, unique=True)
    created_on = Column(DateTime(), default=datetime.now)

    def __repr__(self):
        return "Exchange(id='{self.id}', " \
            "name='{self.name}', " \
            "symbol='{self.symbol}')".format(self=self)


class Stock(Base):
    __tablename__ = 'stocks'
    __table_args__ = (UniqueConstraint('exchange_id', 'symbol'),)

    id = Column(Integer, primary_key=True)
    exchange_id = Column(Integer, ForeignKey('exchanges.id'))
    name = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    ipo_year = Column(Integer, nullable=True)
    created_on = Column(DateTime(), default=datetime.now)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)

    exchange = relationship("Exchange", backref=backref('stocks', order_by=symbol))

    def __repr__(self):
        return "Stock(id='{self.id}', " \
            "name='{self.name}', " \
            "symbol='{self.symbol}', " \
            "exchange='{self.exchange.symbol}', " \
            "ipo_year='{self.ipo_year}', " \
            "created_on='{self.created_on}', " \
            "updated_on='{self.updated_on}')".format(self=self)


class HistoricalQuote(Base):
    __tablename__ = 'historicalquotes'
    __table_args__ = (UniqueConstraint('stock_id', 'date'),)

    id = Column(Integer, primary_key=True)
    stock_id = Column(Integer, ForeignKey('stocks.id'))
    volume = Column(Integer, nullable=False)
    adj_close = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    open = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    date = Column(Date, nullable=False)

    stock = relationship("Stock", backref=backref('quotes', order_by=id))

    def __repr__(self):
        return "HistoricalQuote(id='{self.id}', " \
            "stock='{self.stock.symbol}', " \
            "volume='{self.volume}', " \
            "adj_close='{self.adj_close}', " \
            "high='{self.high}', " \
            "low='{self.low}', " \
            "open='{self.open}', " \
            "close='{self.close}', " \
            "date='{self.date}'".format(self=self)


class DataAccessLayer:

    def __init__(self):
        self.engine = None
        self.Session = None
        self.conn_string = None

    def connect(self, conn_string=None):
        self.conn_string = conn_string
        if self.conn_string:
            self.engine = create_engine(self.conn_string)
            Base.metadata.create_all(self.engine)
            self.Session = sessionmaker(bind=self.engine)


dal = DataAccessLayer()
