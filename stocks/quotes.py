#! /usr/bin/env python
from datetime import date, datetime
import requests
from requests.utils import quote
from db import dal, HistoricalQuote, Stock, StockPointer

DATE_FORMAT = '%Y-%m-%d'
YAHOO_API_URL = 'https://query.yahooapis.com/v1/public/yql?q='


def fetch_and_insert_next_stock_historical_data():
    """
    This function calls 'get_next_stock()' for symbol
    and 'get_latest_year()' for start and end dates
    """
    stock = get_next_stock()
    print('stock: %s' % stock)
    year = get_latest_year(stock) - 1
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    data = fetch_historical_data(stock.symbol, start, end)
    insert_historical_data(data)


def fetch_historical_data(symbol, start, end):
    """
    Returns a JSON array of historical stock prices
    for symbol 'symbol', 'start' day, and 'end' day.
    Example Usage: get_historical_data('APPL', '01-01-2010', '12-31-2010')
    """
    # I can't seem to get the url encoding working correctly, so here's a hack from YQL :D
    yql = 'select%20*%20from%20yahoo.finance.historicaldata%20where%20symbol%20%3D%20%22{0}' \
                '%22%20and%20startDate%20%3D%20%22{1}%22%20and%20endDate%20%3D%20%22{2}' \
                '%22&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=' \
                .format(symbol, start.strftime(DATE_FORMAT), end.strftime(DATE_FORMAT))
    query = '{0}{1}'.format(YAHOO_API_URL, yql)
    response = requests.get(query)
    # print('RESPONSE.TEXT: %s' % response.text)
    return response.json()['query']['results']['quote']


# Volume': u'311488100', u'Symbol': u'AAPL', u'Adj_Close': u'25.409244', u'High': u'202.199995',
# u'Low': u'190.250002', u'Date': u'2010-01-29', u'Close': u'192.060003', u'Open': u'201.079996'}
def insert_historical_data(data):
    """
    Inserts an array of JSON data into a SQL database.
    All data must belong to the same stock symbol.
    """
    if len(data) > 0:
        session = dal.Session()
        # symbol = data[0]['Symbol']
        # Currently, fail silently by returning if stock is not found by symbol
        try:
            stock = session.query(Stock).filter(Stock.symbol == data[0]['Symbol']).one()
        except Exception as e:
            print('insert_data Exception: %s' % e)
            return

        try:
            quotes = []
            for d in data:
                quotes.append(HistoricalQuote(
                    stock_id = stock.id,
                    volume = d['Volume'],
                    adj_close = d['Adj_Close'],
                    high = d['High'],
                    low = d['Low'],
                    open = d['Open'],
                    close = d['Close'],
                    date = datetime.strptime(d['Date'], DATE_FORMAT).date()))
            session.bulk_save_objects(quotes)
            session.commit()
        except Exception as e:
            print('insert_data EXCEPTION: %s' % e)
            session.rollback()


def get_next_stock():
    """
    This function has side effects.
    It updates StockPointer to the next stock.
    Returns: the new stock pointed to by StockPointer.
    """
    session = dal.Session()
    try:
        pointer = session.query(StockPointer).order_by(StockPointer.stock_id).one()
        current_stock = session.query(Stock).filter(Stock.id == pointer.stock_id).one()
        try:
            next_stock = session.query(Stock).filter(Stock.id > current_stock.id).filter(Stock.historically_complete == None).order_by(Stock.id.asc()).first()
        except Exception as e:
            print('EXCEPTION 1: %s' % e)
            # There is no 'next_stock', so we'll save a pointer to, and return, the very first stock.
            next_stock = session.query(Stock).order_by(Stock.id.asc()).first()
        session.delete(pointer)
        pointer = StockPointer(stock_id = next_stock.id)
        session.add(pointer)
        session.commit()
        # next_stock = session.query(Stock).filter(Stock.id == next_stock.id).one()
        exchange = next_stock.exchange
        session.close()
        return next_stock
    except Exception as e:
        try:
            print('EXCEPTION 2: %s' % e)
            stock = session.query(Stock).order_by(Stock.id.asc()).first()
            pointer = StockPointer(stock_id=stock.id)
            session.add(pointer)
            session.commit()
            # Force lazy eval of exchange, because session will be closed before function returns.
            exchange = stock.exchange
            session.close()
            return stock
        except Exception as e:
            print('EXCEPTION 3: %s' % e)
            session.rollback()
            session.close()
            return None


def get_current_stock():
    """
    Returns: stock pointed to by StockPointer.
    If no StockPointer exists, returns the first
    stock.
    """
    session = dal.Session()
    try:
        pointer = session.query(StockPointer).order_by(StockPointer.stock_id.asc()).one()
        stock = session.query(Stock).filter(Stock.id == pointer.stock_id).one()
        session.close()
        return stock
    except:
        try:
            stock = session.query(Stock).order_by(Stock.id.asc()).first()
            # Force lazy eval of exchange, because session will be closed before function returns.
            exchange = stock.exchange
            session.close()
            return stock
        except:
            session.close()
            return None


def get_latest_year(stock):
    """
    Return the last year for which historical data exists.
    If no historical data exists for this stock,
    returns the current year + 1.
    """
    session = dal.Session()
    try:
        last_quote = session.query(HistoricalQuote).filter(stock_id == stock.id).order_by(HistoricalQuote.date.asc()).first()
        session.close()
        return last_quote.date.year
    except:
        session.close()
        return date.today().year + 1
