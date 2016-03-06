#! /usr/bin/env python
from datetime import date, datetime
import requests
from requests.utils import quote
from db import dal, HistoricalQuote, Stock, StockPointer

DATE_FORMAT = '%Y-%m-%d'
YAHOO_API_URL = 'https://query.yahooapis.com/v1/public/yql?q='

def get_historical_data(symbol, start, end):
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
        # This is where we do some fancy query called an ANTI-JOIN.
        # SELECT
        # *
        # FROM table1 t1
        # LEFT JOIN table2 t2 ON t1.id = t2.id
        # WHERE t2.id IS NULL
        #
        # https://www.google.ca/search?q=limit+offset+postgres&ie=utf-8&oe=utf-8&gws_rd=cr&ei=unPbVte_Kde8jwOspIjQDw
        # http://blog.montmere.com/2010/12/08/the-anti-join-all-values-from-table1-where-not-in-table2/
        # http://blog.jooq.org/2014/08/12/the-difference-between-row_number-rank-and-dense_rank/
        # http://stackoverflow.com/questions/17437317/complex-query-subqueries-window-functions-with-sqlalchemy
        # n is the current StockPointer.stock_id
        # SELECT
        # stocks.id
        # FROM stocks
        # LEFT JOIN completehistoricaldata ON stocks.id = completehistoricaldata.stock_id
        # WHERE completehistoricaldata.id IS NULL ORDER BY stocks.id DESC OFFSET n

        # pointer = session.query(StockPointer).order_by(StockPointer.stock_id.desc()).one()
        # stock = session.query(Stock).filter(Stock.id == pointer.stock_id).one()
        # session.close()
        # return stock

        # 
        # # join table_a and table_b
        # query = session.query(table_a, table_b)
        # query = query.filter(table_a.id == table_b.id)

        # # create subquery
        # subquery = session.query(table_c.id)
        # # select all from table_a not in subquery
        # query = query.filter(~table_a.id.in_(subquery))

        query = session.query(Stock, StockPointer)
        query = query.filter(Stock.id == StockPointer.stock_id)
        query = query.filter(~Stock.id.in_(query))

        # Do this the naive way for the time being. This assumes no historically complete stocks.
        pointer = session.query(StockPointer).order_by(StockPointer.stock_id).one()
        stock = session.query(Stock).filter(Stock.id == pointer.stock_id).one()
        try:
            next_stock = session.query(Stock).filter(Stock.id > stock.id).order_by(Stock.id.desc()).one()
        except:
            # There is no 'next_stock', so we'll save a pointer to, and return, the very first stock.
            next_stock = session.query(Stock).order_by(Stock.id.desc()).one()
        session.delete(pointer)
        pointer = StockPointer(stock_id = next_stock.id)
        session.add(pointer)
        session.commit()
        session.close()
        return next_stock
    except:
        try:
            stock = session.query(Stock).order_by(Stock.id.desc()).first()
            pointer = StockPointer(stock_id=stock.id)
            session.add(pointer)
            session.commit()
            # Force lazy eval of exchange, because session will be closed before function returns.
            exchange = stock.exchange
            session.close()
            return stock
        except Exception as e:
            print('EXCEPTION: %s' % e)
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
        pointer = session.query(StockPointer).order_by(StockPointer.stock_id.desc()).one()
        stock = session.query(Stock).filter(Stock.id == pointer.stock_id).one()
        session.close()
        return stock
    except:
        try:
            stock = session.query(Stock).order_by(Stock.id.desc()).first()
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
        last_quote = session.query(HistoricalQuote).filter(stock_id == stock.id).order_by(HistoricalQuote.date.desc()).one()
        session.close()
        return last_quote.date.year
    except:
        session.close()
        return date.today().year
