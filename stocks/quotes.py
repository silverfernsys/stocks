#! /usr/bin/env python
from datetime import date, datetime
import requests
from requests.utils import quote
from db import dal, HistoricalQuote, Stock

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
def insert_data(data):
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
