#! /usr/bin/env python

import os
import mock
import unittest
import requests
import requests_mock
from datetime import date
from stocks.db import dal, Exchange, Stock, HistoricalQuote
from stocks.load_data import parseExchangeData, parseStockData
from stocks.quotes import get_historical_data, insert_data


class QuotesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixtures =  os.path.join(os.path.abspath(os.path.dirname(__file__)), 'fixtures')
        dal.connect('sqlite:///:memory:')
        dal.session = dal.Session()

        # Load exchange and stock data
        parseExchangeData(os.path.join(fixtures, 'exchanges.csv'))
        parseStockData(os.path.join(fixtures, 'amex.csv'), 'amex')
        parseStockData(os.path.join(fixtures, 'nasdaq.csv'), 'nasdaq')
        parseStockData(os.path.join(fixtures, 'nyse.csv'), 'nyse')

    def setUp(self):
        self.fixtures =  os.path.join(os.path.abspath(os.path.dirname(__file__)), 'fixtures')

    def tearDown(self):
    	self.fixtures = None

    @requests_mock.Mocker()
    def test_insert_historical_data(self, m):
        stock_symbol = 'AAPL'
    	response_data = open(os.path.join(self.fixtures, 'aapl_2010-01-01_2010-01-31.json')).read()
    	m.register_uri(requests_mock.ANY, requests_mock.ANY, text=response_data)
    	quotes = get_historical_data(stock_symbol, date(2010, 1, 1), date(2010, 1, 31))
        insert_data(quotes)
        session = dal.session
        stock = session.query(Stock).filter(Stock.symbol == stock_symbol).one()
        count = session.query(HistoricalQuote).filter(HistoricalQuote.stock == stock).count()
        self.assertEqual(len(quotes), count, "Saved all records downloaded.")
