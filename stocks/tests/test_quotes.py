#! /usr/bin/env python

import os
import mock
import unittest
import requests
import requests_mock
from datetime import date
from stocks.db import dal, Exchange, Stock, HistoricalQuote, CompleteHistoricalData
from stocks.load_data import parseExchangeData, parseStockData
from stocks.quotes import (fetch_historical_data, insert_historical_data,
    get_latest_year, get_current_stock, get_next_stock)


class QuotesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixtures =  os.path.join(os.path.abspath(os.path.dirname(__file__)), 'fixtures')
        dal.connect('sqlite:///:memory:')
        dal.session = dal.Session()

        # Load exchange and stock data
        parseExchangeData(os.path.join(fixtures, 'exchanges.csv'))
        parseStockData(os.path.join(fixtures, 'amex.csv'), 'AMEX')
        parseStockData(os.path.join(fixtures, 'nasdaq.csv'), 'NASDAQ')
        parseStockData(os.path.join(fixtures, 'nyse.csv'), 'NYSE')

    def setUp(self):
        self.fixtures =  os.path.join(os.path.abspath(os.path.dirname(__file__)), 'fixtures')

    def tearDown(self):
    	self.fixtures = None

    @requests_mock.Mocker()
    def test_insert_historical_data(self, m):
        stock_symbol = 'AAPL'
    	response_data = open(os.path.join(self.fixtures, 'aapl_2010-01-01_2010-01-31.json')).read()
    	m.register_uri(requests_mock.ANY, requests_mock.ANY, text=response_data)
    	quotes = fetch_historical_data(stock_symbol, date(2010, 1, 1), date(2010, 1, 31))
        insert_historical_data(quotes)
        session = dal.session
        stock = session.query(Stock).filter(Stock.symbol == stock_symbol).one()
        count = session.query(HistoricalQuote).filter(HistoricalQuote.stock == stock).count()
        self.assertEqual(len(quotes), count, "Saved all records downloaded.")

    def test_get_latest_year(self):
        session = dal.Session()
        stock = session.query(Stock).filter(Stock.symbol == 'AAPL').one()
        self.assertEqual(date.today().year + 1, get_latest_year(stock))

    def test_get_current_stock(self):
        session = dal.Session()
        first_stock = session.query(Stock).order_by(Stock.id.asc()).first()
        queried_stock = get_current_stock()
        self.assertEqual(first_stock.symbol, queried_stock.symbol)
        self.assertEqual(first_stock.exchange.symbol, queried_stock.exchange.symbol)

    def test_get_next_stock(self):
        session = dal.Session()
        first_stock = session.query(Stock).order_by(Stock.id.asc()).first()
        queried_stock = get_next_stock()
        self.assertEqual(first_stock.symbol, queried_stock.symbol)
        self.assertEqual(first_stock.exchange.symbol, queried_stock.exchange.symbol)
        second_stock = session.query(Stock).filter(Stock.id > first_stock.id).order_by(Stock.id.asc()).first()
        queried_stock = get_next_stock()
        self.assertEqual(second_stock.id, queried_stock.id)

        # Now we test the case where there's a historically complete stock for the third stock. We expect that the 
        # id returned from get_next_stock to be the fourth stock's id.
        third_stock = session.query(Stock).filter(Stock.id > second_stock.id).order_by(Stock.id.asc()).first()
        fourth_stock = session.query(Stock).filter(Stock.id > third_stock.id).order_by(Stock.id.asc()).first()
        c = CompleteHistoricalData(stock_id = third_stock.id)
        session.add(c)
        session.commit()
        queried_stock = get_next_stock()
        self.assertEqual(fourth_stock.id, queried_stock.id)


