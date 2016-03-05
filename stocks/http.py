#!/usr/bin/env python
import json
from socket import gethostname
import tornado.httpserver
import tornado.web
from db import dal, Stock, Exchange


SERVER_VERSION = '0.0.1a'


class HTTPVersionHandler(tornado.web.RequestHandler):
    @tornado.web.addslash
    def get(self):
        data = {'version': SERVER_VERSION, 'name': 'Stocks Server'}
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(data))


class HTTPStockHandler(tornado.web.RequestHandler):
    @tornado.web.addslash
    def get(self):
        try:
        	stock_arg = self.get_query_argument('symbol')
        	exchange_arg = self.get_query_argument('exchange', None)
        	query = dal.Session().query(Stock).filter(Stock.symbol == stock_arg)
        	if exchange_arg != None:
        		query = query.filter(Stock.exchange.symbol == exchange_arg)
        	data = []
        	for stock in query:
        		print(stock)
        		data.append({'name': stock.name, 'symbol': stock.symbol, 'exchange': stock.exchange.symbol })
        except tornado.web.MissingArgumentError:
        	data = {'error':'missing stock symbol'}
        except Exception as e:
            print('Error: %s' % e)
            try:
                logger = logging.getLogger('Web Server')
                logger.error(e)
            except:
                pass
            data = {'error': str(e)}
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(data))
