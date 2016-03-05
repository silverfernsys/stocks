#! /usr/bin/env python

import argparse
import ConfigParser
import csv
from db import dal, Exchange, Stock


def resolveContext(config, args):
    """
    Returns a dictionary consisting of keys found
    in config and the values of config overwritten
    by values of args
    """
    data = {}
    data['database'] = args.database or config.get('stocks', 'database')
    return data


def parseExchangeData(filename):
    with open(filename) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            symbol = row['Symbol'].strip()
            name = row['Name'].strip()
            session = dal.Session()
            exchange = Exchange(name=name, symbol=symbol)
            try:
                session.add(exchange)
                session.commit()
            except Exception as e:
                print("e: %s" % e)


def parseStockData(filename, exchange_symbol):
    session = dal.Session()
    # for row in session.query(Exchange):
    #     print('row: %s' % row)
    try:
        exchange = session.query(Exchange).filter(Exchange.symbol == exchange_symbol).one()
        # print('exchange_symbol: %s exchange: %s' % (exchange_symbol, exchange))
        stocks = []
        with open(filename) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                symbol = row['Symbol'].strip()
                name = row['Name'].strip()
                ipo_year = row['IPOyear'].strip()
                if ipo_year != 'n/a':
                    ipo = int(ipo_year)
                else:
                    ipo = None
                stock = Stock(exchange_id=exchange.id,
                    name=name, symbol=symbol, ipo_year=ipo)
                stocks.append(stock)
        session.bulk_save_objects(stocks)
        session.commit()
    except Exception as e:
        print(e)
        session.rollback()
    # print('exchange.stocks: %s' % exchange.stocks)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stocks", help="the stocks csv file to load")
    parser.add_argument("--exchange_name", help="the exchange name to add stocks to")
    parser.add_argument("--exchanges", help="the exchanges csv file to load")
    parser.add_argument("--config", help="the configuration file to use")
    parser.add_argument("--database", help="the database connection to use")
    args = parser.parse_args()

    try:
        config = ConfigParser.ConfigParser()
        config_file_path = args.config or '/etc/stocks/conf.d/default.conf'
        config.read(config_file_path)
        context = resolveContext(config, args)

        dal.connect(context['database'])
        dal.session = dal.Session()

        if args.exchanges:
            parseExchangeData(args.exchanges)
        if args.stocks:
            if args.exchange_name is None:
                print('Please provide an exhange name. --exchange_name=...')
            else:
                parseStockData(args.stocks, args.exchange_name)
        if args.exchanges is None and args.stocks is None:
            printStocks()
    except Exception as e:
        print('Error loading configuration file at %s.' % config_file_path)
        print('DETAILS: %s' % e)


if __name__ == "__main__":
    main()
