#! /usr/bin/env python
import argparse
import ConfigParser
from db import dal, Stock, Exchange

def resolveContext(config, args):
    """
    Returns a dictionary consisting of keys found
    in config and the values of config overwritten
    by values of args
    """
    data = {}
    data['database'] = args.database or config.get('stocks', 'database')
    return data


def printStocks():
    session = dal.Session()
    print("========EXCHANGES========")
    for row in session.query(Exchange):
        print(row)
    print("=========STOCKS==========")
    for row in session.query(Stock):
        print(row)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="the configuration file to use")
    parser.add_argument("--database", help="the database connection to use")
    args = parser.parse_args()

    try:
        config = ConfigParser.ConfigParser()
        config_file_path = args.config or '/etc/stocks/conf.d/default.conf'
        config.read(config_file_path)
        context = resolveContext(config, args)

        dal.connect(context['database'])
        printStocks()

    except Exception as e:
        print('Error loading configuration file at %s.' % config_file_path)
        print('DETAILS: %s' % e)

if __name__ == '__main__':
    main()