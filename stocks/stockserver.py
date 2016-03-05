#! /usr/bin/env python
import argparse
import logging
import signal
import time
import ConfigParser
from setproctitle import setproctitle
from server import Server


def main():
    setproctitle('stockserver')
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="configuration file to read -- /etc/stocks/stocks.conf otherwise")
    parser.add_argument("--log_level", help="log level")
    parser.add_argument("--log_file", help="log file path")
    parser.add_argument("--database", help="the database connection to use")
    parser.add_argument("--max_wait_seconds_before_shutdown", help="the number of seconds to wait before shutting down server")
    parser.add_argument("--port", help="server port")
    args = parser.parse_args()

    try:
        config = ConfigParser.ConfigParser()
        config_file_path = args.config or '/etc/stocks/conf.d/default.conf'
        config.read(config_file_path)
        context = resolveContext(config, args)
        logging.basicConfig(filename=context['log_file'], format='%(asctime)s::%(levelname)s::%(name)s::%(message)s', level=logging.DEBUG)
        server = Server(context)
    except Exception as e:
        print('Error loading configuration file at %s.' % config_file_path)
        print('DETAILS: %s' % e)


def resolveContext(config, args):
    """
    Returns a dictionary consisting of keys found
    in config and the values of config overwritten
    by values of args
    """
    data = {}
    try:
        data['log_level'] = args.log_level or config.get('stocks', 'log_level')
    except:
        data['log_level'] = config.get('stocks', 'log_level')
    try:
        data['log_file'] = args.log_file or config.get('stocks', 'log_file')
    except:
        data['log_file'] = config.get('stocks', 'log_file')
    try:
        data['database'] = args.database or config.get('stocks', 'database')
    except:
        data['database'] = config.get('stocks', 'database')
    try:
        data['port'] = args.port or config.getint('stocks', 'port')
    except:
        data['port'] = config.getint('stocks', 'port')
    try:
        data['max_wait_seconds_before_shutdown'] = args.max_wait_seconds_before_shutdown or config.getint('stocks', 'max_wait_seconds_before_shutdown')
    except:
        data['max_wait_seconds_before_shutdown'] = config.getint('stocks', 'max_wait_seconds_before_shutdown')
    return data


if __name__ == "__main__":
    main()
