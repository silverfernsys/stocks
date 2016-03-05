#!/usr/bin/env python
import logging
import signal
import time
import tornado.ioloop
import tornado.web
import tornado.httpserver
from db import dal
from http import HTTPVersionHandler, HTTPStockHandler # , HTTPStatusHandler, HTTPTokenHandler
# from ws import AgentWSHandler, UserWSHandler


class Server():
    # Adapted from code found at https://gist.github.com/mywaiting/4643396
    def sig_handler(self, sig, frame):
        self.logger.warning("Caught signal: %s", sig)
        tornado.ioloop.IOLoop.instance().add_callback(self.shutdown)

    def __init__(self, context):
    	dal.connect(context['database'])
        self.logger = logging.getLogger('Web Server')

        application = tornado.web.Application([
            (r'/', HTTPVersionHandler),
            (r'/stock/', HTTPStockHandler),
            # (r'/status/', HTTPStatusHandler),
            # (r'/token/', HTTPTokenHandler),
            # (r'/user/', UserWSHandler),
            # (r'/agent/', AgentWSHandler),
        ])

        self.max_wait_seconds_before_shutdown = context['max_wait_seconds_before_shutdown']
        port = context['port']
        server = tornado.httpserver.HTTPServer(application)
        server.listen(port)
        self.logger.info('Running on port %s' % port)
        self.server = server
        signal.signal(signal.SIGTERM, self.sig_handler)
        signal.signal(signal.SIGINT, self.sig_handler)
        tornado.ioloop.IOLoop.instance().start()
        self.logger.info("Exit...")

    def shutdown(self):
        self.logger.info('Stopping HTTP Server.')
        self.server.stop()
        seconds = self.max_wait_seconds_before_shutdown
        self.logger.info('Will shutdown in %s seconds...', seconds)
        io_loop = tornado.ioloop.IOLoop.instance()
        deadline = time.time() + seconds

        def stop_loop():
            now = time.time()
            if now < deadline and (io_loop._callbacks or io_loop._timeouts):
                io_loop.add_timeout(now + 1, stop_loop)
            else:
                io_loop.stop()
                self.logger.info('Shutdown')
        stop_loop()
