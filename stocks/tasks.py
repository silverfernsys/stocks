from celery import Celery
from quotes import fetch_and_insert_next_stock_historical_data
from db import dal
from datetime import timedelta
import sys
import ConfigParser

def load_config():
    config = ConfigParser.ConfigParser()
    if (sys.argv[-1].split('=')[0] == '--config'):
        config_file_path = sys.argv[-1].split('=')[1]
    else:
        config_file_path = '/etc/stocks/conf.d/default.conf'
    config.read(config_file_path)
    return config

conf = load_config()

class CeleryConfig(object):
    BROKER_URL = 'amqp://guest:guest@localhost:5672//'

    def __init__(self, conf):
        update_period = int(conf.get('stocks', 'update_period'))
        CeleryConfig.CELERYBEAT_SCHEDULE = {
            'every-minute': {
                'task': 'tasks.fetch_quotes',
                'schedule': timedelta(seconds=update_period), 
                'args': (),
            },
        }


def database_uri(conf):
    return conf.get('stocks', 'database')


celery = Celery('stocks')
celery.config_from_object(CeleryConfig(conf))
dal.connect(database_uri(conf))


@celery.task
def fetch_quotes():
    fetch_and_insert_next_stock_historical_data()


