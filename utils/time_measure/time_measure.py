import time
import logging.config
logging.config.fileConfig('logs/logging.conf')
logger = logging.getLogger('analyzing_data')

def time_measure(function,args):
    start=time.time()
    data=function(*args)
    stop=time.time()
    logger.info("Time mesasure {}: {} ".format(function,stop-start))
    return data

