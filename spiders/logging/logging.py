import logging
import os


def log_candidate(c):
    log_file, log_level = 'debug.log', logging.DEBUG
    log_file = '{0}_{1}'.format(c.guid, log_file)
    log_file = os.path.join(c.entry, log_file)
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    # logging.basicConfig(filename=log_file, level=log_level, format=log_format)
    logger = logging.getLogger('{0}'.format(c))
    file_handler = logger.FileHandler(filename=log_file, mode='w')
    logger.setLevel(log_level)
    logger.setFormatter(log_format)
    logger.captureWarnings(capture=True)
    logger.addFilehandler(file_handler)

def log_info(self , msg):
    log = logging.getLogger('{0}'.format(self))
    log.info(msg)

def log_warning(self , msg):
    log = logging.getLogger('{0}'.format(self))
    log.warning(msg)

def log_error(self , msg):
    log = logging.getLogger('{0}'.format(self))
    log.error(msg)

def log_exception(self , msg):
    log = logging.getLogger('{0}'.format(self))
    log.exception(msg)

# c.log_info('Logging out to log one...')
