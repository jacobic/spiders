import os
import logging
import src.globals as glo


class CustomAdapter(logging.LoggerAdapter):
    """
    https://docs.python.org/2/howto/logging-cookbook.html#using
    -loggeradapters-to-impart-contextual-information
    This example adapter expects the passed in dict-like object to have a
    'connid' key, whose value in brackets is prepended to the log message.
    """

    def __init__(self, logger, extra, key):
        super().__init__(logger=logger, extra=extra)
        self.key = key

    def process(self, msg, kwargs):
        return f'[{self.extra[self.key]}] {msg}', kwargs


class SparkLogger:
    """
    In order to set up spark logging, add this file to the spark context:
        ss = SparkSession.builder.appName("Foo").getOrCreate()
        sc = ss.sparkContext
        file = 'file:///{0}/spark_logging.py'.format(path_to_SparkLogger_class))
        sc.addPyFile(file)

    This allows every spark executor to use the same logger from the python
    logging module and therefore they can all write to the same file.

    Examples
    --------
    import spark_logging

    class FooBar:

        def __init__(self, a, b, c):

        @property
        def log(self):
            if os.environ['HOST'].startswith('draco'):
            # Host checking is useful if you only run spark on some machines.
                return spark_logging.SparkLogger()

    foobar = FooBar(a=1, b=2, c=3)
    foobar.log.info("Just created instance of FooBar class")

    """

    def __init__(self, extra, key):
        self.extra = extra
        self.key = key

    @staticmethod
    def setup_logger(file):
        """
        Additional formatting options can be chosen from here:
            https://docs.python.org/3/library/logging.html#logrecord-attributes

        """
        fmt = '%(asctime)s.%(msecs)03d - %(processName)s %(process)d - %(' \
              'threadName)s %(thread)d %(levelname)s %(module)s - %(' \
              'funcName)s: %(message)s'
        # logging.basicConfig(filename=file, level=logging.DEBUG, format=fmt)
        log_name = 'pyspark'
        log_level = logging.DEBUG
        logger = logging.getLogger(log_name)
        logger.setLevel(log_level)
        # create a file handler
        handler = logging.FileHandler(file, mode='a')
        handler.setLevel(log_level)
        # create a logging format
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(handler)

    @property
    def _logger(self):
        return logging.getLogger('pyspark')

    @property
    def logger(self):
        return CustomAdapter(self._logger, extra=self.extra, key=self.key)

    def __getattr__(self, key):
        """
        Parameters
        ----------

        key : {'info', 'error', 'exception', 'warn'}
            Key to access logger.

        Returns
        -------
        Logger with key. e.g. logging.info.
        """

        return getattr(self.logger, key)


log = os.path.join(glo.DIR_PROJECT, 'pyspark.log')
SparkLogger.setup_logger(file=log)
