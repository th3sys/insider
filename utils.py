import decimal
import time
import json
import logging
import boto3
import datetime
import uuid
import inspect


class CloudLogger(object):
    CRITICAL = 50
    FATAL = CRITICAL
    ERROR = 40
    WARNING = 30
    WARN = WARNING
    INFO = 20
    DEBUG = 10
    NOTSET = 0

    def __init__(self, level):
        self.__fileLogger = logging.getLogger()
        self.__fileLogger.setLevel(level)
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
        self.__cloudWatchLogger = boto3.client('logs')
        self.__groupName = '/aws/docker/Insider_Save'
        self.__sequenceToken = None
        self.__level = level
        self.__stream = (datetime.datetime.today().strftime('%Y/%m/%d/[$LATEST]'), uuid.uuid4().hex)
        response = self.__cloudWatchLogger.create_log_stream(
            logGroupName=self.__groupName,
            logStreamName='%s%s' % self.__stream
        )
        self.info('LogStream Created: %s' % response)

    def __logToStream(self, msg):
        if self.__sequenceToken is None:
            response = self.__cloudWatchLogger \
                .put_log_events(logGroupName=self.__groupName, logStreamName='%s%s' % self.__stream,
                                logEvents=[dict(timestamp=int(round(time.time() * 1000)),
                                                message=time.strftime("%m/%d/%Y %H:%M:%S") + msg)])
        else:
            response = self.__cloudWatchLogger \
                .put_log_events(logGroupName=self.__groupName, logStreamName='%s%s' % self.__stream,
                                logEvents=[dict(timestamp=int(round(time.time() * 1000)),
                                                message=msg)],
                                sequenceToken=self.__sequenceToken)
        self.__sequenceToken = response['nextSequenceToken']

    def info(self, msg):
        if self.__level not in [CloudLogger.INFO, CloudLogger.DEBUG]: return
        self.__fileLogger.info(msg)
        self.__logToStream('%s [%s] %s' % (time.strftime("%m/%d/%Y %H:%M:%S"), 'INFO', msg))

    def debug(self, msg):
        if self.__level not in [CloudLogger.DEBUG]: return
        self.__fileLogger.debug(msg)
        self.__logToStream('%s [%s] %s' % (time.strftime("%m/%d/%Y %H:%M:%S"), 'DEBUG', msg))

    def warn(self, msg):
        if self.__level not in [CloudLogger.INFO, CloudLogger.DEBUG, CloudLogger.WARN]: return
        self.__fileLogger.warn(msg)
        self.__logToStream('%s [%s] %s' % (time.strftime("%m/%d/%Y %H:%M:%S"), 'WARN', msg))

    def error(self, msg):
        if self.__level not in [CloudLogger.INFO, CloudLogger.DEBUG, CloudLogger.WARN, CloudLogger.ERROR]: return
        self.__fileLogger.error(msg)
        self.__logToStream('%s [%s] %s' % (time.strftime("%m/%d/%Y %H:%M:%S"), 'ERROR', msg))


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class Connection(object):
    retries = 5

    def __init__(self):
        pass

    @staticmethod
    def ioreliablehttp(func):
        async def _decorator(self, *args, **kwargs):
            tries = 0
            result = await func(self, *args, **kwargs)
            check = result is None or (isinstance(result, tuple) and None in result) or \
                    (len(result[2]) == 1 and result[2] != [200])

            if check:
                while check and tries < Connection.retries:
                    tries += 1
                    time.sleep(2 ** tries)
                    result = await func(self, *args, **kwargs)
                    check = result is None or (isinstance(result, tuple) and None in result) or \
                            (len(result[2]) == 1 and result[2] != [200])
            return result

        return _decorator

    @staticmethod
    def ioreliable(func):
        async def _decorator(self, *args, **kwargs):
            tries = 0
            result = await func(self, *args, **kwargs)
            check = result is None or (isinstance(result, tuple) and None in result)

            if check:
                while check and tries < Connection.retries:
                    tries += 1
                    time.sleep(2 ** tries)
                    result = await func(self, *args, **kwargs)
                    check = result is None or (isinstance(result, tuple) and None in result)
            return result

        return _decorator

    @staticmethod
    def reliable(func):
        def _decorator(self, *args, **kwargs):
            tries = 0
            result = func(self, *args, **kwargs)
            if result is None:
                while result is None and tries < Connection.retries:
                    tries += 1
                    time.sleep(2 ** tries)
                    result = func(self, *args, **kwargs)
            return result

        return _decorator
