import decimal
import time
import json
import logging
import boto3
import datetime
import uuid
import inspect


class CloudLogger(object):
    def __init__(self):
        self.__fileLogger = logging.getLogger()
        self.__fileLogger.setLevel(logging.INFO)
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
        self.__cloudWatchLogger = boto3.client('logs')
        self.__groupName = '/aws/docker/Insider_Save'
        self.__sequenceToken = None
        self.__stream = (datetime.datetime.today().strftime('%Y/%m/%d/[$LATEST]'), uuid.uuid4().hex)
        response = self.__cloudWatchLogger.create_log_stream(
            logGroupName=self.__groupName,
            logStreamName='%s%s' % self.__stream
        )
        self.info('LogStream Created: %s' % response)

    def __logToStream(self, msg):
        if self.__sequenceToken is None:
            response = self.__cloudWatchLogger\
                .put_log_events(logGroupName=self.__groupName, logStreamName='%s%s' % self.__stream,
                                logEvents=[dict(timestamp=int(round(time.time() * 1000)),
                                                message=time.strftime("%m/%d/%Y %H:%M:%S") + msg)])
        else:
            response = self.__cloudWatchLogger\
                .put_log_events(logGroupName=self.__groupName, logStreamName='%s%s' % self.__stream,
                                logEvents=[dict(timestamp=int(round(time.time() * 1000)),
                                                message=msg)],
                                sequenceToken=self.__sequenceToken)
        self.__sequenceToken = response['nextSequenceToken']

    def info(self, msg):
        self.__fileLogger.info(msg)
        name = inspect.getframeinfo(inspect.currentframe()).function.upper()
        self.__logToStream('%s [%s] %s' % (time.strftime("%m/%d/%Y %H:%M:%S"), name,  msg))

    def debug(self, msg):
        self.__fileLogger.debug(msg)
        name = inspect.getframeinfo(inspect.currentframe()).function.upper()
        self.__logToStream('%s [%s] %s' % (time.strftime("%m/%d/%Y %H:%M:%S"), name, msg))

    def warning(self, msg):
        self.__fileLogger.warning(msg)
        name = inspect.getframeinfo(inspect.currentframe()).function.upper()
        self.__logToStream('%s [%s] %s' % (time.strftime("%m/%d/%Y %H:%M:%S"), name, msg))

    def error(self, msg):
        self.__fileLogger.error(msg)
        name = inspect.getframeinfo(inspect.currentframe()).function.upper()
        self.__logToStream('%s [%s] %s' % (time.strftime("%m/%d/%Y %H:%M:%S"), name, msg))


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
    def ioreliable(func):
        async def _decorator(self, *args, **kwargs):
            tries = 0
            result = await func(self, *args, **kwargs)
            if result is None:
                while result is None and tries < Connection.retries:
                    tries += 1
                    time.sleep(2 ** tries)
                    result = await func(self, *args, **kwargs)
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
