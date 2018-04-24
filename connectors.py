import asyncio

import async_timeout
import boto3
from botocore.exceptions import ClientError


class StoreManager(object):
    def __init__(self, logger, timeout, loop=None):
        self.__timeout = timeout
        self.__logger = logger
        self.__loop = loop if loop is not None else asyncio.get_event_loop()

    def UpdateCompany(self, cik, state, name):
        pass

    def GetCompany(self):
        pass

    def GetStates(self):
        try:
            self.__logger.info('Calling GetStates query ...')
            file = 'states.csv'
            self.s3.meta.client.download_file('chaos-insider', file, '/tmp/%s' % file)

            tmp = open('/tmp/%s' % file, 'r')
            lines = tmp.readlines()
            tmp.close()
            count = 0
            states = []
            for line in lines:
                count += 1
                if count == 1: continue
                states.append(tuple(line.split(',')))
            return states

        except ClientError as e:
            self.__logger.error(e.response['Error']['Message'])
            return None
        except Exception as e:
            self.__logger.error(e)
            return None

    def __enter__(self):
        self.s3 = boto3.resource('s3')
        self.__logger.info('StoreManager created')
        return self

    def __exit__(self, *args, **kwargs):
        self.__logger.info('StoreManager destroyed')
