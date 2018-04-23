import asyncio

import async_timeout
import boto3
from botocore.exceptions import ClientError


class StoreManager(object):
    def __init__(self, logger, loop=None):
        self.__timeout = 10
        self.__logger = logger
        self.__loop = loop if loop is not None else asyncio.get_event_loop()

    def UpdateCompany(self, cik, state, name):
        try:

            response = self.Companies.update_item(
                Key={
                    'CIK': cik,
                    'State': state
                },
                UpdateExpression="set #n = :n",
                ExpressionAttributeNames={
                    '#n': 'Name'

                },
                ExpressionAttributeValues={
                    ':n': name,
                },
                ReturnValues="UPDATED_NEW")

        except ClientError as e:
            self.__logger.error(e.response['Error']['Message'])
        except Exception as e:
            self.__logger.error(e)
        else:
            # self.__logger.info(response)
            return response

    def UpdateState(self, code, name):
        try:

            response = self.States.update_item(
                Key={
                    'Code': code,
                },
                UpdateExpression="set #n = :n",
                ExpressionAttributeNames={
                    '#n': 'Name'

                },
                ExpressionAttributeValues={
                    ':n': name,
                },
                ReturnValues="UPDATED_NEW")

        except ClientError as e:
            self.__logger.error(e.response['Error']['Message'])
        except Exception as e:
            self.__logger.error(e)
        else:
            self.__logger.info(response)

    def GetCompany(self):
        try:
            self.__logger.info('Calling GetCompany query ...')

            with async_timeout.timeout(self.__timeout):
                response = self.Companies.scan()
                return response['Items']

        except ClientError as e:
            self.__logger.error(e.response['Error']['Message'])
            return None
        except Exception as e:
            self.__logger.error(e)
            return None

    def GetStates(self):
        try:
            self.__logger.info('Calling GetStates query ...')

            with async_timeout.timeout(self.__timeout):
                response = self.States.scan()
                return response['Items']

        except ClientError as e:
            self.__logger.error(e.response['Error']['Message'])
            return None
        except Exception as e:
            self.__logger.error(e)
            return None

    def __enter__(self):
        db = boto3.resource('dynamodb', region_name='us-east-1')
        self.States = db.Table('States')
        self.Companies = db.Table('Companies')
        self.__logger.info('StoreManager created')
        return self

    def __exit__(self, *args, **kwargs):
        self.__logger.info('StoreManager destroyed')
