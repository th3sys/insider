import asyncio
import boto3
import io
from boto3.dynamodb.conditions import Key, Attr
import json
from utils import DecimalEncoder
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
import pandas as pd


class FileType(object):
    OWNER = 'OWNER'
    ISSUER = 'ISSUER'


class Period(object):
    DAY = 'DAY'
    MONTH = 'MONTH'


class StoreManager(object):
    def __init__(self, logger, notify, timeout, loop=None):
        self.__timeout = timeout
        self.__notify = notify
        self.__logger = logger
        self.__loop = loop if loop is not None else asyncio.get_event_loop()

    def UpdateOwnersTransactions(self, cik, items):
        try:
            self.__logger.info('Calling UpdateOwnersTransactions query ...')
            file = '%s.csv' % cik
            f = open('/tmp/%s' % file, 'w')
            f.write('A/D,DATE,ISSUER,FORM,TYPE,DIRECT/INDIRECT,NUMBER,TOTAL NUMBER,LINE NUMBER, ISSUER CIK,SECURITY NAME,OWNER TYPE\n')
            # items.sort(key=lambda el: (el[1], el[2]))
            for item in items:
                ad, date, issuer, form, tran_type, di, num, total, line, i_cik, sec_name, o_type = item
                f.write('%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s \n'
                        % (ad, date, issuer.replace(',', ''), form, tran_type, di, num, total, line, i_cik, sec_name, o_type))
            f.close()
            self.s3.meta.client.upload_file('/tmp/%s' % file, 'chaos-insider', 'OWNRS/%s' % file)

        except Exception as e:
            self.__logger.error(e)

    def UpdateResults(self, date, items):
        try:
            # cik, pLM, pBLM, pRatio, mLM, mBLM, mRatio
            self.__logger.info('Calling UpdateResults query ...')
            file = 'cluster_buying_%s.csv' % date.strftime('%Y%m%d')
            f = open('/tmp/%s' % file, 'w')
            f.write('CIK,PLM,PBLM,P_RATIO,MLM,MBLM,M_RATIO\n')
            for item in items:
                cik, pLM, pBLM, pRatio, mLM, mBLM, mRatio = item
                f.write('%s, %s, %s, %s, %s, %s, %s\n'
                        % (cik, pLM, pBLM, pRatio, mLM, mBLM, mRatio))
            f.close()
            self.s3.meta.client.upload_file('/tmp/%s' % file, 'chaos-insider', 'ANALYTICS/%s' % file)

        except Exception as e:
            self.__logger.error(e)

    def UpdateTransactions(self, cik, items):
        try:
            self.__logger.info('Calling UpdateTransactions query ...')
            file = '%s.csv' % cik
            f = open('/tmp/%s' % file, 'w')
            f.write('A/D,DATE,OWNER,FORM,TYPE,DIRECT/INDIRECT,NUMBER,TOTAL NUMBER,LINE NUMBER, OWNER CIK,SECURITY NAME,OWNER TYPE\n')
            # items.sort(key=lambda el: (el[1], el[2]))
            for item in items:
                ad, date, owner, form, tran_type, di, num, total, line, o_cik, sec_name, o_type = item
                f.write('%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s \n'
                        % (ad, date, owner.replace(',', ''), form, tran_type, di, num, total, line, o_cik, sec_name, o_type))
            f.close()
            self.s3.meta.client.upload_file('/tmp/%s' % file, 'chaos-insider', 'CORPS/%s' % file)

        except Exception as e:
            self.__logger.error(e)

    def GetTimeSeries(self, name, fileType):
        try:
            key = name
            if fileType == FileType.ISSUER:
                key = 'CORPS/%s.csv' % name
            if fileType == FileType.OWNER:
                key = 'OWNRS/%s.csv' % name
            obj = self.s3.meta.client.get_object(Bucket='chaos-insider', Key=key)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()))
            return df
        except Exception as e:
            self.__logger.error(e)
            return None

    def UpdateCompanies(self, items):
        try:
            self.__logger.info('Calling UpdateCompanies query ...')
            file = 'companies.csv'
            f = open('/tmp/%s' % file, 'w')
            f.write('CODE,STATE,NAME\n')
            items.sort(key=lambda el: (el[1], el[2]))
            for item in items:
                code, state, name = item
                f.write('%s,%s,"%s"\n' % (code, state, name))
            f.close()
            self.s3.meta.client.upload_file('/tmp/%s' % file, 'chaos-insider', file)

        except Exception as e:
            self.__logger.error(e)

    def GetCompanies(self):
        try:
            self.__logger.info('Calling GetCompanies query ...')
            file = 'companies.csv'
            self.s3.meta.client.download_file('chaos-insider', file, '/tmp/%s' % file)

            tmp = open('/tmp/%s' % file, 'r')
            lines = tmp.readlines()
            tmp.close()
            count = 0
            companies = []
            for line in lines:
                count += 1
                if count == 1: continue
                companies.append(tuple(line.split(',')))
            return companies

        except Exception as e:
            self.__logger.error(e)
            return None

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

        except Exception as e:
            self.__logger.error(e)
            return None

    def GetAnalytics(self, analytic, date, period):
        try:
            self.__logger.info('Calling GetAnalytics query ...')
            if period == Period.DAY:
                start = datetime(date.year, date.month, date.day, 0, 0, 0, 1)
                start = str((start - datetime(1970, 1, 1)).total_seconds())
                end = datetime(date.year, date.month, date.day, 23, 59, 59, 999999)
                end = str((end - datetime(1970, 1, 1)).total_seconds())
            if period == Period.MONTH:
                startDate = datetime(date.year, date.month, date.day, 0, 0, 0, 1)
                end = str((startDate - datetime(1970, 1, 1)).total_seconds())

                first = startDate.replace(day=1)
                lastMonth = first - timedelta(days=1)
                endDate = datetime(lastMonth.year, lastMonth.month, date.day, 0, 0, 0, 1)
                start = str((endDate - datetime(1970, 1, 1)).total_seconds())
            response = self.__Analytics.query(
                KeyConditionExpression=Key('AnalyticId').eq(analytic) & Key('TransactionTime').between(start, end))
        except ClientError as e:
            self.__logger.error(e.response['Error']['Message'])
            return None
        except Exception as e:
            self.__logger.error(e)
            return None
        else:
            if 'Items' in response:
                return response['Items']

    def SaveAnalytics(self, action, description, message, today, count, requestId, chunks):
        try:
            # date = datetime.strptime(today, '%Y%m%d')
            currentTime = datetime.now().time()
            todayWithCurrentTime = datetime.combine(today, currentTime)
            key = (todayWithCurrentTime - datetime(1970, 1, 1)).total_seconds()
            # datetime.fromtimestamp(key)

            response = self.__Analytics.update_item(
                Key={
                    'AnalyticId': action,
                    'TransactionTime': str(key),
                },
                UpdateExpression="set #desc = :desc, #m = :m, #d = :d, #c = :c, #r = :r, #ch = :ch",
                ExpressionAttributeNames={
                    '#desc': 'Description',
                    '#m': 'Message',
                    '#d': 'Date',
                    '#c': 'Count',
                    '#r': 'RequestId',
                    '#ch': 'Chunks'

                },
                ExpressionAttributeValues={
                    ':desc': description,
                    ':m': message,
                    ':d': today.strftime('%Y%m%d'),
                    ':c': count,
                    ':r': requestId,
                    ':ch': chunks
                },
                ReturnValues="UPDATED_NEW")

        except ClientError as e:
            self.__logger.error(e.response['Error']['Message'])
        except Exception as e:
            self.__logger.error(e)
        else:
            self.__logger.info('Analytics Saved')
            self.__logger.info(json.dumps(response, indent=4, cls=DecimalEncoder))

    def __enter__(self):
        db = boto3.resource('dynamodb', region_name='us-east-1')
        self.__Analytics = db.Table('Insiders.Analytics')
        self.s3 = boto3.resource('s3')
        self.sns = boto3.client('sns')
        self.__logger.info('StoreManager created')
        return self

    def __exit__(self, *args, **kwargs):
        self.__logger.info('StoreManager destroyed')
