import asyncio
import boto3
import json


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

    def Notify(self, label, count):
        try:
            message = {label: count}
            response = self.sns.publish(
                TargetArn=self.__notify,
                Message=json.dumps({'default': json.dumps(message)}),
                MessageStructure='json'
            )
            self.__logger.info(response)
        except Exception as e:
            self.__logger.error(e)

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

    def __enter__(self):
        self.s3 = boto3.resource('s3')
        self.sns = boto3.client('sns')
        self.__logger.info('StoreManager created')
        return self

    def __exit__(self, *args, **kwargs):
        self.__logger.info('StoreManager destroyed')
