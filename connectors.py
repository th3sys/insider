import asyncio
import boto3


class StoreManager(object):
    def __init__(self, logger, timeout, loop=None):
        self.__timeout = timeout
        self.__logger = logger
        self.__loop = loop if loop is not None else asyncio.get_event_loop()

    def UpdateCompany(self, items):
        try:
            self.__logger.info('Calling UpdateCompany query ...')
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

        except Exception as e:
            self.__logger.error(e)
            return None

    def __enter__(self):
        self.s3 = boto3.resource('s3')
        self.__logger.info('StoreManager created')
        return self

    def __exit__(self, *args, **kwargs):
        self.__logger.info('StoreManager destroyed')
