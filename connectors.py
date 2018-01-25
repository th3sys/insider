import pymysql.cursors


class DbParams(object):
    def __init__(self):
        self.Host = ''
        self.User = ''
        self.Password = ''
        self.Name = ''


class InsiderDb:
    def __init__(self, params, logger):
        self.Timeout = 10
        self.__logger = logger
        self.__params = params
        self.__connection = None

    async def __aenter__(self):
        self.__connection = pymysql.connect(host=self.__params.Host, user=self.__params.User,
                             password=self.__params.Password, db=self.__params.Name,
                             charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        self.__cursor = self.__connection.__enter__()
        self.__logger.info('MySql created')
        return self

    def GetStates(self):
        sql = "SELECT `CODE`, `NAME` FROM `STATE`"
        self.__cursor.execute(sql)
        result = self.__cursor.fetchall()
        return result

    async def __aexit__(self, *args, **kwargs):
        self.__connection.__exit__(*args, **kwargs)
        self.__connection.close()
        self.__logger.info('MySql destroyed')