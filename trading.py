import aiohttp
import asyncio
import async_timeout
import bs4
from utils import Connection
from connectors import StoreManager
import random
import socket

class EdgarParams(object):
    def __init__(self):
        self.Url = ''
        self.PageSize = ''


class EdgarClient:
    """Edgar client."""

    def __init__(self, params, logger, loop=None):
        self.__timeout = 600
        self.__logger = logger
        self.__params = params
        self.__tokens = None
        self.__loop = loop if loop is not None else asyncio.get_event_loop()

    @Connection.ioreliable
    async def GetTransaction(self):
        # https://www.sec.gov/cgi-bin/own-disp
        pass

    @Connection.ioreliable
    async def GetCompaniesByState(self, state, path=None):
        def GetText(tag):
            nxt = tag.next
            while type(nxt) is not bs4.element.NavigableString:
                nxt = nxt.next
            return nxt
        try:
            companies = []
            path = path if path is not None else \
                'company=&match=&filenum=&State=%s&Country=&SIC=&myowner=include&action=getcompany&count=%s' % \
                (state, self.__params.PageSize)
            url = '%s/cgi-bin/browse-edgar?%s' % (self.__params.Url, path)
            with async_timeout.timeout(self.__timeout):
                self.__logger.debug('Calling SearchByState ...')
                response = await self.__connection.get(url=url)
                self.__logger.debug('SearchByState Response Code: {}'.format(response.status))
                payload = await response.text()
                soup = bs4.BeautifulSoup(payload, "html.parser")

                rows = [tr for table in soup.find_all('table') if 'Results' in table.attrs['summary']
                        for tr in table.children if tr != '\n']
                for row in rows:
                    tds = list(filter(lambda x: x != '\n', row.children))
                    cik = GetText(tds[0])
                    name = GetText(tds[1])
                    if cik != 'CIK':
                        companies.append((cik, name, state))

                links = (tag.attrs['onclick'] for tag in soup.find_all('input') if 'button' in tag.attrs['type']
                         and 'Next %s' % self.__params.PageSize in tag.attrs['value'])
                for link in links:
                    self.__logger.debug(link)
                    parts = link.split('?')
                    more = await self.GetCompaniesByState(state, parts[1])
                    companies.extend(more)
                return companies
        except Exception as e:
            self.__logger.error(e)
            return None

    async def __aenter__(self):

        connector = aiohttp.TCPConnector(verify_ssl=False, family=socket.AF_INET)
        self.__session = aiohttp.ClientSession(loop=self.__loop, connector=connector)
        self.__connection = await self.__session.__aenter__()
        self.__logger.info('Session created')
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.__session.__aexit__(*args, **kwargs)
        self.__logger.info('Session destroyed')


class Scheduler:
    def __init__(self, params, logger, loop=None):
        self.Timeout = 600
        self.__logger = logger
        self.__params = params
        self.__loop = loop if loop is not None else asyncio.get_event_loop()

    async def SyncCompanies(self):
        #states = self.__insiderSession.GetStates()
        states = [{'Code': 'AL'}]
        self.__logger.info('Loaded states: %s' % states)

        futures = [self.__edgarConnection.GetCompaniesByState(state['Code']) for state in states]
        done, _ = await asyncio.wait(futures, timeout=self.Timeout)

        for fut in done:
            payload = fut.result()
            if payload is not None and len(payload) > 1:
                self.__logger.info('Total companies: %s' % len(payload))
                for company in payload:
                    code, name, state = company
                    self.__db.UpdateCompany(str(code), str(state), str(name))

    async def __aenter__(self):
        self.__client = EdgarClient(self.__params, self.__logger, self.__loop)
        self.__edgarConnection = await self.__client.__aenter__()
        self.__db = StoreManager(self.__logger)
        self.__insiderSession = self.__db.__enter__()
        self.__logger.info('Scheduler created')
        return self

    async def __aexit__(self, *args, **kwargs):
        await self.__client.__aexit__(*args, **kwargs)
        self.__db.__exit__(*args, **kwargs)
        self.__logger.info('Scheduler destroyed')
