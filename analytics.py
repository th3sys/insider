import pandas as pd
from datetime import datetime, timedelta


class DecisionEngine:
    """Decision engine."""

    def __init__(self, notify, logger):
        self.__notify = notify
        self.__logger = logger

    def ClusterBuying(self, df, date, count, cik):
        # cleanse
        pd.options.mode.chained_assignment = None  # default='warn'
        df['DATE'] = df['DATE'].str.strip()
        df = df[df.DATE != '0000-00-00']
        df['TYPE'] = df['TYPE'].str.strip()
        # convert
        df['NUMBER'] = pd.to_numeric(df['NUMBER'], errors='coerce')
        df['DATE'] = pd.to_datetime(df['DATE'])
        df = df.sort_values(by='DATE')
        # group
        df = df[df['TYPE'] == 'P-Purchase']
        # filter
        first = date.replace(day=1)
        lastMonth = first - timedelta(days=1)
        fromDate = datetime(lastMonth.year, lastMonth.month, date.day)
        dfLastMonth = df[(df['DATE'] >= fromDate) & (df['DATE'] < date)]
        pLM = len(dfLastMonth.groupby('OWNER').count())
        mLM = dfLastMonth.sum().NUMBER

        if pLM < count:
            line = (cik, pLM, 0, 0, mLM, 0, 0)
            self.__logger.info('%s, %s, %s, %s, %s, %s, %s' % line)
            return line

        dfPriorMonths = df[(df['DATE'] < fromDate)]
        pBLM = len(dfPriorMonths.groupby('OWNER').count())
        mBLM = dfPriorMonths.sum().NUMBER

        pRatio = round(pLM / pBLM if pBLM != 0 else pLM, 2)
        mRatio = round(mLM / mBLM if mBLM != 0 else mLM, 2)
        line = (cik, pLM, pBLM, pRatio, mLM, mBLM, mRatio)
        self.__logger.info('%s, %s, %s, %s, %s, %s, %s' % line)
        return line
