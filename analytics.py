import pandas as pd
from datetime import datetime, timedelta


class DecisionEngine:
    """Decision engine."""

    def __init__(self, notify):
        self.__notify = notify

    def ClusterBuying(self, df, date, count):
        # filter out null
        pd.options.mode.chained_assignment = None  # default='warn'
        df['DATE'] = df['DATE'].str.strip()
        df = df[df.DATE != '0000-00-00']
        df['DATE'] = pd.to_datetime(df['DATE'])
        df = df.sort_values(by='DATE')

        first = date.replace(day=1)
        lastMonth = first - timedelta(days=1)
        fromDate = datetime(lastMonth.year, lastMonth.month, date.day)
        df = df[(df['DATE'] >= fromDate) & (df['DATE'] < date)]
        df['TYPE'] = df['TYPE'].str.strip()
        df = df[df['TYPE'] == 'P-Purchase']

        return len(df.groupby('OWNER').count()) > count
