import pandas as pd
from datetime import datetime, timedelta


class DecisionEngine:
    """Decision engine."""

    def __init__(self, notify):
        self.__notify = notify

    def ClusterBuying(self, df, date, count):
        df['DATE'] = pd.to_datetime(df['DATE'])
        df = df.sort_values(by='DATE')

        first = date.replace(day=1)
        lastMonth = first - timedelta(days=1)
        fromDate = datetime(date.year, lastMonth.month, date.day)
        df = df[(df['DATE'] >= fromDate) & (df['DATE'] < date)]
        df['TYPE'] = df['TYPE'].str.strip()
        df = df[df['TYPE'] == 'P-Purchase']

        return len(df.groupby('OWNER').count()) > count
