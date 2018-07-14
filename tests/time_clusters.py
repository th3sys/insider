import pandas as pd
from datetime import datetime, timedelta
import json
import boto3

# df = pd.DataFrame({"ID":["A", "A", "C" ,"B", "B"], "date":["06/24/2014","06/25/2014","06/23/2014","07/02/1999","07/02/1999"], "value": ["3","5","1","7","8"] })
df = pd.read_csv('1378706.csv')
print(df.head(3))
cik = '1378706'
date = datetime.strptime('2016-01-01', '%Y-%m-%d')
print(date.strftime('%Y%m%d'))
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

dfPriorMonths = df[(df['DATE'] < fromDate)]
pBLM = len(dfPriorMonths.groupby('OWNER').count())
mBLM = dfPriorMonths.sum().NUMBER

pRatio = round(pLM/pBLM if pBLM is not 0 else pLM, 2)
mRatio = round(mLM/mBLM if mBLM is not 0 else mLM, 2)
line = (cik, pLM, pBLM, pRatio, mLM, mBLM, mRatio)
print("%s, %s, %s, %s, %s, %s, %s\na" % line)