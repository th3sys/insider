import pandas as pd
# df = pd.DataFrame({"ID":["A", "A", "C" ,"B", "B"], "date":["06/24/2014","06/25/2014","06/23/2014","07/02/1999","07/02/1999"], "value": ["3","5","1","7","8"] })
df = pd.read_csv('918541.csv')
print(df.head(3))

df['DATE'] = pd.to_datetime(df['DATE'])
df = df.sort_values(by='DATE')

df = df[(df['DATE'] >= '2014-05-01') & (df['DATE'] < '2014-06-01')]
df['TYPE'] = df['TYPE'].str.strip()
df = df[df['TYPE'] == 'S-Sale']

if len(df.groupby('OWNER').count()) > 2:
    print(df.count())
df['group'] = (df['DATE'].diff() > pd.Timedelta(days=2)).cumsum()
print(df)