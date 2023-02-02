from datetime import datetime as dt
import pandas as pd

import sys
from os import path
wd = path.dirname( path.dirname( path.abspath(__file__) ) )
sys.path.append(wd)
from DB.dbconn import oracleDB



# Raw data
f = dt.today().strftime('%Y%m%d')
# f = '20230125'
df = pd.read_csv('C:/Infomax/bin/usr/scinfo/local/Data/' + f + '.csv', encoding='euc-kr')
df.sort_index(axis=0, ascending=False, inplace=True)
df.reset_index(inplace=True)
df.reset_index(inplace=True)
df['[일시]통합코드―USDSP―중간값'] = f[:4] + "-" + f[4:6] + "-" + f[6:8] + ' ' + df['시간']
df.drop(columns=['index', '시간', '전일대비', '전일등락율'], inplace=True)
df['[일시]통합코드―USDSP―중간값'] = df['[일시]통합코드―USDSP―중간값'].astype('datetime64[ns]', copy=False)

# Result
dfResult = pd.DataFrame(None, columns=['BASE_DATETIME', 'NIX', 'ASSET_NAME', 'ASSET_TYPE', 'MATURITY', 'PRICE', 'VOLUME'])
dfResult['BASE_DATETIME'] = df['[일시]통합코드―USDSP―중간값']
dfResult['NIX'] = df['level_0']
dfResult['ASSET_NAME'] = 'USDKRW'
dfResult['ASSET_TYPE'] = 'SPOT'
dfResult['MATURITY'] = None
dfResult['PRICE'] = df['종가']
dfResult['VOLUME'] = None

# Insert
db = oracleDB('oradb1')
db.executemany("INSERT INTO market_data_tick VALUES (:1, :2, :3, :4, :5, :6, :7)", dfResult.values.tolist())
db.commit()