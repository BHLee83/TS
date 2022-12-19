from System.strategy import Strategy
from System.function import Function

import pandas as pd
from datetime import datetime as dt



class TS_RB_0008():
    def __init__(self, info) -> None:
        super().__init__()

        # General info
        self.npPriceInfo = None

        # Global setting variables
        self.dfInfo = info
        self.lstAssetCode = self.dfInfo['ASSET_CODE'].split(',') # 거래대상은 여러개일 수 있음
        self.lstAssetType = self.dfInfo['ASSET_TYPE'].split(',')
        self.lstUnderId = self.dfInfo['UNDERLYING_ID'].split(',')
        self.lstTimeFrame = self.dfInfo['TIMEFRAME'].split(',')
        self.lstTrUnit = list(map(int, self.dfInfo['TR_UNIT'].split(',')))
        self.fWeight = self.dfInfo['WEIGHT']

        self.lstProductNCode = list(map(lambda x: 'KRDRVFU'+x, self.lstUnderId))    # for SHi-indi spec. 연결선물 코드
        self.lstProductCode = Strategy.setProductCode(self.lstUnderId)
        self.lstTimeFrame_tmp = Strategy.setTimeFrame(self.lstTimeFrame)  # for SHi-indi spec.
        self.lstTimeWnd = self.lstTimeFrame_tmp[0]
        self.lstTimeIntrvl = self.lstTimeFrame_tmp[1]
        self.ix = 0 # 대상 상품의 인덱스
        self.nPosition = 0
        self.amt_entry = 0
        self.amt_exit = 0

        # Local setting variables
        self.lstData = [pd.DataFrame(None)] * len(self.lstAssetCode)
        self.nP = 80    # Period value for MA calculation
        self.nMulti = 1


    # 과거 데이터 생성 (인디로 수신시 일봉은 연결선물, 분봉은 근월물 코드로 생성)
    def createHistData(self, instInterface):
        # for i, v in enumerate(self.lstProductCode):
        for i, v in enumerate(self.lstProductNCode):
            if Strategy.getHistData(v, self.lstTimeFrame[i]) == False:
                instInterface.price.rqHistData(v, self.lstTimeWnd[i], self.lstTimeIntrvl[i], Strategy.strStartDate, Strategy.strEndDate, Strategy.strRqCnt)
                instInterface.event_loop.exec_()


    # 과거 데이터 로드
    def getHistData(self):
        data = Strategy.getHistData(self.lstProductNCode[self.ix], self.lstTimeFrame[self.ix])
        if type(data) == bool:
            if data == False:
                return pd.DataFrame(None)
        
        data = Strategy.convertNPtoDF(data)
        return data


    # 전략 적용
    def applyChart(self):   # Strategy apply on historical chart
        df = self.lstData[self.ix].sort_index(ascending=False).reset_index()
        strColName1 = 'MA' + str(self.nP)
        strColName2 = 'BBUP'
        strColName3 = 'BBDN'
        df[strColName1] = df['종가'].rolling(window=self.nP).mean()
        std = df['종가'].rolling(window=self.nP).std()
        df[strColName2] = df[strColName1] + self.nMulti * std
        df[strColName3] = df[strColName1] - self.nMulti * std
        df['MP'] = 0
        for i in df.index:
            if i > self.nP:
                df.loc[i, 'MP'] = df['MP'][i-1]
                if Function.CrossUp(df['종가'][i-1:i+1].values, df[strColName2][i-1:i+1].values):   # Entry
                    df.loc[i, 'MP'] = 1
                if Function.CrossDown(df['종가'][i-1:i+1].values, df[strColName3][i-1:i+1].values):
                    df.loc[i, 'MP'] = -1
                if Function.CrossUp(df['종가'][i-1:i+1].values, df[strColName1][i-1:i+1].values):   # Exit
                    df.loc[i, 'MP'] = 0
                if Function.CrossDown(df['종가'][i-1:i+1].values, df[strColName1][i-1:i+1].values):
                    df.loc[i, 'MP'] = 0
        df = df.sort_index(ascending=False).reset_index()
        self.lstData[self.ix]['MP'] = df['MP']


    # 전략 실행
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행인 경우에만
            tNow = dt.now().time()
            if tNow.hour < 9:   # 9시 전이면
                self.lstData[self.ix] = self.getHistData()
                if self.lstData[self.ix].empty:
                    return False
                else:
                    self.applyChart()
                    try:
                        self.nPosition = Strategy.dfPosition['POSITION'][Strategy.dfPosition['STRATEGY_ID']==__class__.__name__ \
                                            and Strategy.dfPosition['ASSET_NAME']==self.lstAssetCode[self.ix] \
                                            and Strategy.dfPosition['ASSET_TYPE']==self.lstAssetType[self.ix]].values[0]
                    except:
                        self.nPosition = 0
                    self.amt_entry = abs(self.nPosition) + self.lstTrUnit[self.ix] * self.fWeight
                    self.amt_exit = abs(self.nPosition)
                    df = self.lstData[self.ix]
                    if df['MP'][1] != df['MP'][2]:  # 포지션 변동시
                        # Entry
                        if df['MP'][1] == 1:
                            Strategy.setOrder(self, self.lstProductCode[self.ix], 'B', self.amt_entry, 0)   # 상품코드, 매수/매도, 계약수, 가격
                            df.loc[0, 'MP'] = 1
                        if df['MP'][1] == -1:
                            Strategy.setOrder(self, self.lstProductCode[self.ix], 'S', self.amt_entry, 0)
                            df.loc[0, 'MP'] = -1
                        # Exit
                        if df['MP'][1] == 0:
                            if df['MP'][2] == -1:
                                Strategy.setOrder(self, self.lstProductCode[self.ix], 'B', self.amt_exit, 0)
                            if df['MP'][2] == 1:
                                Strategy.setOrder(self, self.lstProductCode[self.ix], 'S', self.amt_exit, 0)
                            df.loc[0, 'MP'] = 0