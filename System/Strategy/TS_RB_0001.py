from System.strategy import Strategy
from System.function import Function

import pandas as pd
from datetime import datetime as dt



class TS_RB_0001():
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

        # Local setting variables
        self.lstData = [pd.DataFrame(None)] * len(self.lstAssetCode)
        self.nP1 = 5    # Period value for MA calculation
        self.nP2 = 20


    # 과거 데이터 생성
    def createHistData(self, instInterface):
        for i, v in enumerate(self.lstProductNCode):
            if Strategy.getHistData(v, self.lstTimeFrame[i]) == False:
                instInterface.price.rqHistData(v, self.lstProductCode[i], self.lstTimeWnd[i], self.lstTimeIntrvl[i], Strategy.strStartDate, Strategy.strEndDate, Strategy.strRqCnt)
                instInterface.event_loop.exec_()


    # 과거 데이터 로드
    def getHistData(self, ix):
        data = Strategy.getHistData(self.lstProductCode[ix], self.lstTimeFrame[ix])
        if data == False:
            return pd.DataFrame(None)            
        
        data = Strategy.convertNPtoDF(data)
        return data


    # 전략 적용
    def applyChart(self, ix):   # Strategy apply on historical chart
        df = self.lstData[ix].sort_index(ascending=False).reset_index()
        strColName1 = 'MA' + str(self.nP1)
        strColName2 = 'MA' + str(self.nP2)
        df[strColName1] = df['종가'].rolling(window=self.nP1).mean()
        df[strColName2] = df['종가'].rolling(window=self.nP2).mean()
        for i in df.index:
            if int(i) < 1:
                df.loc[i, 'MP'] = 0
                continue
            if Function.CrossUp(df[strColName1][i-1:i+1].values, df[strColName2][i-1:i+1].values):
                df.loc[i, 'MP'] = 1
            elif Function.CrossDown(df[strColName1][i-1:i+1].values, df[strColName2][i-1:i+1].values):
                df.loc[i, 'MP'] = -1
            else:
                df.loc[i, 'MP'] = df.loc[i-1, 'MP']
        df = df.sort_index(ascending=False).reset_index()
        self.lstData[ix]['MP'] = df['MP']


    # 전략 실행
    def execute(self, PriceInfo):
        if PriceInfo == 0:  # 최초 실행인 경우에만
            tNow = dt.now().time()
            if tNow.hour < 9:   # 9시 전이면
                ix = 0  # 대상 상품의 인덱스                
                self.lstData[ix] = self.getHistData(ix)
                if self.lstData[ix].empty:
                    return False
                else:
                    self.applyChart(ix)
                    if self.lstData[ix]['MP'][1] != self.lstData[ix]['MP'][2]:  # 포지션 변동시
                        amt = abs(self.lstData[ix]['MP'][1] - self.lstData[ix]['MP'][2])    # 변동된 수량만큼
                        if self.lstData[ix]['MP'][1] == 1:
                                Strategy.setOrder(self, self.lstProductCode[ix], 'B', amt*self.lstTrUnit[ix]*self.fWeight, 0) # 상품코드, 매수/매도, 계약수, 가격
                        elif self.lstData[ix]['MP'][1] == -1:
                                Strategy.setOrder(self, self.lstProductCode[ix], 'S', amt*self.lstTrUnit[ix]*self.fWeight, 0)

            Strategy.setOrder(self, self.lstProductCode[0], 'S', self.lstTrUnit[0], 0) # test