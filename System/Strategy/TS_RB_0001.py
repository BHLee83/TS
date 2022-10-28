from System.strategy import Strategy
from System.indicator import Indicator
from System.function import Function

import pandas as pd
import time
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
        # self.lstTrUnit = list(map(int, self.lstTrUnit))
        self.fWeight = 100

        self.lstProductCode = Strategy.setProductCode(self.lstUnderId)
        self.lstTimeFrame_tmp = Strategy.setTimeFrame(self.lstTimeFrame)  # for SHi-indi spec.
        self.lstTimeWnd = self.lstTimeFrame_tmp[0]
        self.lstTimeIntrvl = self.lstTimeFrame_tmp[1]

        # Local setting variables
        self.nP1 = 5    # Period value for MA calculation
        self.nP2 = 20


    # 과거 데이터 생성 (DB에서 가져오게 변경 필요)
    def createHistData(self, price):
        for i, v in enumerate(self.lstProductCode):
            while Strategy.getHistData(v, self.lstTimeFrame[i]) == False:  # 과거 데이터는 즉시 수신하지 못할 수 있음
                price.rqHistData(v, self.lstTimeWnd[i], self.lstTimeIntrvl[i], Strategy.strStartDate, Strategy.strEndDate, Strategy.strRqCnt)
                time.sleep(1)


    # 과거 데이터 로드
    def getHistData(self, ix):
        data = Strategy.getHistData(self.lstProductCode[ix], self.lstTimeFrame[ix])
        if data == False:
            return pd.DataFrame(None)            
        
        data = Strategy.convertNPtoDF(data)
        return data


    # 전략
    def execute(self, PriceInfo):
        if PriceInfo == 0:  # 최초 실행인 경우에만
            tNow = dt.now().time()
            if tNow.hour < 9:   # 9시 전이면
                ix = 0  # 대상 상품의 인덱스                
                data = self.getHistData(ix)
                if data.empty:
                    return False
                else:
                    shortMA = Indicator.ma(data, '종가', self.nP1, False)
                    longMA = Indicator.ma(data, '종가', self.nP2, False)
                    
                    if Function.CrossUp(shortMA, longMA):
                            Strategy.setOrder(self, self.lstProductCode[ix], 'B', self.lstTrUnit[ix], 0) # 매수/매도, 계약수, 가격
                    elif Function.CrossDown(shortMA, longMA):
                            Strategy.setOrder(self, self.lstProductCode[ix], 'S', self.lstTrUnit[ix], 0)

            Strategy.setOrder(self, self.lstProductCode[0], 'S', self.lstTrUnit[0], 0) # test