from System.strategy import Strategy

import pandas as pd
from datetime import datetime as dt



class TS_RB_0004():
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

        self.lstProductCode = Strategy.setProductCode(self.lstUnderId)
        self.lstProductNCode = list(map(lambda x: 'KRDRVFU'+x, self.lstUnderId))    # for SHi-indi spec. 연결선물 코드
        self.lstTimeFrame_tmp = Strategy.setTimeFrame(self.lstTimeFrame)  # for SHi-indi spec.
        self.lstTimeWnd = self.lstTimeFrame_tmp[0]
        self.lstTimeIntrvl = self.lstTimeFrame_tmp[1]

        # Local setting variables
        self.lstData = [pd.DataFrame(None)] * len(self.lstAssetCode)


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
        lstMonth_close = []
        for i in df.index:
            if int(i) < 1:
                df.loc[i, 'MP'] = 0
                continue
            else:
                nLast_month = df.loc[i-1, '일자'][4:6]
                nCurrent_month = df.loc[i, '일자'][4:6]
                if nCurrent_month != nLast_month:    # 월 변경시
                    if any(nLast_month==3, nLast_month==6, nLast_month==9, nLast_month==12):    # 분기마다
                        lstMonth_close.append(df.loc[i-1, '종가'])
                        if len(lstMonth_close) > 1:
                            if lstMonth_close[-1] > lstMonth_close[-2]:
                                df.loc[i-1, 'MP'] = 1
                            elif lstMonth_close[-1] < lstMonth_close[-2]:
                                df.loc[i-1, 'MP'] = -1
                df.loc[i, 'MP'] = df.loc[i-1, 'MP']
        df = df.sort_index(ascending=False).reset_index()
        self.lstData[ix]['MP'] = df['MP']


    # 전략
    def execute(self, PriceInfo):
        if PriceInfo == 0:  # 최초 실행인 경우에만
            ix = 0  # 대상 상품의 인덱스                
            self.lstData[ix] = self.getHistData(ix)
            if self.lstData[ix].empty:
                return False
            else:
                self.applyChart(ix)
        else:
            if self.lstData[ix]['MP'][1] != self.lstData[ix]['MP'][2]:  # 포지션 변동시
                amt = abs(self.lstData[ix]['MP'][1] - self.lstData[ix]['MP'][2])    # 변동된 수량만큼
                if self.lstData[ix]['MP'][1] == 1:
                        Strategy.setOrder(self, self.lstProductCode[ix], 'B', amt*self.lstTrUnit[ix]*self.fWeight, 0) # 상품코드, 매수/매도, 계약수, 가격
                elif self.lstData[ix]['MP'][1] == -1:
                        Strategy.setOrder(self, self.lstProductCode[ix], 'S', amt*self.lstTrUnit[ix]*self.fWeight, 0)