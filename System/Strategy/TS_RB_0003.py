from System.strategy import Strategy

import pandas as pd
from datetime import datetime as dt



class TS_RB_0003():
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
        self.ix = 0 # 대상 상품의 인덱스
        self.nPosition = 0
        self.amt = 0

        # Local setting variables
        self.lstData = [pd.DataFrame(None)] * len(self.lstAssetCode)


    # 과거 데이터 생성
    def createHistData(self, instInterface):
        for i, v in enumerate(self.lstProductNCode):
            if Strategy.getHistData(v, self.lstTimeFrame[i]) == False:
                instInterface.price.rqHistData(v, self.lstProductCode[i], self.lstTimeWnd[i], self.lstTimeIntrvl[i], Strategy.strStartDate, Strategy.strEndDate, Strategy.strRqCnt)
                instInterface.event_loop.exec_()


    # 과거 데이터 로드
    def getHistData(self):
        data = Strategy.getHistData(self.lstProductCode[self.ix], self.lstTimeFrame[self.ix])
        if data == False:
            return pd.DataFrame(None)            
        
        data = Strategy.convertNPtoDF(data)
        return data


    # 전략 적용
    def applyChart(self):   # Strategy apply on historical chart
        df = self.lstData[self.ix].sort_index(ascending=False).reset_index()
        lstMonth_close = []
        df['MP'] = 0
        for i in df.index-1:
            if i > 0:
                df.loc[i, 'MP'] = df['MP'][i-1]
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
        df = df.sort_index(ascending=False).reset_index()
        self.lstData[self.ix]['MP'] = df['MP']


    # 전략
    def execute(self, PriceInfo):
        if PriceInfo == 0:  # 최초 실행인 경우에만
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
                    self.amt = abs(self.nPosition) + self.lstTrUnit[self.ix]*self.fWeight
                    df = self.lstData[self.ix]
                    if df['MP'][1] != df['MP'][2]:  # 포지션 변동시
                        if df['MP'][1] == 1:
                            Strategy.setOrder(self, self.lstProductCode[self.ix], 'B', self.amt, 0) # 상품코드, 매수/매도, 계약수, 가격
                            df['MP'][0] = 1
                        elif df['MP'][1] == -1:
                            Strategy.setOrder(self, self.lstProductCode[self.ix], 'S', self.amt, 0)
                            df['MP'][0] = -1