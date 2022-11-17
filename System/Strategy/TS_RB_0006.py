from System.strategy import Strategy

import pandas as pd
from datetime import datetime as dt



class TS_RB_0006():
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
        self.nWeek = 4


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
        df['dt'] = pd.to_datetime(df['일자'])
        df['woy'] = list(map(lambda x: x.weekofyear, df['dt']))
        df['MP'] = 0
        df['chUpper'] = 0.0
        df['chLower'] = 0.0
        for i in df.index:
            if i >= (self.nWeek + 1) * 5:
                df.loc[i, 'MP'] = df['MP'][i-1]
                df.loc[i, 'chUpper'] = df['chUpper'][i-1]
                df.loc[i, 'chLower'] = df['chLower'][i-1]
                    
                cnt = 0 # nWeek 주 가격 탐색
                for j in range(i, 1, -1):
                    if df['woy'][j] != df['woy'][j-1]:
                        cnt += 1
                        if cnt == 1:
                            ixEnd = j
                        if cnt == self.nWeek+1:
                            ixStart = j
                            break
                df.loc[i, 'chUpper'] = df['고가'][ixStart:ixEnd].max()  # nWeek 주 고가
                df.loc[i, 'chLower'] = df['저가'][ixStart:ixEnd].min()  # nWeek 주 저가
                if df['고가'][i] >= df['chUpper'][i]:   # 채널 상단 돌파 매수
                    df.loc[i, 'MP'] = 1
                if df['저가'][i] <= df['chLower'][i]:   # 채널 하단 돌파 매도
                    df.loc[i, 'MP'] = -1
        df = df.sort_index(ascending=False).reset_index()
        self.lstData[ix]['MP'] = df['MP']
        self.lstData[ix]['chUpper'] = df['chUpper']
        self.lstData[ix]['chLower'] = df['chLower']


    # 전략
    def execute(self, PriceInfo):
        ix = 0  # 대상 상품의 인덱스                
        if PriceInfo == 0:  # 최초 실행인 경우에만
            self.lstData[ix] = self.getHistData(ix)
            if self.lstData[ix].empty:
                return False
            else:
                self.applyChart(ix)
        else:
            if self.npPriceInfo != None:
                amt = abs(self.lstData[ix]['MP'][0]) * self.lstTrUnit[ix] * self.fWeight * 2
                if self.lstData[ix]['MP'][0] < 0:
                    if (self.npPriceInfo['현재가'] < self.lstData[ix]['chUpper'][0]) and (PriceInfo['현재가'] >= self.lstData[ix]['chUpper'][0]): # 채널 상단 터치시
                        Strategy.setOrder(self, self.lstProductCode[ix], 'B', amt, PriceInfo['현재가'])   # 매수
                        self.lstData[ix]['MP'][0] = 1
                if self.lstData[ix]['MP'][0] > 0:
                    if (self.npPriceInfo['현재가'] > self.lstData[ix]['chLower'][0]) and (PriceInfo['현재가'] <= self.lstData[ix]['chLower'][0]): # 채널 하단 터치시
                        Strategy.setOrder(self, self.lstProductCode[ix], 'S', amt, PriceInfo['현재가'])   # 매도
                        self.lstData[ix]['MP'][0] = -1
            self.npPriceInfo = PriceInfo.copy()