from System.strategy import Strategy

import pandas as pd
from datetime import datetime as dt
import logging



class TS_RB_0003():
    def __init__(self, info) -> None:
        super().__init__()
        self.logger = logging.getLogger(__class__.__name__)  # 로그 생성
        self.logger.info('Init. start')
    
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


    # 과거 데이터 생성 (인디로 수신시 일봉은 연결선물, 분봉은 근월물 코드로 생성)
    def createHistData(self, instInterface):
        # for i, v in enumerate(self.lstProductCode):
        for i, v in enumerate(self.lstProductNCode):
            data = Strategy.getHistData(v, self.lstTimeFrame[i])
            if type(data) == bool:
                if data == False:
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
        lstMonth_close = []
        df['MP'] = 0
        for i in df.index:
            if i < 1:
                continue
            df.loc[i, 'MP'] = df['MP'][i-1]
            nLast_month = int(df.loc[i-1, '일자'][4:6])
            nCurrent_month = int(df.loc[i, '일자'][4:6])
            if nCurrent_month != nLast_month:    # 월 변경시
                if any([nLast_month==3, nLast_month==6, nLast_month==9, nLast_month==12]):  # 분기마다
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
        if type(PriceInfo) == int:  # 최초 실행인 경우에만
            tNow = dt.now().time()
            if tNow.hour < 9:   # 9시 전이면
                self.lstData[self.ix] = self.getHistData()
                if self.lstData[self.ix].empty:
                    return False
                else:
                    self.applyChart()
                    self.nPosition = Strategy.getPosition(self.dfInfo['NAME'], self.lstAssetCode[self.ix], self.lstAssetType[self.ix])    # 포지션 확인 및 수량 지정
                    self.amt = abs(self.nPosition) + self.lstTrUnit[self.ix]*self.fWeight
                    df = self.lstData[self.ix]
                    if df['MP'][0] != df['MP'][1]:  # 포지션 변동시
                        if df['MP'][0] == 1:
                            Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt, 0) # 상품코드, 매수/매도, 계약수, 가격
                            self.logger.info('Buy %s amount ordered', self.amt)
                        elif df['MP'][0] == -1:
                            Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt, 0)
                            self.logger.info('Sell %s amount ordered', self.amt)