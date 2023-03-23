from System.strategy import Strategy
from System.function import Function
from System.indicator import Indicator

import pandas as pd
from datetime import datetime as dt
import logging



class TS_RB_0001():
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

        self.lstProductNCode = list(map(lambda x: 'KRDRVFU'+x, self.lstUnderId))    # for SHi-indi spec. 연결선물 코드
        self.lstProductCode = Strategy.setProductCode(self.lstUnderId)
        self.lstTimeFrame_tmp = Strategy.setTimeFrame(self.lstTimeFrame)  # for SHi-indi spec.
        self.lstTimeWnd = self.lstTimeFrame_tmp[0]
        self.lstTimeIntrvl = self.lstTimeFrame_tmp[1]
        self.ix = 0 # 대상 상품의 인덱스
        self.nPosition = 0
        self.amt = 0

        # Local setting variables
        self.lstData = [pd.DataFrame(None)] * len(self.lstAssetCode)
        self.nP1 = 5    # Period value for MA calculation
        self.nP2 = 20


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
        
        return data


    # 전략 적용
    def applyChart(self):   # Strategy apply on historical chart
        df = self.lstData[self.ix].sort_index(ascending=False).reset_index()
        strColName1 = 'MA' + str(self.nP1)
        strColName2 = 'MA' + str(self.nP2)
        df[strColName1] = Indicator.MA(df['종가'], self.nP1)
        df[strColName2] = Indicator.MA(df['종가'], self.nP2)
        df['MP'] = 0
        for i in df.index:
            if i < self.nP2:
                continue
            df.loc[i, 'MP'] = df['MP'][i-1]
            if Function.CrossUp(df[strColName1][i-2:i].values, df[strColName2][i-2:i].values):
                df.loc[i, 'MP'] = 1
            elif Function.CrossDown(df[strColName1][i-2:i].values, df[strColName2][i-2:i].values):
                df.loc[i, 'MP'] = -1
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
                    self.nPosition = Strategy.getPosition(self.dfInfo['NAME'], self.lstAssetCode[self.ix], self.lstAssetType[self.ix])    # 포지션 확인 및 수량 지정
                    self.amt = abs(self.nPosition) + self.lstTrUnit[self.ix] * self.fWeight
                    df = self.lstData[self.ix]
                    if df['MP'][0] != df['MP'][1]:  # 포지션 변동시
                        if df['MP'][0] == 1:
                            Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt, 0) # 상품코드, 매수/매도, 계약수, 가격
                            self.logger.info('Buy %s amount ordered', self.amt)
                        if df['MP'][0] == -1:
                            Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt, 0)
                            self.logger.info('Sell %s amount ordered', self.amt)