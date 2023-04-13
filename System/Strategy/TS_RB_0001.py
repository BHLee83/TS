from System.strategy import Strategy
from System.function import Function
from System.indicator import Indicator

import pandas as pd
from datetime import datetime as dt
import logging



class TS_RB_0001():
    def __init__(self, info) -> None:
        self.logger = logging.getLogger(__class__.__name__)  # 로그 생성
        self.logger.info('Init. start')

        # General info
        self.npPriceInfo = None

        # Global setting variables
        self.dfInfo = info
        self.strName = self.dfInfo['NAME']
        self.lstAssetCode = self.dfInfo['ASSET_CODE'].split(',') # 거래대상은 여러개일 수 있음
        self.lstAssetType = self.dfInfo['ASSET_TYPE'].split(',')
        self.lstUnderId = self.dfInfo['UNDERLYING_ID'].split(',')
        self.lstTimeFrame = self.dfInfo['TIMEFRAME'].split(',')
        self.isON = bool(int(self.dfInfo['OVERNIGHT']))
        self.isPyramid = bool(int(self.dfInfo['PYRAMID']))
        self.lstTrUnit = list(map(int, self.dfInfo['TR_UNIT'].split(',')))
        self.fWeight = self.dfInfo['WEIGHT']

        self.lstProductCode = Strategy.setProductCode(self.lstUnderId)
        self.lstProductNCode = list(map(lambda x: 'KRDRVFU'+x, self.lstUnderId))    # for SHi-indi spec. 연결선물 코드
        self.lstTimeFrame_tmp = Strategy.setTimeFrame(self.lstTimeFrame)  # for SHi-indi spec.
        self.lstTimeWnd = self.lstTimeFrame_tmp[0]
        self.lstTimeIntrvl = self.lstTimeFrame_tmp[1]
        self.ix = 0 # 대상 상품의 인덱스
        self.nPosition = 0
        self.amt_entry = 0
        
        # Local setting variables
        self.lstData = [pd.DataFrame(None)] * len(self.lstAssetCode)
        self.nP1 = 5    # Period value for MA calculation
        self.nP2 = 20


    # # 과거 데이터 생성 (인디로 수신시 일봉은 연결선물, 분봉은 근월물 코드로 생성)
    # def createHistData(self, instInterface):
    #     # for i, v in enumerate(self.lstProductCode):
    #     for i, v in enumerate(self.lstProductNCode):
    #         data = Strategy.getHistData(v, self.lstTimeFrame[i])
    #         if type(data) == bool:
    #             if data == False:
    #                 instInterface.price.rqHistData(v, self.lstTimeWnd[i], self.lstTimeIntrvl[i], Strategy.strStartDate, Strategy.strEndDate, Strategy.strRqCnt)
    #                 instInterface.event_loop.exec_()


    # # 과거 데이터 로드
    # def getHistData(self):
    #     data = Strategy.getHistData(self.lstProductCode[self.ix], self.lstTimeFrame[self.ix], self.nP2+1+30)
    #     if type(data) == bool:
    #         if data == False:
    #             return pd.DataFrame(None)

    #     return data


    # 공통 프로세스
    def common(self):
        # Data load & apply
        self.lstData[self.ix] = Strategy.getHistData(self.lstProductCode[self.ix], self.lstTimeFrame[self.ix], self.nP2+1)
        if self.lstData[self.ix].empty:
            self.logger.warning('과거 데이터 로드 실패. 전략이 실행되지 않습니다.')
            return False
        self.applyChart()   # 전략 적용


    # Position check & amount setup
    def chkPos(self, amt=0):
        if amt == 0:
            self.nPosition = Strategy.getPosition(self.strName, self.lstAssetCode[self.ix], self.lstAssetType[self.ix])    # 포지션 확인 및 수량 지정
        else:
            self.nPosition += amt
        self.amt_entry = abs(self.nPosition) + self.lstTrUnit[self.ix] * self.fWeight
        self.amt_exit = abs(self.nPosition)


    # 전략 적용
    def applyChart(self):   # Strategy apply on historical chart
        df = self.lstData[self.ix]
        strColName1 = 'MA' + str(self.nP1)
        strColName2 = 'MA' + str(self.nP2)
        df[strColName1] = Indicator.MA(df['종가'], self.nP1)
        df[strColName2] = Indicator.MA(df['종가'], self.nP2)
        df['MP'] = 0
        dfSignal = pd.DataFrame(None, columns=df.columns)
        for i in df.index:
            if i < self.nP2:
                continue

            df.loc[i, 'MP'] = df.iloc[i-1]['MP']
        
            # Entry
            if Function.CrossUp(df[strColName1][i-2:i].values, df[strColName2][i-2:i].values):
                df.loc[i, 'MP'] = 1
            elif Function.CrossDown(df[strColName1][i-2:i].values, df[strColName2][i-2:i].values):
                df.loc[i, 'MP'] = -1

            # Position check
            if df.iloc[i]['MP'] != df.iloc[i-1]['MP']:
                dfSignal.loc[len(dfSignal)] = df.iloc[i]


    # 전략 실행
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행시
            tNow = dt.now().time()
            if tNow.hour < 9:   # 9시 전이면
                self.common()
                self.chkPos()

                # Entry
                df = self.lstData[self.ix]
                if df.iloc[-1]['MP'] != df.iloc[-2]['MP']:  # 포지션 변동시
                    if df.iloc[-1]['MP'] == 1:
                        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_entry, 0) # 상품코드, 매수/매도, 계약수, 가격
                        self.logger.info('Buy %s amount ordered', self.amt_entry)
                    if df.iloc[-1]['MP'] == -1:
                        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_entry, 0)
                        self.logger.info('Sell %s amount ordered', self.amt_entry)