# - Timeframe: 일봉
# - Entry
#   Long(Short): Reference Deviation Value가 Long(Short) Threshold보다 클(작을) 경우 시초가에
# - Exit
#   EL(ES): Reference Deviation Value가 0보다 작을(클) 경우 시초가에

from System.strategy import Strategy
from System.indicator import Indicator

import pandas as pd
from datetime import datetime as dt
import logging



class TS_RB_0037():
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
        self.boolON = bool(int(self.dfInfo['OVERNIGHT']))
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
        self.nLen = 15
        self.fETLong = 5.0
        self.fETShort = -5.0
        
        
    # 과거 데이터 생성 (인디로 수신시 일봉은 연결선물, 분봉은 근월물 코드로 생성)
    def createHistData(self, instInterface):
        for i, v in enumerate(self.lstProductNCode):
        # for i, v in enumerate(self.lstProductCode):
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
        
        df.insert(len(df.columns), 'RMA', Indicator.MA(df['종가'], self.nLen))
        df['DRD'] = df['종가'] - df['RMA']
        df['NDV'] = df['RMA'].rolling(window=self.nLen).sum()
        df['TDV'] = abs(df['RMA']).rolling(window=self.nLen).sum()
        df['RDV'] = df['NDV'] / df['TDV'] * 100

        df['MP'] = 0
        df['EntryLv'] = 0.0
        df['ExitLv'] = 0.0
        for i in df.index-1:
            if i < self.nLen:
                continue

            df.loc[i, 'MP'] = df['MP'][i-1]
            df.loc[i, 'EntryLv'] = df['EntryLv'][i-1]
            df.loc[i, 'ExitLv'] = df['ExitLv'][i-1]
            
            # Entry
            if df['MP'][i] != 1:
                if df['RDV'][i-1] > self.fETLong:
                    df.loc[i, 'MP'] = 1
                    df.loc[i, 'EntryLv'] = df['시가'][i]
            if df['MP'][i] != -1:
                if df['RDV'][i-1] < self.fETShort:
                    df.loc[i, 'MP'] = -1
                    df.loc[i, 'EntryLv'] = df['시가'][i]
                    
            # Exit
            if df['MP'][i-1] == 1 and df['RDV'][i-1] < 0:
                df.loc[i, 'ExitLv'] = df['시가'][i]
                df.loc[i, 'MP'] = 0
            if df['MP'][i-1] == -1 and df['RDV'][i-1] > 0:
                df.loc[i, 'ExitLv'] = df['시가'][i]
                df.loc[i, 'MP'] = 0

        df = df.sort_index(ascending=False).reset_index()
        self.lstData[self.ix]['MP'] = df['MP']
        self.lstData[self.ix]['RDV'] = df['RDV']


    # 전략 실행
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행
            self.lstData[self.ix] = self.getHistData()  # 과거 데이터 수신
            if self.lstData[self.ix].empty:
                self.logger.warning('과거 데이터 로드 실패. 전략이 실행되지 않습니다.')
                return False
            else:
                self.applyChart()   # 전략 적용 (chart)
                self.nPosition = Strategy.getPosition(self.dfInfo['NAME'], self.lstAssetCode[self.ix], self.lstAssetType[self.ix])    # 포지션 확인 및 수량 지정
                self.amt_entry = abs(self.nPosition) + self.lstTrUnit[self.ix] * self.fWeight
                self.amt_exit = abs(self.nPosition)

                df = self.lstData[self.ix]
                df.loc[0, 'MP'] = df['MP'][1]
                tNow = dt.now().time()
                if tNow.hour < 9:   # 9시 전이면
                    # Entry
                    if df['MP'][0] != 1 and df['RDV'][1] > self.fETLong:
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt_entry, 0)
                        self.logger.info('Buy %s amount ordered', self.amt_entry)
                        df.loc[0, 'MP'] = 1
                    if df['MP'][0] != -1 and df['RDV'][1] < self.fETShort:
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt_entry, 0)
                        self.logger.info('Sell %s amount ordered', self.amt_entry)
                        df.loc[0, 'MP'] = -1
                    # Exit
                    if df['MP'][0] == 1 and df['RDV'][1] < 0:
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'EL', self.amt_entry, 0)
                        self.logger.info('ExitLong %s amount ordered', self.amt_entry)
                        df.loc[0, 'MP'] = 0
                    if df['MP'][0] == -1 and df['RDV'][1] > 0:
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'ES', self.amt_entry, 0)
                        self.logger.info('ExitShort %s amount ordered', self.amt_entry)
                        df.loc[0, 'MP'] = 0

            # self.npPriceInfo = PriceInfo.copy()