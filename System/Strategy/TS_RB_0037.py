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
        self.amt_exit = 0

        # Local setting variables
        self.lstData = [pd.DataFrame(None)] * len(self.lstAssetCode)
        self.nLen = 15
        self.fETLong = 5.0
        self.fETShort = -5.0
        
        
    # 공통 프로세스
    def common(self):
        # Data load & apply
        self.lstData[self.ix] = Strategy.getHistData(self.lstProductCode[self.ix], self.lstAssetType[self.ix], self.lstTimeFrame[self.ix], self.nLen*4)
        if self.lstData[self.ix].empty:
            self.logger.warning('과거 데이터 로드 실패. 전략이 실행되지 않습니다.')
            return False
        self.applyChart()   # 전략 적용


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
        
        df.insert(len(df.columns), 'RMA', Indicator.MA(df['종가'], self.nLen))
        df['DRD'] = df['종가'] - df['RMA']
        df['NDV'] = df['DRD'].rolling(window=self.nLen).sum()
        df['TDV'] = abs(df['DRD']).rolling(window=self.nLen).sum()
        df['RDV'] = df['NDV'] / df['TDV'] * 100

        df['MP'] = 0
        df['EntryLv'] = 0.0
        df['ExitLv'] = 0.0
        for i in df.index:
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


    # 전략 실행
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행시
            tNow = dt.now().time()
            if tNow.hour < Strategy.MARKETOPEN_HOUR:   # 장 시작 전이면
                self.common()
                self.chkPos()
            
                df = self.lstData[self.ix]
                if df.iloc[-1]['MP'] != df.iloc[-2]['MP']:  # 포지션 변동시
                    # Entry
                    if df.iloc[-1]['MP'] == 1:
                        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_entry, 0)
                        self.logger.info('Buy %s amount ordered', self.amt_entry)
                    if df.iloc[-1]['MP'] == -1:
                        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_entry, 0)
                        self.logger.info('Sell %s amount ordered', self.amt_entry)
                    # Exit
                    if df.iloc[-1]['MP'] == 0:
                        if df.iloc[-2]['MP'] == 1:
                            Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'EL', self.amt_exit, 0)
                            self.logger.info('ExitLong %s amount ordered', self.amt_exit)
                        if df.iloc[-2]['MP'] == -1:
                            Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'ES', self.amt_exit, 0)
                            self.logger.info('ExitShort %s amount ordered', self.amt_exit)