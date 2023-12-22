# - Timeframe: 일봉
# - Setup: SLR (Simple Linear Regression)
# - Entry: fastLR이 slowLR을 CrossUp(Down)시 다음봉 시가에 매수(매도)
# - Exit: 없음 
# - Fee: 없음 (거래횟수가 적어 무시 가능)
# - Slippage: 0.005


from System.strategy import Strategy

import pandas as pd
from datetime import datetime as dt
import logging
import talib as ta


class TS_RB_0041():
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
        self.nFastLen = 10
        self.nSlowLen = 20


    # 공통 프로세스
    def common(self):
        # Data load & apply
        self.lstData[self.ix] = Strategy.getHistData(self.lstProductCode[self.ix], self.lstTimeFrame[self.ix], self.nSlowLen*10)
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
        fastLR = ta.LINEARREG(df['종가'], timeperiod=self.nFastLen)
        slowLR = ta.LINEARREG(df['종가'], timeperiod=self.nSlowLen)
        df.insert(len(df.columns), "fastLR", fastLR)
        df.insert(len(df.columns), "slowLR", slowLR)
        df['MP'] = 0
        # dfSignal = pd.DataFrame(None, columns=df.columns)
        for i in df.index:
            if i < self.nSlowLen:
                continue

            df.loc[i, 'MP'] = df['MP'][i-1]
        
            # Entry
            if df['MP'][i] <= 0:
                if (df['fastLR'][i-2] <= df['slowLR'][i-2]) and (df['fastLR'][i-1] >= df['slowLR'][i-1]):
                    df.loc[i, 'MP'] = 1
            if df['MP'][i] >= 0:
                if (df['fastLR'][i-2] >= df['slowLR'][i-2]) and (df['fastLR'][i-1] <= df['slowLR'][i-1]):
                    df.loc[i, 'MP'] = -1

            # # Position check
            # if df.iloc[i]['MP'] != df.iloc[i-1]['MP']:
            #     dfSignal.loc[len(dfSignal)] = df.iloc[i]


    # 전략 실행
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행시
            tNow = dt.now().time()
            if tNow.hour < Strategy.MARKETOPEN_HOUR:   # 장 시작 전이면
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