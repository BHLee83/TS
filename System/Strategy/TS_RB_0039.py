# - Timeframe: 일봉
# - Setup: 최근 10개봉 내 종가 상승시 +1, 하락시 -1로 TrendScore 점수 산출
# - Entry: TrendScore가 TSA보다 크고(작고), 종가가 단순이동평균보다 클(작을) 때
# - Exit: 없음 
# - Fee: 없음 (거래횟수가 적어 무시 가능)
# - Slippage: 0.005

from System.strategy import Strategy
from System.indicator import Indicator

import pandas as pd
from datetime import datetime as dt
import logging



class TS_RB_0039():
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
        self.nP = 20
        
        
    # 공통 프로세스
    def common(self):
        # Data load & apply
        self.lstData[self.ix] = Strategy.getHistData(self.lstProductCode[self.ix], self.lstTimeFrame[self.ix], self.nP*2)
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
        
        df['MP'] = 0
        df['EntryLv'] = 0.0
        df['ExitLv'] = 0.0
        df['TrendScore'] = 0

        # Setup
        TrendScore = 0
        for i in df.index:
            if i < 10:
                continue
            TrendScore = 0
            for j in range(10):
                if df['종가'][i] >= df['종가'][i-j-1]:
                    TrendScore += 1
                else:
                    TrendScore -= 1
            df.loc[i, 'TrendScore'] = TrendScore

        df['TSA'] = Indicator.MA(df['TrendScore'], self.nP)
        df['SMA'] = Indicator.MA(df['종가'], self.nP)

        for i in df.index:
            if i < self.nP:
                continue

            df.loc[i, 'MP'] = df['MP'][i-1]
            df.loc[i, 'EntryLv'] = df['EntryLv'][i-1]
            df.loc[i, 'ExitLv'] = df['ExitLv'][i-1]
            
            # Entry
            if df['MP'][i] != 1:
                if (df['TrendScore'][i-1] > df['TSA'][i-1]) and (df['종가'][i-1] > df['SMA'][i-1]):
                    df.loc[i, 'MP'] = 1
                    df.loc[i, 'EntryLv'] = df['시가'][i]
                    df.loc[i, 'dailyPL'] = (df['종가'][i] - df['EntryLv'][i]) * df['MP'][i]
                    if df['MP'][i-1] == -1:
                        df.loc[i, 'dailyPL'] += (df['EntryLv'][i] - df['종가'][i-1]) * df['MP'][i-1]
                        
            if df['MP'][i] != -1:
                if (df['TrendScore'][i-1] < df['TSA'][i-1]) and (df['종가'][i-1] < df['SMA'][i-1]):
                    df.loc[i, 'MP'] = -1
                    df.loc[i, 'EntryLv'] = df['시가'][i]
                    df.loc[i, 'dailyPL'] = (df['종가'][i] - df['EntryLv'][i]) * df['MP'][i]
                    if df['MP'][i-1] == 1:
                        df.loc[i, 'dailyPL'] += (df['EntryLv'][i] - df['종가'][i-1]) * df['MP'][i-1]


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
                        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_entry, 0)   # 동호 매수 청산
                        df.loc[len(df)-1, 'MP'] = 1
                        self.logger.info('Buy %s amount ordered', self.amt_entry)
                        self.chkPos(self.amt_entry)
                    if df.iloc[-1]['MP'] == -1:
                        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_entry, 0)
                        df.loc[len(df)-1, 'MP'] = -1
                        self.logger.info('Sell %s amount ordered', self.amt_entry)
                        self.chkPos(-self.amt_entry)