# 타임프레임: 5분봉
# - Setup
#   
# - Entry
#   금일 시가에서 전일 레인지의 일정 비율로 계산한 밴드 상단(하단) 돌파시 매수(매도)
# - Exit
#   진입가에서 1% 하락(상승)시 / 진입 이후 고점(저점) 기준 1% 하락(상승)시
# 수수료: 0.0006%
# 슬리피지: 0.05pt

from System.strategy import Strategy
from System.indicator import Indicator

import pandas as pd
import datetime as dt
import logging



class TS_RB_0043_3():
    def __init__(self, info) -> None:
        super().__init__()
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
        self.fR1 = 0.3
        self.fR2 = 0.01
        self.fDayOpen = 0.0
        self.fDayRange = 0.0
        self.fBuyPrice = 0.0
        self.fSellPrice = 0.0
        self.fEL = 0.0
        self.fES = 0.0
        self.nEntryIdx = 0
        self.strExitName = ''
        self.strExitDate = ''
        self.fExitPrice = 0.0
        self.bCond1 = False
        self.bCond2 = False
        self.dtEntryLimit = dt.time(15,30)
        

    # 공통 프로세스
    def common(self):
        # Data load & apply
        self.lstData[self.ix] = Strategy.getHistData(self.lstProductCode[self.ix], self.lstTimeFrame[self.ix], int(400/int(self.lstTimeIntrvl[self.ix]))*10)
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

        df['MP'] = 0
        df['EntryLv'] = 0.0
        df['ExitLv'] = 0.0

        dfSignal = pd.DataFrame(None, columns=df.columns)
        for i in df.index-1:
            if i < 1:
                continue

            df.loc[i, 'MP'] = df['MP'][i-1]
            df.loc[i, 'EntryLv'] = df['EntryLv'][i-1]
            df.loc[i, 'ExitLv'] = df['ExitLv'][i-1]
            
            # Setup
            self.bCond1 = False
            self.bCond2 = False
            if df['일자'][i] != df['일자'][i-1]:
                self.fDayOpen = df['시가'][i]
                dayHigh = df[(df['일자'] >= df['일자'][i-1]) & (df['일자'] < df['일자'][i])]['고가'].max()
                dayLow = df[(df['일자'] >= df['일자'][i-1]) & (df['일자'] < df['일자'][i])]['저가'].min()
                self.fDayRange = dayHigh - dayLow
                
            if self.fDayOpen == 0:
                continue
                
            if (self.strExitName == 'EL') and (self.strExitDate == df['일자'][i]) and (self.fExitPrice > (self.fDayOpen + self.fDayRange * self.fR1)):
                self.bCond1 = True
            if (self.strExitName == 'ES') and (self.strExitDate == df['일자'][i]) and (self.fExitPrice < (self.fDayOpen - self.fDayRange * self.fR1)):
                self.bCond2 = True

            if df['일자'][i] == df['일자'][i+1]:
                # Exit
                if df['MP'][i] > 0:    # execution
                    if self.fEL != 0.0:
                        if df['저가'][i] <= self.fEL:
                            df.loc[i, 'ExitLv'] = min(self.fEL, df['시가'][i])
                            df.loc[i, 'MP'] = 0
                            self.strExitName = 'EL'
                            self.strExitDate = df['일자'][i]
                            self.fExitPrice = df['ExitLv'][i]
                            self.fEL = 0.0
                    else:   # setup
                        self.fEL = df['고가'][self.nEntryIdx-1:i].max() * (1 - self.fR2)
                if df['MP'][i] < 0:
                    if self.fES != 0.0:
                        if df['고가'][i] >= self.fES:
                            df.loc[i, 'ExitLv'] = max(self.fES, df['시가'][i])
                            df.loc[i, 'MP'] = 0
                            self.strExitName = 'ES'
                            self.strExitDate = df['일자'][i]
                            self.fExitPrice = df['ExitLv'][i]
                            self.fES = 0.0
                    else:   # setup
                        self.fES = df['저가'][self.nEntryIdx-1:i].min() * (1 + self.fR2)
                        
                # Entry
                if int(df['시간'][i]) < 150000:
                    if self.fBuyPrice != 0.0 and df['고가'][i] >= self.fBuyPrice:    # execution
                        df.loc[i, 'MP'] = 1
                        df.loc[i, 'EntryLv'] = max(self.fBuyPrice, df['시가'][i])
                        self.fBuyPrice = 0.0
                        self.nEntryIdx = i
                    if self.fSellPrice != 0.0 and df['저가'][i] <= self.fSellPrice:
                        df.loc[i, 'MP'] = -1
                        df.loc[i, 'EntryLv'] = min(self.fSellPrice, df['시가'][i])
                        self.fSellPrice = 0.0
                        self.nEntryIdx = i
                    if (self.bCond1 == False) and (df['MP'][i] <= 0): # setup
                        self.fBuyPrice = self.fDayOpen + self.fDayRange * self.fR1
                    if (self.bCond2 == False) and (df['MP'][i] >= 0):
                        self.fSellPrice = self.fDayOpen - self.fDayRange * self.fR1

            # Position check
            if df['MP'][i] != df['MP'][i-1]:
                dfSignal.loc[len(dfSignal)] = df.loc[i, :]

        print(dfSignal)


    # 전략 실행
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행시
            self.common()
            self.chkPos()
            self.lstData[self.ix].loc[len(self.lstData[self.ix])-1, 'MP'] = self.lstData[self.ix].iloc[-2]['MP']
            return
        
        if (self.npPriceInfo == None) or (self.npPriceInfo['시가'] == 0):    # 첫 데이터 수신시
            self.npPriceInfo = PriceInfo.copy()

        # 분봉 업데이트 시
        if (str(self.npPriceInfo['체결시간'])[4:6] != str(PriceInfo['체결시간'])[4:6]) and \
        (int(str(PriceInfo['체결시간'])[4:6]) % int(self.lstTimeIntrvl[self.ix]) == 0):
            self.common()
            self.lstData[self.ix].loc[len(self.lstData[self.ix])-1, 'MP'] = self.lstData[self.ix].iloc[-2]['MP']
            
        df = self.lstData[self.ix]

        # Exit
        if self.fEL != 0.0:
            if (self.nPosition > 0) and (df.iloc[-1]['MP'] > 0):
                if self.npPriceInfo['현재가'] >= self.fEL and PriceInfo['현재가'] <= self.fEL:
                    # Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'EL', self.amt_exit, PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] = 0
                    self.fEL = 0.0
                    self.logger.info('ExitLong %s amount ordered', self.amt_exit)
                    self.chkPos(-self.amt_exit)
        if self.fES != 0.0:
            if (self.nPosition < 0) and (df.iloc[-1]['MP'] < 0):
                if self.npPriceInfo['현재가'] <= self.fES and PriceInfo['현재가'] >= self.fES:
                    # Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'ES', self.amt_exit, PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] = 0
                    self.fES = 0.0
                    self.logger.info('ExitShort %s amount ordered', self.amt_exit)
                    self.chkPos(self.amt_exit)

        # Entry
        if dt.datetime.now().time() < self.dtEntryLimit:
            if self.fBuyPrice != 0.0:
                if (self.nPosition <= 0) and (df.iloc[-1]['MP'] <= 0):
                    if (self.npPriceInfo['현재가'] <= self.fBuyPrice) and (PriceInfo['현재가'] >= self.fBuyPrice):
                        # Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])
                        df.loc[len(df)-1, 'MP'] = 1
                        self.fBuyPrice = 0.0
                        self.logger.info('Buy %s amount ordered', self.amt_entry)
                        self.chkPos(self.amt_entry)
            if self.fSellPrice != 0.0:
                if (self.nPosition >= 0) and (df.iloc[-1]['MP'] >= 0):
                    if (self.npPriceInfo['현재가'] >= self.fSellPrice) and (PriceInfo['현재가'] <= self.fSellPrice):
                        # Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])
                        df.loc[len(df)-1, 'MP'] = -1
                        self.fSellPrice = 0.0
                        self.logger.info('Sell %s amount ordered', self.amt_entry)
                        self.chkPos(-self.amt_entry)

        self.npPriceInfo = PriceInfo.copy()