# 타임프레임: 5분봉
# - Setup: 
# 수수료: 0.006%
# 슬리피지: 0.05pt

from System.strategy import Strategy
from System.indicator import Indicator

import pandas as pd
import math
import logging



class TS_RB_0033():
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
        self.nFLen = 5
        self.nSLen = 30
        self.nADXLen = 12
        self.bLSetup = False
        self.bSSetup = False
        self.bBFlag = False
        self.bSFlag = False
        self.bADXSetup = False
        self.fBuyPrice1 = 0.0
        self.fBuyPrice2 = 0.0
        self.fSellPrice1 = 0.0
        self.fSellPrice2 = 0.0
        self.fEL = 0.0
        self.fES = 0.0
        

    # 공통 프로세스
    def common(self):
        # Data load & apply
        self.lstData[self.ix] = Strategy.getHistData(self.lstProductCode[self.ix], self.lstTimeFrame[self.ix], int(400/int(self.lstTimeIntrvl[self.ix]))+self.nSLen)
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

        FLen = math.ceil((self.nFLen+1)*0.5)
        SLen = math.ceil((self.nSLen+1)*0.5)
        tmp1 = Indicator.MA(df['종가'], FLen)
        tmp2 = Indicator.MA(df['종가'], SLen)
        df['TRIA1'] = Indicator.MA(tmp1, FLen)
        df['TRIA2'] = Indicator.MA(tmp2, SLen)
        df['ADXVal'] = Indicator.ADX(df['고가'], df['저가'], df['종가'], self.nADXLen)

        df['MP'] = 0
        df['EntryLv'] = 0.0
        df['ExitLv'] = 0.0
        dfSignal = pd.DataFrame(None, columns=df.columns)
        for i in df.index-1:
            if i < self.nSLen:
                continue

            df.loc[i, 'MP'] = df['MP'][i-1]
            df.loc[i, 'EntryLv'] = df['EntryLv'][i-1]
            df.loc[i, 'ExitLv'] = df['ExitLv'][i-1]
            
            # Setup
            if df['TRIA1'][i] > df['TRIA2'][i]:
                self.bLSetup = True
            else:
                self.bLSetup = False
            if df['TRIA1'][i] < df['TRIA2'][i]:
                self.bSSetup = True
            else:
                self.bSSetup = False
            if df['ADXVal'][i] > df['ADXVal'][i-self.nFLen]:
                self.bADXSetup = True
            else:
                self.bADXSetup = False
                
            if self.bLSetup and self.bADXSetup and df['고가'][i] < df['TRIA1'][i]:
                self.bBFlag = True
            else:
                self.bBFlag = False
            if self.bSSetup and self.bADXSetup and df['저가'][i] > df['TRIA1'][i]:
                self.bSFlag = True
            else:
                self.bSFlag = False
            
            if df['시간'][i].startswith('1545'):
                continue

            # Entry
            if df['MP'][i] <= 0:
                if self.fBuyPrice1 != 0.0 and df['고가'][i] >= self.fBuyPrice1:    # execution
                    if df['시가'][i] > self.fBuyPrice1:
                        df.loc[i, 'EntryLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'EntryLv'] = self.fBuyPrice1
                    if df['MP'][i-1] < 0:
                        df.loc[i, 'MP'] = 1
                    else:
                        df.loc[i, 'MP'] += 1
                    self.fBuyPrice1 = 0.0
            else:
                if self.fBuyPrice2 != 0.0 and df['고가'][i] >= self.fBuyPrice2:
                    if df['시가'][i] > self.fBuyPrice2:
                        df.loc[i, 'EntryLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'EntryLv'] = self.fBuyPrice2
                    if df['MP'][i-1] < 0:
                        df.loc[i, 'MP'] = 1
                    else:
                        df.loc[i, 'MP'] += 1
                    self.fBuyPrice2 = 0.0
            if df['MP'][i] >= 0:
                if self.fSellPrice1 != 0.0 and df['저가'][i] <= self.fSellPrice1:
                    if df['시가'][i] < self.fSellPrice1:
                        df.loc[i, 'EntryLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'EntryLv'] = self.fSellPrice1
                    if df['MP'][i-1] > 0:
                        df.loc[i, 'MP'] = -1
                    else:
                        df.loc[i, 'MP'] -= 1
                    self.fSellPrice1 = 0.0
            else:
                if self.fSellPrice2 != 0.0 and df['저가'][i] <= self.fSellPrice2:
                    if df['시가'][i] < self.fSellPrice2:
                        df.loc[i, 'EntryLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'EntryLv'] = self.fSellPrice2
                    if df['MP'][i-1] > 0:
                        df.loc[i, 'MP'] = -1
                    else:
                        df.loc[i, 'MP'] -= 1
                    self.fSellPrice2 = 0.0
            
            if (self.bLSetup and df['MP'][i] < 1) or (self.bBFlag and abs(df['MP'][i]) < 3):    # setup
                if df['MP'][i] < 1:
                    self.fBuyPrice1 = df['고가'][i] + 1
                else:
                    self.fBuyPrice2 = df['TRIA1'][i] + 1
            if (self.bSSetup and df['MP'][i] > -1) or (self.bSFlag and abs(df['MP'][i]) < 3):
                if df['MP'][i] > -1:
                    self.fSellPrice1 = df['저가'][i] - 1
                else:
                    self.fSellPrice2 = df['TRIA1'][i] - 1

            # Exit
            if df['MP'][i-1] > 0:    # execution
                if self.fEL != 0.0 and df['저가'][i] <= self.fEL:
                    if df['시가'][i] < self.fEL:
                        df.loc[i, 'ExitLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'ExitLv'] = self.fEL
                    df.loc[i, 'MP'] = 0
                    self.fEL = 0.0
            if df['MP'][i-1] < 0:
                if self.fES != 0.0 and df['고가'][i] >= self.fES:
                    if df['시가'][i] > self.fES:
                        df.loc[i, 'ExitLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'ExitLv'] = self.fES
                    df.loc[i, 'MP'] = 0
                    self.fES = 0.0
                
            if df['MP'][i] > 0:    # setup
                self.fEL = df['TRIA2'][i] - 1
            if df['MP'][i] < 0:
                self.fES = df['TRIA2'][i] + 1

            # Position check
            if df['MP'][i] != df['MP'][i-1]:
                dfSignal.loc[len(dfSignal)] = df.loc[i, :]


    # 전략 실행
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행시
            self.common()
            self.chkPos()
            self.lstData[self.ix].loc[len(self.lstData[self.ix])-1, 'MP'] = self.lstData[self.ix].iloc[-2]['MP']
            return
        
        if self.npPriceInfo == None:    # 첫 데이터 수신시
            self.npPriceInfo = PriceInfo.copy()

        # 분봉 업데이트 시
        if (str(self.npPriceInfo['체결시간'])[4:6] != str(PriceInfo['체결시간'])[4:6]) and \
        (int(str(PriceInfo['체결시간'])[4:6]) % int(self.lstTimeIntrvl[self.ix]) == 0):
            self.common()
            self.lstData[self.ix].loc[len(self.lstData[self.ix])-1, 'MP'] = self.lstData[self.ix].iloc[-2]['MP']
            
        df = self.lstData[self.ix]

        # Entry
        if self.fBuyPrice1 != 0.0:
            if (self.nPosition <= 0) and (df.iloc[-1]['MP'] <= 0):
                if (self.npPriceInfo['현재가'] <= self.fBuyPrice1) and (PriceInfo['현재가'] >= self.fBuyPrice1):
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] = 1
                    self.fBuyPrice1 = 0.0
                    self.logger.info('Buy %s amount ordered', self.amt_entry)
                    self.chkPos(self.amt_entry)
        if self.fBuyPrice2 != 0.0:
            if (self.nPosition > 0) and (df.iloc[-1]['MP'] > 0):
                if (self.npPriceInfo['현재가'] <= self.fBuyPrice2) and (PriceInfo['현재가'] >= self.fBuyPrice2):
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.lstTrUnit[self.ix], PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] += 1
                    self.fBuyPrice2 = 0.0
                    self.logger.info('Buy %s amount ordered', self.lstTrUnit[self.ix])
                    self.chkPos(self.lstTrUnit[self.ix])

        if self.fSellPrice1 != 0.0:
            if (self.nPosition >= 0) and (df.iloc[-1]['MP'] >= 0):
                if (self.npPriceInfo['현재가'] >= self.fSellPrice1) and (PriceInfo['현재가'] <= self.fSellPrice1):
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] = -1
                    self.fSellPrice1 = 0.0
                    self.logger.info('Sell %s amount ordered', self.amt_entry)
                    self.chkPos(-self.amt_entry)
        if self.fSellPrice2 != 0.0:
            if (self.nPosition < 0) and (df.iloc[-1]['MP'] < 0):
                if (self.npPriceInfo['현재가'] >= self.fSellPrice2) and (PriceInfo['현재가'] <= self.fSellPrice2):
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.lstTrUnit[self.ix], PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] -= 1
                    self.fSellPrice2 = 0.0
                    self.logger.info('Sell %s amount ordered', self.lstTrUnit[self.ix])
                    self.chkPos(-self.lstTrUnit[self.ix])

        # Exit
        if self.fEL != 0.0:
            if (self.nPosition > 0) and (df.iloc[-1]['MP'] > 0):
                if self.npPriceInfo['현재가'] >= self.fEL and PriceInfo['현재가'] <= self.fEL:
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'EL', self.amt_exit, PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] = 0
                    self.fEL = 0.0
                    self.logger.info('ExitLong %s amount ordered', self.amt_exit)
                    self.chkPos(-self.amt_exit)
        if self.fES != 0.0:
            if (self.nPosition < 0) and (df.iloc[-1]['MP'] < 0):
                if self.npPriceInfo['현재가'] <= self.fES and PriceInfo['현재가'] >= self.fES:
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'ES', self.amt_exit, PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] = 0
                    self.fES = 0.0
                    self.logger.info('ExitShort %s amount ordered', self.amt_exit)
                    self.chkPos(self.amt_exit)

        self.npPriceInfo = PriceInfo.copy()