# - Timeframe: 일봉
# - Entry
#   Long(Short): 고(저)가가 Length 기간중 고(저)가 상향(하향) 돌파(이탈) 시, 종가+(-)기간ATR*Constant 에서
# - Exit
#   EL(ES): Parabolic stop

from System.strategy import Strategy
from System.indicator import Indicator

import pandas as pd
import logging



class TS_RB_0036():
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
        self.nLen = 20
        self.fConstant = 0.66
        self.fBuyPrice = 0.0
        self.fSellPrice = 0.0
        
        self.fAcceleration = 0.02
        self.fFirstBarMult = 1.5
        self.fAF = 0.0
        self.fStopPrice = 0.0
        self.fHighValue = 0.0
        self.fHighValue_1 = 0.0
        self.fLowValue = 9999.9
        self.fLowValue_1 = 9999.9
        

    # 공통 프로세스
    def common(self):
        # Data load & apply
        self.lstData[self.ix] = Strategy.getHistData(self.lstProductCode[self.ix], self.lstTimeFrame[self.ix], self.nLen*2)
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
        
        df.insert(len(df.columns), 'ATR', Indicator.ATR(df['고가'], df['저가'], df['종가'], self.nLen))
        df.insert(len(df.columns), 'ATR_3', Indicator.ATR(df['고가'], df['저가'], df['종가'], 3))
        df['MP'] = 0
        df['EntryLv'] = 0.0
        df['ExitLv'] = 0.0
        dfSignal = pd.DataFrame(None, columns=df.columns)
        for i in df.index-1:
            if i < self.nLen+1:
                continue

            df.loc[i, 'MP'] = df['MP'][i-1]
            df.loc[i, 'EntryLv'] = df['EntryLv'][i-1]
            df.loc[i, 'ExitLv'] = df['ExitLv'][i-1]
            
            # Entry
            if self.fBuyPrice != 0.0:   # execution
                if (df['MP'][i-1] != 1) and (df['고가'][i] >= self.fBuyPrice):
                    df.loc[i, 'MP'] = 1
                    if df['시가'][i] > self.fBuyPrice:    # Gap up
                        df.loc[i, 'EntryLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'EntryLv'] = self.fBuyPrice
                    self.fBuyPrice = 0.0
            if self.fSellPrice != 0.0:
                if (df['MP'][i-1] != -1) and (df['저가'][i] <= self.fSellPrice):
                    df.loc[i, 'MP'] = -1
                    if df['시가'][i] < self.fSellPrice:    # Gap dn
                        df.loc[i, 'EntryLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'EntryLv'] = self.fSellPrice
                    self.fSellPrice = 0.0
        
            if df['고가'][i] > max(df['고가'][i-self.nLen:i]):
                self.fBuyPrice = df['종가'][i] + self.fConstant * df['ATR'][i]
            else:
                self.fBuyPrice = 0.0
            if df['저가'][i] < min(df['저가'][i-self.nLen:i]):
                self.fSellPrice = df['종가'][i] - self.fConstant * df['ATR'][i]
            else:
                self.fSellPrice = 0.0

            # Exit
            if self.fStopPrice != 0.0:  # Parabolic stop (execution)
                if df['MP'][i-1] == 1 and df['저가'][i] <= self.fStopPrice:
                    df.loc[i, 'ExitLv'] = self.fStopPrice
                    df.loc[i, 'MP'] = 0
                    self.fStopPrice = 0.0
                if df['MP'][i-1] == -1 and df['고가'][i] >= self.fStopPrice:
                    df.loc[i, 'ExitLv'] = self.fStopPrice
                    df.loc[i, 'MP'] = 0
                    self.fStopPrice = 0.0
                    
            if df['고가'][i] > self.fHighValue:   # Parabolic stop (setup)
                self.fHighValue_1 = self.fHighValue
                self.fHighValue = df['고가'][i]
            if df['저가'][i] < self.fLowValue:
                self.fLowValue_1 = self.fLowValue
                self.fLowValue = df['저가'][i]
            if df['MP'][i] == 1:
                if df['MP'][i-1] != 1:
                    self.fStopPrice = df['저가'][i] - df['ATR_3'][i] * self.fFirstBarMult
                    self.fAF = self.fAcceleration
                    self.fHighValue = df['고가'][i]
                else:
                    self.fStopPrice += self.fAF * (self.fHighValue - self.fStopPrice)
                    if self.fHighValue > self.fHighValue_1 and self.fAF < 0.2:
                        self.fAF += min(self.fAcceleration, 0.2-self.fAF)
                if self.fStopPrice > df['저가'][i]:
                    self.fStopPrice = df['저가'][i]    
            if df['MP'][i] == -1:
                if df['MP'][i-1] != -1:
                    self.fStopPrice = df['고가'][i] + df['ATR_3'][i] * self.fFirstBarMult
                    self.fAF = self.fAcceleration
                    self.fLowValue = df['저가'][i]
                else:
                    self.fStopPrice -= self.fAF * (self.fStopPrice - self.fLowValue)
                    if self.fLowValue < self.fLowValue_1 and self.fAF < 0.2:
                        self.fAF += min(self.fAcceleration, 0.2-self.fAF)
                if self.fStopPrice < df['고가'][i]:
                    self.fStopPrice = df['고가'][i]

            if df['MP'][i] != df['MP'][i-1]:
                dfSignal.loc[len(dfSignal),] = df.iloc[i]


    # 전략 실행
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행시
            self.common()
            self.chkPos()
            self.lstData[self.ix].loc[len(self.lstData[self.ix])-1, 'MP'] = self.lstData[self.ix].iloc[-2]['MP']
            return

        df = self.lstData[self.ix]
        if (self.npPriceInfo == None) or (self.npPriceInfo['시가'] == 0):    # 첫 데이터 수신시
            self.npPriceInfo = PriceInfo.copy()
            if PriceInfo['현재가'] == PriceInfo['시가']:    # 시초가인 경우
                self.npPriceInfo['체결시간'] = df.iloc[-2]['시간']  # 전봉 정보 세팅
                self.npPriceInfo['시가'] = df.iloc[-2]['시가']
                self.npPriceInfo['고가'] = df.iloc[-2]['고가']
                self.npPriceInfo['저가'] = df.iloc[-2]['저가']
                self.npPriceInfo['현재가'] = df.iloc[-2]['종가']

        # Entry
        if self.fBuyPrice != 0.0:
            if (df.iloc[-1]['MP'] != 1) and (df.iloc[-2]['MP'] != 1):   # Buy
                if (self.npPriceInfo['현재가'] <= self.fBuyPrice) and (PriceInfo['현재가'] >= self.fBuyPrice):
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] = 1
                    self.fBuyPrice = 0.0
                    self.logger.info('Buy %s amount ordered', self.amt_entry)
                    self.chkPos(self.amt_entry)
        if self.fSellPrice != 0.0:
            if (df.iloc[-1]['MP'] != -1) and (df.iloc[-2]['MP'] != -1): # Sell
                if (self.npPriceInfo['현재가'] >= self.fSellPrice) and (PriceInfo['현재가'] <= self.fSellPrice):
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] = -1
                    self.fSellPrice = 0.0
                    self.logger.info('Sell %s amount ordered', self.amt_entry)
                    self.chkPos(-self.amt_entry)

        # Exit
        if self.fStopPrice != 0.0:  # Parabolic stop (execution)
            if (df.iloc[-1]['MP'] == 1) and (df.iloc[-2]['MP'] == 1):
                if (self.npPriceInfo['현재가'] >= self.fStopPrice) and (PriceInfo['현재가'] <= self.fStopPrice):
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'EL', self.amt_exit, PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] = 0
                    self.fStopPrice = 0.0
                    self.logger.info('ExitLong %s amount ordered', self.amt_exit)
                    self.chkPos(-self.amt_exit)
            if(df.iloc[-1]['MP'] == -1) and (df.iloc[-2]['MP'] == -1):
                if (self.npPriceInfo['현재가'] <= self.fStopPrice) and (PriceInfo['현재가'] >= self.fStopPrice):
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'ES', self.amt_exit, PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] = 0
                    self.fStopPrice = 0.0
                    self.logger.info('ExitShort %s amount ordered', self.amt_exit)
                    self.chkPos(self.amt_exit)

        self.npPriceInfo = PriceInfo.copy()