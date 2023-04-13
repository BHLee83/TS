# - Timeframe: 일봉
# - Entry
#   
# - Exit
#   

from System.strategy import Strategy
from System.indicator import Indicator

import pandas as pd
from datetime import datetime as dt
import logging



class TS_RB_0038():
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
        self.nLen = 5
        self.fMult = 0.2
        self.bBuySetup = False
        self.bSellSetup = False
        self.fLEntryValue = 0.0
        self.fSEntryValue = 0.0
        self.fBuyPrice = 0.0
        self.fSellPrice = 0.0
        self.nLoc = 0
        
        self.nProtectiveATRs = 3
        self.nBrkevenATRs = 4
        self.fPosHigh = 0.0
        self.fPosLow = 9999.9
        self.fEL_BES = 0.0
        self.fES_BES = 0.0
        self.fEL_PS = 0.0
        self.fES_PS = 0.0
        

    # 공통 프로세스
    def common(self):
        # Data load & apply
        self.lstData[self.ix] = Strategy.getHistData(self.lstProductCode[self.ix], self.lstTimeFrame[self.ix], self.nLen*16*2)
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
        
        df.insert(len(df.columns), 'MA1', Indicator.MA(df['종가'], self.nLen*1))
        df.insert(len(df.columns), 'MA2', Indicator.MA(df['종가'], self.nLen*2))
        df.insert(len(df.columns), 'MA3', Indicator.MA(df['종가'], self.nLen*4))
        df.insert(len(df.columns), 'MA4', Indicator.MA(df['종가'], self.nLen*8))
        df.insert(len(df.columns), 'MA5', Indicator.MA(df['종가'], self.nLen*16))
        df.insert(len(df.columns), 'ATR', Indicator.ATR(df['고가'], df['저가'], df['종가'], self.nLen))
        df['Range'] = df['고가'] - df['저가']
        df['MP'] = 0
        df['EntryLv'] = 0.0
        df['ExitLv'] = 0.0
        df['LongCounter'] = 0
        df['ShortCounter'] = 0
        dfSignal = pd.DataFrame(None, columns=df.columns)
        for i in df.index-1:
            if i < self.nLen*16:
                continue

            df.loc[i, 'MP'] = df['MP'][i-1]
            df.loc[i, 'EntryLv'] = df['EntryLv'][i-1]
            df.loc[i, 'ExitLv'] = df['ExitLv'][i-1]
            
            # Setup
            if df['MP'][i] != df['MP'][i-1]:
                self.bBuySetup = False
                self.bSellSetup = False
            
            for j in range(1, 6):
                if df['MA'+str(j)][i] > df['MA'+str(j)][i-1]:
                    df.loc[i, 'LongCounter'] += 1
                if df['MA'+str(j)][i] < df['MA'+str(j)][i-1]:
                    df.loc[i, 'ShortCounter'] += 1

            if df['LongCounter'][i] == 5:
                self.bBuySetup = True
                self.fLEntryValue = df['종가'][i] + df['Range'][i] * self.fMult
            if df['ShortCounter'][i] == 5:
                self.bSellSetup = True
                self.fSEntryValue = df['종가'][i] - df['Range'][i] * self.fMult

            # Entry
            if self.fBuyPrice != 0.0:   # execution
                if df['MP'][i-1] != 1 and df['고가'][i] >= self.fBuyPrice:
                    df.loc[i, 'MP'] = 1
                    self.nLoc = i
                    if df['시가'][i] > self.fBuyPrice:    # Gap up
                        df.loc[i, 'EntryLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'EntryLv'] = self.fBuyPrice
                    self.fBuyPrice = 0.0
            if self.fSellPrice != 0.0:
                if df['MP'][i-1] != -1 and df['저가'][i] <= self.fSellPrice:
                    df.loc[i, 'MP'] = -1
                    self.nLoc = i
                    if df['시가'][i] < self.fSellPrice:    # Gap dn
                        df.loc[i, 'EntryLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'EntryLv'] = self.fSellPrice
                    self.fSellPrice = 0.0

            if df['MP'][i] != 1 and self.bBuySetup: # setup
                self.fBuyPrice = self.fLEntryValue
            if df['MP'][i] != -1 and self.bSellSetup:
                self.fSellPrice = self.fSEntryValue

            # Exit
            if df['MP'][i-1] == 1 and df['LongCounter'][i-1] <= 2:  # Method 1
                df.loc[i, 'MP'] = 0
                df.loc[i, 'ExitLv'] = df['시가'][i]
            if df['MP'][i-1] == -1 and df['ShortCounter'][i-1] <= 2:
                df.loc[i, 'MP'] = 0
                df.loc[i, 'ExitLv'] = df['시가'][i]

            if df['MP'][i-1] != 0:  # Method 2
                if df['MP'][i-1] == 1:
                    if self.fEL_BES != 0.0 and df['저가'][i] <= self.fEL_BES:   # Breakeven Stop(execution)
                        if df['시가'][i] < self.fEL_BES:
                            df.loc[i, 'ExitLv'] = df['시가'][i]
                        else:
                            df.loc[i, 'ExitLv'] = self.fEL_BES
                        df.loc[i, 'MP'] = 0
                        self.fEL_BES = 0.0
                    if self.fEL_PS != 0.0 and df['저가'][i] <= self.fEL_PS:    # Protective Stop (execution)
                        if df['시가'][i] < self.fEL_PS:
                            df.loc[i, 'ExitLv'] = df['시가'][i]
                        else:
                            df.loc[i, 'ExitLv'] = self.fEL_PS
                        df.loc[i, 'MP'] = 0
                        self.fEL_PS = 0.0
                if df['MP'][i-1] == -1:
                    if self.fES_BES != 0.0 and df['고가'][i] >= self.fES_BES:
                        if df['시가'][i] > self.fES_BES:
                            df.loc[i, 'ExitLv'] = df['시가'][i]
                        else:
                            df.loc[i, 'ExitLv'] = self.fES_BES
                        df.loc[i, 'MP'] = 0
                        self.fES_BES = 0.0
                    if self.fES_PS != 0.0 and df['고가'][i] >= self.fES_PS:
                        if df['시가'][i] > self.fES_PS:
                            df.loc[i, 'ExitLv'] = df['시가'][i]
                        else:
                            df.loc[i, 'ExitLv'] = self.fES_PS
                        df.loc[i, 'MP'] = 0
                        self.fES_PS = 0.0

                self.fPosHigh = max(df['고가'][self.nLoc-1:i])
                self.fPosLow = min(df['저가'][self.nLoc-1:i])
                if self.fPosHigh > df['EntryLv'][i] + df['ATR'][i] * self.nBrkevenATRs:    # Breakeven Stop (setup)
                    self.fEL_BES = df['EntryLv'][i]
                if self.fPosLow < df['EntryLv'][i] - df['ATR'][i] * self.nBrkevenATRs:
                    self.fES_BES = df['EntryLv'][i]
                    
                if df['MP'][i-1] == 1:    # Protective Stop (setup)
                    self.fEL_PS = df['EntryLv'][i] - df['ATR'][i] * self.nProtectiveATRs
                if df['MP'][i-1] == -1:
                    self.fES_PS = df['EntryLv'][i] + df['ATR'][i] * self.nProtectiveATRs

            if df['MP'][i] != df['MP'][i-1]:
                dfSignal.loc[len(dfSignal),] = df.iloc[i]


    # 전략 실행
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행시
            self.common()
            self.chkPos()
            self.lstData[self.ix].loc[len(self.lstData[self.ix])-1, 'MP'] = self.lstData[self.ix].iloc[-2]['MP']

            df = self.lstData[self.ix]
            tNow = dt.now().time()
            if tNow.hour < 9:   # 9시 전이면
                # Exit
                if df.iloc[-1]['MP'] == 1 and df.iloc[-2]['LongCounter'] <= 2:  # Method 1
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'EL', self.amt_exit, 0)   # 동호 매수 청산
                    df.loc[len(df)-1, 'MP'] = 0
                    self.logger.info('ExitLong %s amount ordered', self.amt_exit)
                    self.chkPos(-self.amt_exit)
                if df.iloc[-1]['MP'] == -1 and df.iloc[-2]['ShortCounter'] <= 2:
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'ES', self.amt_exit, 0)   # 동호 매도 청산
                    df.loc[len(df)-1, 'MP'] = 0
                    self.logger.info('ExitShort %s amount ordered', self.amt_exit)
                    self.chkPos(self.amt_exit)
            return
                        
        df = self.lstData[self.ix]
        if self.npPriceInfo == None:    # 첫 데이터 수신시
            self.npPriceInfo = PriceInfo.copy()
            if PriceInfo['현재가'] == PriceInfo['시가']:    # 시초가인 경우
                self.npPriceInfo['체결시간'] = df.iloc[-2]['시간']  # 전봉 정보 세팅
                self.npPriceInfo['시가'] = df.iloc[-2]['시가']
                self.npPriceInfo['고가'] = df.iloc[-2]['고가']
                self.npPriceInfo['저가'] = df.iloc[-2]['저가']
                self.npPriceInfo['현재가'] = df.iloc[-2]['종가']

        # Entry
        if self.fBuyPrice != 0.0:
            if (df.iloc[-1]['MP'] != 1) and (df.iloc[-2]['MP'] != 1):
                if (self.npPriceInfo['현재가'] <= self.fBuyPrice) and (PriceInfo['현재가'] >= self.fBuyPrice):
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] = 1
                    self.fBuyPrice = 0.0
                    self.logger.info('Buy %s amount ordered', self.amt_entry)
                    self.chkPos(self.amt_entry)
        if self.fSellPrice != 0.0:
            if (df.iloc[-1]['MP'] != -1) and (df.iloc[-2]['MP'] != -1):
                if (self.npPriceInfo['현재가'] >= self.fSellPrice) and (PriceInfo['현재가'] <= self.fSellPrice):
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] = -1
                    self.fSellPrice != 0.0
                    self.logger.info('Sell %s amount ordered', self.amt_entry)
                    self.chkPos(-self.amt_entry)

        # Exit
        if df.iloc[-2]['MP'] != 0:
            if df.iloc[-1]['MP'] == 1:    # EL
                if self.fEL_BES != 0.0: # Breakeven Stop
                    if (self.npPriceInfo['현재가'] >= self.fEL_BES) and (PriceInfo['현재가'] <= self.fEL_BES):
                        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'EL', self.amt_exit, PriceInfo['현재가'])
                        df.loc[len(df)-1, 'MP'] = 0
                        self.fEL_BES = 0.0
                        self.logger.info('EL_BES %s amount ordered', self.amt_exit)
                        self.chkPos(-self.amt_exit)
                if self.fEL_PS != 0.0:  # Protective Stop
                    if (self.npPriceInfo['현재가'] >= self.fEL_PS) and (PriceInfo['현재가'] <= self.fEL_PS):
                        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'EL', self.amt_exit, PriceInfo['현재가'])
                        df.loc[len(df)-1, 'MP'] = 0
                        self.fEL_PS = 0.0
                        self.logger.info('EL_PS %s amount ordered', self.amt_exit)
                        self.chkPos(-self.amt_exit)
            if df.iloc[-1]['MP'] == -1:   # ES
                if self.fES_BES != 0.0: # Breakeven Stop
                    if (self.npPriceInfo['현재가'] <= self.fES_BES) and (PriceInfo['현재가'] >= self.fES_BES):
                        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'ES', self.amt_exit, PriceInfo['현재가'])
                        df.loc[len(df)-1, 'MP'] = 0
                        self.fES_BES = 0.0
                        self.logger.info('ES_BES %s amount ordered', self.amt_exit)
                        self.chkPos(self.amt_exit)
                if self.fES_PS != 0.0:  # Protective Stop
                    if (self.npPriceInfo['현재가'] <= self.fES_PS) and (PriceInfo['현재가'] >= self.fES_PS):
                        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'ES', self.amt_exit, PriceInfo['현재가'])
                        df.loc[len(df)-1, 'MP'] = 0
                        self.fES_PS = 0.0
                        self.logger.info('ES_PS %s amount ordered', self.amt_exit)
                        self.chkPos(self.amt_exit)

        self.npPriceInfo = PriceInfo.copy()