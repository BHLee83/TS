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
        self.nLen = 5
        self.fMult = 0.2
        self.bBuySetup = False
        self.bSellSetup = False
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
        
        return data


    # 전략 적용
    def applyChart(self):   # Strategy apply on historical chart
        df = self.lstData[self.ix].sort_index(ascending=False).reset_index()
        
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
            for j in range(1, 6):
                if df['MA'+str(j)][i] > df['MA'+str(j)][i-1]:
                    df.loc[i, 'LongCounter'] += 1
                if df['MA'+str(j)][i] < df['MA'+str(j)][i-1]:
                    df.loc[i, 'ShortCounter'] += 1
            if df['LongCounter'][i-1] == 5:
                self.bBuySetup = True
                self.fBuyPrice = df['종가'][i-1] + df['Range'][i-1] * self.fMult
            else:
                self.bBuySetup = False
            if df['ShortCounter'][i-1] == 5:
                self.bSellSetup = True
                self.fSellPrice = df['종가'][i-1] - df['Range'][i-1] * self.fMult
            else:
                self.bSellSetup = False

            # Entry
            if df['MP'][i] != 1 and self.bBuySetup:
                if df['고가'][i] >= self.fBuyPrice:
                    df.loc[i, 'MP'] = 1
                    self.nLoc = i
                    if df['시가'][i] > self.fBuyPrice:    # Gap up
                        df.loc[i, 'EntryLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'EntryLv'] = self.fBuyPrice
            if df['MP'][i] != -1 and self.bSellSetup:
                if df['저가'][i] <= self.fSellPrice:
                    df.loc[i, 'MP'] = -1
                    self.nLoc = i
                    if df['시가'][i] < self.fSellPrice:    # Gap dn
                        df.loc[i, 'EntryLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'EntryLv'] = self.fSellPrice
        
            # Exit
            if df['MP'][i-1] == 1 and df['LongCounter'][i-1] <= 2:  # Method 1
                df.loc[i, 'MP'] = 0
                df.loc[i, 'ExitLv'] = df['시가'][i]
            if df['MP'][i-1] == -1 and df['ShortCounter'][i-1] <= 2:
                df.loc[i, 'MP'] = 0
                df.loc[i, 'ExitLv'] = df['시가'][i]

            if df['MP'][i] != 0:    # Method 2
                if df['MP'][i] == 1:
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
                if df['MP'][i] == -1:
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

                self.fPosHigh = max(df['고가'][self.nLoc:i+1])
                self.fPosLow = min(df['저가'][self.nLoc:i+1])
                if self.fPosHigh > self.fBuyPrice + df['ATR'][i] * self.nBrkevenATRs:    # Breakeven Stop (setup)
                    self.fEL_BES = self.fBuyPrice
                if self.fPosLow < self.fSellPrice - df['ATR'][i] * self.nBrkevenATRs:
                    self.fES_BES = self.fSellPrice
                    
                if df['MP'][i] == 1:    # Protective Stop (setup)
                    self.fEL_PS = self.fBuyPrice - df['ATR'][i] * self.nProtectiveATRs
                if df['MP'][i] == -1:
                    self.fES_PS = self.fSellPrice + df['ATR'][i] * self.nProtectiveATRs

            if df['MP'][i] != df['MP'][i-1]:
                dfSignal.loc[len(dfSignal),] = df.iloc[i]

        df = df.sort_index(ascending=False).reset_index()
        self.lstData[self.ix]['MP'] = df['MP']
        self.lstData[self.ix]['Range'] = df['Range']
        self.lstData[self.ix]['ATR'] = df['ATR']
        self.lstData[self.ix]['LongCounter'] = df['LongCounter']
        self.lstData[self.ix]['ShortCounter'] = df['ShortCounter']


    # 전략 실행
    def execute(self, PriceInfo):
        self.nPosition = Strategy.getPosition(self.dfInfo['NAME'], self.lstAssetCode[self.ix], self.lstAssetType[self.ix])    # 포지션 확인 및 수량 지정
        self.amt_entry = abs(self.nPosition) + self.lstTrUnit[self.ix] * self.fWeight
        self.amt_exit = abs(self.nPosition)
        if type(PriceInfo) == int:  # 최초 실행
            self.lstData[self.ix] = self.getHistData()  # 과거 데이터 수신
            if self.lstData[self.ix].empty:
                self.logger.warning('과거 데이터 로드 실패. 전략이 실행되지 않습니다.')
                return False
            else:
                self.applyChart()   # 전략 적용 (chart)
                df = self.lstData[self.ix]
                df.loc[0, 'MP'] = df['MP'][1]

                # Setup
                if df['LongCounter'][1] == 5:
                    self.bBuySetup = True
                    self.fBuyPrice = df['종가'][1] + df['Range'][1] * self.fMult
                else:
                    self.bBuySetup = False
                if df['ShortCounter'][1] == 5:
                    self.bSellSetup = True
                    self.fSellPrice = df['종가'][1] - df['Range'][1] * self.fMult
                else:
                    self.bSellSetup = False

                # Exit
                tNow = dt.now().time()
                if tNow.hour < 9:   # 9시 전이면
                    if df['MP'][0] == 1 and df['LongCounter'][1] <= 2:  # Method 1
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'EL', self.amt_entry, 0)   # 동호 매수 청산
                        df.loc[0, 'MP'] = 0
                        self.logger.info('ExitLong %s amount ordered', self.amt_exit)
                    if df['MP'][0] == -1 and df['ShortCounter'][1] <= 2:
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'ES', self.amt_entry, 0)   # 동호 매도 청산
                        df.loc[0, 'MP'] = 0
                        self.logger.info('ExitShort %s amount ordered', self.amt_exit)
                        
        else:
            df = self.lstData[self.ix]
            if self.npPriceInfo == None:    # 첫 데이터 수신시
                self.npPriceInfo = PriceInfo.copy()
                if PriceInfo['현재가'] == PriceInfo['시가']:    # 장 중간 시스템 작동시 주문 방지 
                    self.npPriceInfo['현재가'] = df['종가'][1]

            # Entry
            if df['MP'][0] != 1 and self.bBuySetup:
                if (self.npPriceInfo['현재가'] < self.fBuyPrice) and (PriceInfo['현재가'] >= self.fBuyPrice):
                    Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])
                    self.logger.info('Buy %s amount ordered', self.amt_entry)
                    df.loc[0, 'MP'] = 1
            if df['MP'][0] != -1 and self.bSellSetup:
                if (self.npPriceInfo['현재가'] > self.fSellPrice) and (PriceInfo['현재가'] <= self.fSellPrice):
                    Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])
                    self.logger.info('Sell %s amount ordered', self.amt_entry)
                    df.loc[0, 'MP'] = -1

            # Exit
            if df['MP'][0] == 1:    # EL
                if self.fEL_BES != 0.0: # Breakeven Stop
                    if (self.npPriceInfo['현재가'] > self.fEL_BES) and (PriceInfo['현재가'] <= self.fEL_BES):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'EL', self.amt_exit, PriceInfo['현재가'])
                        self.logger.info('EL_BES %s amount ordered', self.amt_exit)
                        df.loc[0, 'MP'] = 0
                        self.fEL_BES = 0.0
                if self.fEL_PS != 0.0:  # Protective Stop
                    if (self.npPriceInfo['현재가'] > self.fEL_PS) and (PriceInfo['현재가'] <= self.fEL_PS):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'EL', self.amt_exit, PriceInfo['현재가'])
                        self.logger.info('EL_PS %s amount ordered', self.amt_exit)
                        df.loc[0, 'MP'] = 0
                        self.fEL_PS = 0.0
            if df['MP'][0] == -1:   # ES
                if self.fES_BES != 0.0: # Breakeven Stop
                    if (self.npPriceInfo['현재가'] < self.fES_BES) and (PriceInfo['현재가'] >= self.fES_BES):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'ES', self.amt_exit, PriceInfo['현재가'])
                        self.logger.info('ES_BES %s amount ordered', self.amt_exit)
                        df.loc[0, 'MP'] = 0
                        self.fES_BES = 0.0
                if self.fES_PS != 0.0:  # Protective Stop
                    if (self.npPriceInfo['현재가'] < self.fES_PS) and (PriceInfo['현재가'] >= self.fES_PS):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'ES', self.amt_exit, PriceInfo['현재가'])
                        self.logger.info('ES_PS %s amount ordered', self.amt_exit)
                        df.loc[0, 'MP'] = 0
                        self.fES_PS = 0.0

            self.npPriceInfo = PriceInfo.copy()