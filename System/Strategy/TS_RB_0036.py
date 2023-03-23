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
        self.nLen = 20
        self.fConstant = 0.66
        self.bBuySetup = False
        self.bSellSetup = False
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
            
            # Setup
            if df['고가'][i-1] > max(df['고가'][i-self.nLen-1:i-1]):
                self.bBuySetup = True
            else:
                self.bBuySetup = False
            if df['저가'][i-1] < min(df['저가'][i-self.nLen-1:i-1]):
                self.bSellSetup = True
            else:
                self.bSellSetup = False

            # Entry
            if df['MP'][i] != 1:
                if self.bBuySetup and df['고가'][i] >= df['종가'][i-1] + self.fConstant * df['ATR'][i-1]:
                    df.loc[i, 'MP'] = 1
                    if df['시가'][i] > df['종가'][i-1] + self.fConstant * df['ATR'][i-1]:    # Gap up
                        df.loc[i, 'EntryLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'EntryLv'] = df['종가'][i-1] + self.fConstant * df['ATR'][i-1]
                    self.bBuySetup = False
            if df['MP'][i] != -1:
                if self.bSellSetup and df['저가'][i] <= df['종가'][i-1] - self.fConstant * df['ATR'][i-1]:
                    df.loc[i, 'MP'] = -1
                    if df['시가'][i] < df['종가'][i-1] - self.fConstant * df['ATR'][i-1]:    # Gap dn
                        df.loc[i, 'EntryLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'EntryLv'] = df['종가'][i-1] - self.fConstant * df['ATR'][i-1]
                    self.bSellSetup = False
        
            # Exit
            if self.fStopPrice != 0.0:  # Parabolic stop (execution)
                if df['MP'][i] == 1 and df['저가'][i] < self.fStopPrice:
                    df.loc[i, 'ExitLv'] = self.fStopPrice
                    df.loc[i, 'MP'] = 0
                    self.fStopPrice = 0.0
                if df['MP'][i] == -1 and df['고가'][i] > self.fStopPrice:
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

        df = df.sort_index(ascending=False).reset_index()
        self.lstData[self.ix]['MP'] = df['MP']


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
                if df['고가'][1] > max(df['고가'][2:self.nLen+2]):
                    self.bBuySetup = True
                    self.fBuyPrice = df['종가'][1] + self.fConstant * df['ATR'][1]
                else:
                    self.bBuySetup = False
                if df['저가'][1] < min(df['저가'][2:self.nLen+2]):
                    self.bSellSetup = True
                    self.fSellPrice = df['종가'][1] - self.fConstant * df['ATR'][1]
                else:
                    self.bSellSetup = False
                
        else:
            df = self.lstData[self.ix]
            if self.npPriceInfo == None:    # 첫 데이터 수신시
                self.npPriceInfo = PriceInfo.copy()
                if PriceInfo['현재가'] == PriceInfo['시가']:    # 장 중간 시스템 작동시 주문 방지 
                    self.npPriceInfo['현재가'] = df['종가'][1]

            # Entry
            if df['MP'][0] != 1 and self.bBuySetup:    # Buy
                if (self.npPriceInfo['현재가'] < self.fBuyPrice) and (PriceInfo['현재가'] >= self.fBuyPrice):
                    Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])
                    self.logger.info('Buy %s amount ordered', self.amt_entry)
                    df.loc[0, 'MP'] = 1
                    self.bBuySetup = False
            if df['MP'][0] != -1 and self.bSellSetup:    # Sell
                if (self.npPriceInfo['현재가'] > self.fSellPrice) and (PriceInfo['현재가'] <= self.fSellPrice):
                    Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])
                    self.logger.info('Sell %s amount ordered', self.amt_entry)
                    df.loc[0, 'MP'] = -1
                    self.bSellSetup = False

            # Exit
            if self.fStopPrice != 0.0:  # Parabolic stop (execution)
                if df['MP'][0] == 1:
                    if (self.npPriceInfo['현재가'] > self.fStopPrice) and (PriceInfo['현재가'] <= self.fStopPrice):
                        df.loc[0, 'MP'] = 0
                        self.fStopPrice = 0.0
                if df['MP'][0] == -1:
                    if (self.npPriceInfo['현재가'] < self.fStopPrice) and (PriceInfo['현재가'] >= self.fStopPrice):
                        df.loc[0, 'MP'] = 0
                        self.fStopPrice = 0.0

            self.npPriceInfo = PriceInfo.copy()