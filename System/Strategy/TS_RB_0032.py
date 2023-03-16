# - Timeframe: 일봉
# - Entry
#   
# - Exit

from System.strategy import Strategy
from System.indicator import Indicator

import pandas as pd
from datetime import datetime as dt
import logging



class TS_RB_0032():
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
        self.nLen1 = 20
        self.nLen2 = 5
        self.nLen3 = self.nLen1 + self.nLen2
        self.nLoc = 0
        self.nBigProfitATRs = 7
        self.nProtectiveATRs = 3
        self.nTrailingATRs = 4
        self.fPosHigh = 0.0
        self.fPosLow = 9999.9
        self.fEL_BPS = 0.0
        self.fES_BPS = 0.0
        self.fEL_PS = 0.0
        self.fES_PS = 0.0
        self.fEL_TS = 0.0
        self.fES_TS = 0.0
        

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
        
        df.insert(len(df.columns), 'AvgVal', Indicator.MA(df['종가'], self.nLen1))
        df.insert(len(df.columns), 'ATR', Indicator.ATR(df['고가'], df['저가'], df['종가'], 30))
        df['MP'] = 0
        df['EntryLv'] = 0.0
        df['ExitLv'] = 0.0
        for i in df.index-1:
            if i < self.nLen3+1:
                continue

            df.loc[i, 'MP'] = df['MP'][i-1]
            df.loc[i, 'EntryLv'] = df['EntryLv'][i-1]
            df.loc[i, 'ExitLv'] = df['ExitLv'][i-1]
            
            # Entry
            if df['MP'][i] != 1:
                if (df['AvgVal'][i-1] > df['AvgVal'][i-self.nLen2-1]) and (df['종가'][i-1] < df['종가'][i-self.nLen2-1]) and (df['종가'][i-1] > df['종가'][i-self.nLen3-1]):
                    df.loc[i, 'MP'] = 1
                    df.loc[i, 'EntryLv'] = df['시가'][i]
                    self.nLoc = i
            if df['MP'][i] != -1:
                if (df['AvgVal'][i-1] < df['AvgVal'][i-self.nLen2-1]) and (df['종가'][i-1] > df['종가'][i-self.nLen2-1]) and (df['종가'][i-1] < df['종가'][i-self.nLen3-1]):
                    df.loc[i, 'MP'] = -1
                    df.loc[i, 'EntryLv'] = df['시가'][i]
                    self.nLoc = i
            
            # Exit
            if df['MP'][i] != 0:
                if df['MP'][i] == 1:
                    if self.fEL_BPS != 0.0 and df['저가'][i] <= self.fEL_BPS:   # Big Profit Stop(execution)
                        if df['시가'][i] < self.fEL_BPS:
                            df.loc[i, 'ExitLv'] = df['시가'][i]
                        else:
                            df.loc[i, 'ExitLv'] = self.fEL_BPS
                        df.loc[i, 'MP'] = 0
                        self.fEL_BPS = 0.0
                    if self.fEL_PS != 0.0 and df['저가'][i] <= self.fEL_PS:    # Protective Stop (execution)
                        if df['시가'][i] < self.fEL_PS:
                            df.loc[i, 'ExitLv'] = df['시가'][i]
                        else:
                            df.loc[i, 'ExitLv'] = self.fEL_PS
                        df.loc[i, 'MP'] = 0
                        self.fEL_PS = 0.0
                    if self.fEL_TS != 0.0 and df['저가'][i] <= self.fEL_TS:    # Trailing Stop (execution)
                        if df['시가'][i] < self.fEL_TS:
                            df.loc[i, 'ExitLv'] = df['시가'][i]
                        else:
                            df.loc[i, 'ExitLv'] = self.fEL_TS
                        df.loc[i, 'MP'] = 0
                        self.fEL_TS = 0.0
                if df['MP'][i] == -1:
                    if self.fES_BPS != 0.0 and df['고가'][i] >= self.fES_BPS:
                        if df['시가'][i] > self.fES_BPS:
                            df.loc[i, 'ExitLv'] = df['시가'][i]
                        else:
                            df.loc[i, 'ExitLv'] = self.fES_BPS
                        df.loc[i, 'MP'] = 0
                        self.fES_BPS = 0.0
                    if self.fES_PS != 0.0 and df['고가'][i] >= self.fES_PS:
                        if df['시가'][i] > self.fES_PS:
                            df.loc[i, 'ExitLv'] = df['시가'][i]
                        else:
                            df.loc[i, 'ExitLv'] = self.fES_PS
                        df.loc[i, 'MP'] = 0
                        self.fES_PS = 0.0
                    if self.fES_TS != 0.0 and df['고가'][i] >= self.fES_TS:
                        if df['시가'][i] > self.fES_TS:
                            df.loc[i, 'ExitLv'] = df['시가'][i]
                        else:
                            df.loc[i, 'ExitLv'] = self.fES_TS
                        df.loc[i, 'MP'] = 0
                        self.fES_TS = 0.0

                self.fPosHigh = max(df['고가'][self.nLoc:i+1])
                self.fPosLow = min(df['저가'][self.nLoc:i+1])
                if df['MP'][i] == 1:
                    if self.fPosHigh > df['EntryLv'][i] + df['ATR'][i] * self.nBigProfitATRs:   # Big Profit Stop (setup)
                        self.fEL_BPS = min(df['저가'][i-2:i+1])
                    self.fEL_PS = df['EntryLv'][i] - df['ATR'][i] * self.nProtectiveATRs    # Protective Stop (setup)
                    self.fEL_TS = self.fPosHigh - df['ATR'][i] * self.nTrailingATRs # Trailing Stop (setup)
                if df['MP'][i] == -1:
                    if self.fPosLow < abs(df['EntryLv'][i]) - df['ATR'][i] * self.nBigProfitATRs:
                        self.fES_BPS = max(df['고가'][i-2:i+1])
                    self.fES_PS = abs(df['EntryLv'][i]) + df['ATR'][i] * self.nProtectiveATRs
                    self.fES_TS = self.fPosLow + df['ATR'][i] * self.nTrailingATRs

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
                # Entry
                df = self.lstData[self.ix]
                df.loc[0, 'MP'] = df['MP'][1]
                tNow = dt.now().time()
                if tNow.hour < 9:   # 9시 전이면
                    if df['MP'][0] != 1:
                        if (df['AvgVal'][1] > df['AvgVal'][self.nLen2+1]) and (df['종가'][1] < df['종가'][self.nLen2+1]) and (df['종가'][1] > df['종가'][self.nLen3+1]):
                            Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt_entry, 0)   # 동호 매수
                            df.loc[0, 'MP'] = 1
                            self.logger.info('Buy %s amount ordered', self.amt_entry)
                    if df['MP'][0] != -1:
                        if (df['AvgVal'][1] < df['AvgVal'][self.nLen2+1]) and (df['종가'][1] > df['종가'][self.nLen2+1]) and (df['종가'][1] < df['종가'][self.nLen3+1]):
                            Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt_entry, 0)   # 동호 매도
                            df.loc[0, 'MP'] = -1
                            self.logger.info('Sell %s amount ordered', self.amt_entry)

        else:
            if self.nPosition == 0: # 실시간 청산만 있는 전략이므로, 포지션 없을땐 패스
                return

            df = self.lstData[self.ix]
            if self.npPriceInfo == None:    # 첫 데이터 수신시
                self.npPriceInfo = PriceInfo.copy()
                if PriceInfo['현재가'] == PriceInfo['시가']:    # 장 중간 시스템 작동시 주문 방지 
                    self.npPriceInfo['현재가'] = df['종가'][1]

            # Exit
            if df['MP'][0] == 1:    # EL
                if self.fEL_BPS != 0.0: # Big Profit Stop
                    if (self.npPriceInfo['현재가'] > self.fEL_BPS) and (PriceInfo['현재가'] <= self.fEL_BPS):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'EL', self.amt_exit, PriceInfo['현재가'])
                        self.logger.info('EL_BPS %s amount ordered', self.amt_exit)
                        df.loc[0, 'MP'] = 0
                        self.fEL_BPS = 0.0
                if self.fEL_PS != 0.0:  # Protective Stop
                    if (self.npPriceInfo['현재가'] > self.fEL_PS) and (PriceInfo['현재가'] <= self.fEL_PS):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'EL', self.amt_exit, PriceInfo['현재가'])
                        self.logger.info('EL_PS %s amount ordered', self.amt_exit)
                        df.loc[0, 'MP'] = 0
                        self.fEL_PS = 0.0
                if self.fEL_TS != 0.0:  # Trailing Stop
                    if (self.npPriceInfo['현재가'] > self.fEL_TS) and (PriceInfo['현재가'] <= self.fEL_TS):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'EL', self.amt_exit, PriceInfo['현재가'])
                        self.logger.info('EL_TS %s amount ordered', self.amt_exit)
                        df.loc[0, 'MP'] = 0
                        self.fEL_TS = 0.0
            if df['MP'][0] == -1:   # ES
                if self.fES_BPS != 0.0: # Big Profit Stop
                    if (self.npPriceInfo['현재가'] < self.fES_BPS) and (PriceInfo['현재가'] >= self.fES_BPS):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'ES', self.amt_exit, PriceInfo['현재가'])
                        self.logger.info('ES_BPS %s amount ordered', self.amt_exit)
                        df.loc[0, 'MP'] = 0
                        self.fES_BPS = 0.0
                if self.fES_PS != 0.0:  # Protective Stop
                    if (self.npPriceInfo['현재가'] < self.fES_PS) and (PriceInfo['현재가'] >= self.fES_PS):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'ES', self.amt_exit, PriceInfo['현재가'])
                        self.logger.info('ES_PS %s amount ordered', self.amt_exit)
                        df.loc[0, 'MP'] = 0
                        self.fES_PS = 0.0
                if self.fES_TS != 0.0:  # Trailing Stop
                    if (self.npPriceInfo['현재가'] < self.fES_TS) and (PriceInfo['현재가'] >= self.fES_TS):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'ES', self.amt_exit, PriceInfo['현재가'])
                        self.logger.info('ES_TS %s amount ordered', self.amt_exit)
                        df.loc[0, 'MP'] = 0
                        self.fES_TS = 0.0

            self.npPriceInfo = PriceInfo.copy()