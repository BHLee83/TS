from System.strategy import Strategy
from System.indicator import Indicator

import pandas as pd
import logging



class TS_RB_0011():
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
        self.nSwingP = 5
        self.nP1 = 30
        self.nP2 = 15
        self.fMulti = 1.0
        self.nBBTrend = 0
        self.nBuyCond = 0
        self.nSellCond = 0
        self.boolNewHigh = False
        self.boolNewLow = False
        self.nTrend = 0
        self.strLastSwingPt = ''
        self.lstHighPts = [9999.9, 9999.9]
        self.lstLowPts = [0.0, 0.0]
        self.dHighStop = 0.0
        self.dLowStop = 0.0
        self.fBPrice = 0.0
        self.fSPrice = 0.0
        self.fSL = 0.0
        self.fEL = 0.0
        self.fES = 0.0


    # 공통 프로세스
    def common(self):
        # Data load & apply
        self.lstData[self.ix] = Strategy.getHistData(self.lstProductCode[self.ix], self.lstTimeFrame[self.ix], 100)
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
        strColName1 = 'BBUP1'
        strColName2 = 'BBUP2'
        strColName3 = 'BBDN1'
        strColName4 = 'BBDN2'
        ma1 = Indicator.MA(df['종가'], self.nP1)
        ma2 = Indicator.MA(df['종가'], self.nP2)
        std1 = df['종가'].rolling(window=self.nP1).std()
        std2 = df['종가'].rolling(window=self.nP2).std()
        df[strColName1] = ma1 + self.fMulti * std1
        df[strColName2] = ma2 + self.fMulti * std2
        df[strColName3] = ma1 - self.fMulti * std1
        df[strColName4] = ma2 - self.fMulti * std2
        df['MP'] = 0
        for i in df.index-1:
            if i < self.nP1:
                continue

            self.nTrend = 0
            self.boolNewHigh = False
            self.boolNewLow = False
            df.loc[i, 'MP'] = df['MP'][i-1]
            
            if df['종가'][i] > df['BBUP1'][i]:
                self.nBBTrend = 1
            if df['종가'][i] < df['BBDN1'][i]:
                self.nBBTrend = -1
            if (self.nBBTrend == 1) and (df['종가'][i-1] >= df['BBDN2'][i-1]) and (df['종가'][i] <= df['BBDN2'][i]):
                self.nBuyCond = 1
            if (self.nBBTrend == -1) and (df['종가'][i-1] <= df['BBUP2'][i-1]) and (df['종가'][i] >= df['BBUP2'][i]):
                self.nSellCond = -1
                
            if ((df['종가'][i-1] <= df['BBUP1'][i-1]) and (df['종가'][i] >= df['BBUP1'][i])) \
                or ((df['종가'][i-1] >= df['BBDN1'][i-1]) and (df['종가'][i] <= df['BBDN1'][i])):
                self.nBuyCond = 0
                self.nSellCond = 0
            
            # Entry
            if self.fBPrice != 0.0:    # execution
                if df['고가'][i] >= self.fBPrice:
                    df.loc[i, 'MP'] = 1
                    self.fSL = min(df['저가'][i-1:i+1].values)    # SL(EL) price
                    self.fBPrice = 0.0
            if self.fSPrice != 0.0:
                if df['저가'][i] <= self.fSPrice:
                    df.loc[i, 'MP'] = -1
                    self.fSL = max(df['고가'][i-1:i+1].values)   # SL(ES) price
                    self.fSPrice = 0.0

            if (df['MP'][i] != 1) and (self.nBuyCond == 1): # setup
                self.fBPrice = df['고가'][i]
            else:
                self.fBPrice = 0.0
            if (df['MP'][i] != -1) and (self.nSellCond == -1):
                self.fSPrice = df['저가'][i]
            else:
                self.fSPrice = 0.0
            
            # Exit
            if self.fSL != 0.0: # Stop loss
                if df['MP'][i-1] == 1:  # Exit long
                    if df['저가'][i-1] <= self.fSL:
                        df.loc[i, 'MP'] = 0
                        self.fSL = 0.0
                if df['MP'][i-1] == -1: # Exit short
                    if df['고가'][i] >= self.fSL:
                        df.loc[i, 'MP'] = 0
                        self.fSL = 0.0

            if self.fEL != 0.0: # Trail stop (execution)
                if df['MP'][i-1] == 1 and df['저가'][i] <= self.fEL:
                    df.loc[i, 'MP'] = 0
                    self.fEL = 0.0
            if self.fES != 0.0:
                if df['MP'][i-1] == -1 and df['고가'][i] >= self.fES:
                    df.loc[i, 'MP'] = 0
                    self.fES = 0.0

            if i >= self.nSwingP - 1:   # Trail stop (setup)
                if df['고가'][i] > max(df['고가'][i-self.nSwingP+1:i].values):  # 신고가
                    self.boolNewHigh = True
                if df['저가'][i] < min(df['저가'][i-self.nSwingP+1:i].values):  # 신저가
                    self.boolNewLow = True
            if self.boolNewHigh and self.boolNewLow:  # 신고가, 신저가 동시 발생
                if self.strLastSwingPt == 'LOW':
                    if self.lstLowPts[-1] > df['저가'][i]:
                        self.nTrend = -1
                    else:
                        self.nTrend = 1
                elif self.strLastSwingPt == 'HIGH':
                    if self.lstHighPts[-1] < df['고가'][i]:
                        self.nTrend = 1
                    else:
                        self.nTrend = -1
            elif self.boolNewHigh:
                self.nTrend = 1
            elif self.boolNewLow:
                self.nTrend = -1
            
            if self.nTrend == 1:    # Up trend
                if self.strLastSwingPt == 'LOW':
                    self.lstHighPts.append(df['고가'][i])
                elif df['고가'][i] > self.lstHighPts[-1]:
                    self.lstHighPts[-1] = df['고가'][i]
                self.strLastSwingPt = 'HIGH'
            if self.nTrend == -1:   # Down trend
                if self.strLastSwingPt == 'HIGH':
                    self.lstLowPts.append(df['저가'][i])
                elif df['저가'][i] < self.lstLowPts[-1]:
                    self.lstLowPts[-1] = df['저가'][i]
                self.strLastSwingPt = 'LOW'
            
            if self.nTrend == 1:
                self.dHighStop = self.lstHighPts[-2]
            else:
                self.dHighStop = self.lstHighPts[-1]
            if self.nTrend == -1:
                self.dLowStop = self.lstLowPts[-2]
            else:
                self.dLowStop = self.lstLowPts[-1]

            if df['MP'][i] == 1 and df['저가'][i] > self.dLowStop:
                self.fEL = self.dLowStop
            if df['MP'][i] == -1 and df['고가'][i] < self.dHighStop:
                self.fES = self.dHighStop


    # 전략
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행시
            self.common()
            self.chkPos()
            self.lstData[self.ix].loc[len(self.lstData[self.ix])-1, 'MP'] = self.lstData[self.ix].iloc[-2]['MP']
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
        if (df.iloc[-1]['MP'] != 1) and (self.fBPrice != 0.0):
            if (self.npPriceInfo['현재가'] <= df.iloc[-2]['고가']) and (PriceInfo['현재가'] >= df.iloc[-2]['고가']):
                Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 매수
                df.loc[len(df)-1, 'MP'] = 1
                self.fBPrice = 0.0
                self.logger.info('Buy %s amount ordered', self.amt_entry)
                self.chkPos(self.amt_entry)
        if (df.iloc[-1]['MP'] != -1) and (self.fSPrice != 0.0):
            if (self.npPriceInfo['현재가'] >= df.iloc[-2]['저가']) and (PriceInfo['현재가'] <= df.iloc[-2]['저가']):
                Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])   # 매도
                df.loc[len(df)-1, 'MP'] = -1
                self.fSPrice = 0.0
                self.logger.info('Sell %s amount ordered', self.amt_entry)
                self.chkPos(-self.amt_entry)

        # Exit
        if self.fSL != 0.0: # Stop loss
            if (df.iloc[-2]['MP'] == 1) and (df.iloc[-1]['MP'] == 1):
                if (self.npPriceInfo['현재가'] >= self.fSL) and (PriceInfo['현재가'] <= self.fSL):
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'EL', self.amt_exit, PriceInfo['현재가'])   # 매수 청산
                    df.loc[len(df)-1, 'MP'] = 0
                    self.fSL = 0.0
                    self.logger.info('StopLong %s amount ordered', self.amt_exit)
                    self.chkPos(-self.amt_exit)
            if (df.iloc[-2]['MP'] == -1) and (df.iloc[-1]['MP'] == -1):
                if (self.npPriceInfo['현재가'] <= self.fSL) and (PriceInfo['현재가'] >= self.fSL):
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'ES', self.amt_exit, PriceInfo['현재가'])   # 매도 청산
                    df.loc[len(df)-1, 'MP'] = 0
                    self.fSL = 0.0
                    self.logger.info('StopShort %s amount ordered', self.amt_exit)
                    self.chkPos(self.amt_exit)

        if self.fEL != 0.0: # Trail Stop
            if (df.iloc[-2]['MP'] == 1) and (df.iloc[-1]['MP'] == 1):
                if (self.npPriceInfo['현재가'] >= self.fEL) and (PriceInfo['현재가'] <= self.fEL):
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'EL', self.amt_exit, PriceInfo['현재가'])   # 매수 청산
                    df.loc[len(df)-1, 'MP'] = 0
                    self.fEL = 0.0
                    self.logger.info('ExitLong %s amount ordered', self.amt_exit)
                    self.chkPos(-self.amt_exit)
        if self.fES != 0.0:
            if (df.iloc[-2]['MP'] == -1) and (df.iloc[-1]['MP'] == -1):
                if (self.npPriceInfo['현재가'] <= self.fES) and (PriceInfo['현재가'] >= self.fES):
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'ES', self.amt_exit, PriceInfo['현재가'])   # 매도 청산
                    df.loc[len(df)-1, 'MP'] = 0
                    self.fES = 0.0
                    self.logger.info('ExitShort %s amount ordered', self.amt_exit)
                    self.chkPos(self.amt_exit)

        self.npPriceInfo = PriceInfo.copy()