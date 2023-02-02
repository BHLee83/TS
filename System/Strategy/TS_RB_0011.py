from System.strategy import Strategy

import pandas as pd
import logging



class TS_RB_0011():
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


    # 과거 데이터 생성 (인디로 수신시 일봉은 연결선물, 분봉은 근월물 코드로 생성)
    def createHistData(self, instInterface):
        # for i, v in enumerate(self.lstProductCode):
        for i, v in enumerate(self.lstProductNCode):
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
        strColName1 = 'BBUP1'
        strColName2 = 'BBUP2'
        strColName3 = 'BBDN1'
        strColName4 = 'BBDN2'
        ma1 = df['종가'].rolling(window=self.nP1).mean()
        ma2 = df['종가'].rolling(window=self.nP2).mean()
        std1 = df['종가'].rolling(window=self.nP1).std()
        std2 = df['종가'].rolling(window=self.nP2).std()
        df[strColName1] = ma1 + self.fMulti * std1
        df[strColName2] = ma2 + self.fMulti * std2
        df[strColName3] = ma1 - self.fMulti * std1
        df[strColName4] = ma2 - self.fMulti * std2
        df['MP'] = 0
        df['SL'] = 0.0
        df['EL'] = 0.0
        df['ES'] = 0.0
        for i in df.index:
            if i > self.nP1:
                self.nTrend = 0
                self.boolNewHigh = False
                self.boolNewLow = False
                df.loc[i, 'MP'] = df['MP'][i-1]
                df.loc[i, 'SL'] = df['SL'][i-1]
                df.loc[i, 'EL'] = df['EL'][i-1]
                df.loc[i, 'ES'] = df['ES'][i-1]

                if df['종가'][i] > df['BBUP1'][i]:
                    self.nBBTrend = 1
                if df['종가'][i] < df['BBDN1'][i]:
                    self.nBBTrend = -1
                if (self.nBBTrend == 1) and (df['종가'][i-1] > df['BBDN2'][i-1]) and (df['종가'][i] < df['BBDN2'][i]):
                    self.nBuyCond = 1
                if (self.nBBTrend == -1) and (df['종가'][i-1] < df['BBUP2'][i-1]) and (df['종가'][i] > df['BBUP2'][i]):
                    self.nSellCond = -1
                    
                if ((df['종가'][i-1] < df['BBUP1'][i-1]) and (df['종가'][i] > df['BBUP1'][i])) \
                    or ((df['종가'][i-1] > df['BBDN1'][i-1]) and (df['종가'][i] < df['BBDN1'][i])):
                    self.nBuyCond = 0
                    self.nSellCond = 0
                
                # Entry
                if (df['MP'][i] != 1) and (self.nBuyCond == 1):
                    if df['고가'][i] >= df['고가'][i-1]:
                        df.loc[i, 'MP'] = 1
                        df.loc[i, 'SL'] = min(df['저가'][i-1:i+1].values)    # SL(EL) price
                if (df['MP'][i] != -1) and (self.nSellCond == -1):
                    if df['저가'][i] <= df['저가'][i-1]:
                        df.loc[i, 'MP'] = -1
                        df.loc[i, 'SL'] = max(df['고가'][i-1:i+1].values)   # SL(ES) price
                
                if i >= self.nSwingP - 1:
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
                    df.loc[i, 'EL'] = self.dLowStop
                if df['MP'][i] == -1 and df['고가'][i] < self.dHighStop:
                    df.loc[i, 'ES'] = self.dHighStop
                if df['MP'][i] == 1 and df['저가'][i] <= df['EL'][i]:
                    df.loc[i, 'MP'] = 0
                if df['MP'][i] == -1 and df['고가'][i] >= df['ES'][i]:
                    df.loc[i, 'MP'] = 0

                if df['MP'][i] == 1:    # Stop loss
                    if df['저가'][i] < df['SL'][i]:    # Exit long
                        df.loc[i, 'MP'] = 0
                if df['MP'][i] == -1:
                    if df['고가'][i] > df['SL'][i]:    # Exit short
                            df.loc[i, 'MP'] = 0

        df = df.sort_index(ascending=False).reset_index()
        self.lstData[self.ix]['MP'] = df['MP']
        self.lstData[self.ix]['SL'] = df['SL']
        self.lstData[self.ix]['EL'] = df['EL']
        self.lstData[self.ix]['ES'] = df['ES']


    # 전략
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행인 경우에만
            self.lstData[self.ix] = self.getHistData()
            if self.lstData[self.ix].empty:
                return False
            else:
                self.applyChart()
                if Strategy.dfPosition.empty:   # 포지션 확인 및 수량 지정
                    self.nPosition = 0
                else:
                    try:
                        self.nPosition = Strategy.dfPosition['POSITION'][(Strategy.dfPosition['STRATEGY_ID']==__class__.__name__) \
                                            & (Strategy.dfPosition['ASSET_NAME']==self.lstAssetCode[self.ix]) \
                                            & (Strategy.dfPosition['ASSET_TYPE']==self.lstAssetType[self.ix])].values[0]
                    except:
                        self.nPosition = 0
                self.amt_entry = abs(self.nPosition) + self.lstTrUnit[self.ix] * self.fWeight
                self.amt_exit = abs(self.nPosition)
        else:
            df = self.lstData[self.ix]
            if self.npPriceInfo == None:
                if PriceInfo['현재가'] == PriceInfo['시가']:
                    if (df['MP'][0] != 1) and (self.nBuyCond == 1): # Entry
                        if PriceInfo['현재가'] >= df['고가'][1]:
                            Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 시초가 매수
                            df.loc[0, 'MP'] = 1
                            self.logger.info('Buy %s amount ordered', self.amt_entry)
                    if (df['MP'][0] != -1) and (self.nSellCond == -1):
                        if PriceInfo['현재가'] <= df['저가'][1]:
                            Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])   # 시초가 매도
                            df.loc[0, 'MP'] = -1
                            self.logger.info('Sell %s amount ordered', self.amt_entry)
            else:
                if (df['MP'][0] != 1) and (self.nBuyCond == 1): # Entry
                    if (self.npPriceInfo['현재가'] < df['고가'][1]) and (PriceInfo['현재가'] >= df['고가'][1]):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 매수
                        df.loc[0, 'MP'] = 1
                        self.logger.info('Buy %s amount ordered', self.amt_entry)
                if (df['MP'][0] != -1) and (self.nSellCond == -1):
                    if (self.npPriceInfo['현재가'] > df['저가'][1]) and (PriceInfo['현재가'] <= df['저가'][1]):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])   # 매도
                        df.loc[0, 'MP'] = -1
                        self.logger.info('Sell %s amount ordered', self.amt_entry)

                # Trail stop
                self.boolNewHigh = False
                self.boolNewLow = False
                self.nTrend = 0
                if PriceInfo['고가'] > max(df['고가'][1:self.nSwingP].values):
                    self.boolNewHigh = True
                if PriceInfo['저가'] < min(df['저가'][1:self.nSwingP].values):
                    self.boolNewHigh = True

                if self.boolNewHigh and self.boolNewLow:
                    if self.strLastSwingPt == 'LOW':
                        if self.lstLowPts[-1] > PriceInfo['저가']:
                            self.nTrend = -1
                        else:
                            self.nTrend = 1
                    elif self.strLastSwingPt == 'HIGH':
                        if self.lstHighPts[-1] < PriceInfo['고가']:
                            self.nTrend = 1
                        else:
                            self.nTrend = -1
                elif self.boolNewHigh:
                    self.nTrend = 1
                elif self.boolNewLow:
                    self.nTrend = -1

                if self.nTrend == 1:
                    if self.strLastSwingPt == 'LOW':
                        self.lstHighPts[-1] = PriceInfo['고가']
                    elif self.lstHighPts[-1] < PriceInfo['고가']:
                        self.lstHighPts[-1] = PriceInfo['고가']
                    self.strLastSwingPt = 'HIGH'
                if self.nTrend == -1:
                    if self.strLastSwingPt == 'HIGH':
                        self.lstLowPts[-1] = PriceInfo['저가']
                    elif self.lstLowPts[-1] > PriceInfo['저가']:
                        self.lstLowPts[-1] = PriceInfo['저가']
                    self.strLastSwingPt = 'LOW'

                if self.nTrend == 1:
                    self.dHighStop = self.lstHighPts[-2]
                else:
                    self.dHighStop = self.lstHighPts[-1]
                if self.nTrend == -1:
                    self.dLowStop = self.lstLowPts[-2]
                else:
                    self.dLowStop = self.lstLowPts[-1]

                if (df['MP'][1] == 1) and (PriceInfo['저가'] > self.dLowStop):
                    df.loc[0, 'EL'] = self.dLowStop
                if (df['MP'][1] == -1) and (PriceInfo['고가'] < self.dHighStop):
                    df.loc[0, 'ES'] = self.dHighStop
                if (df['MP'][1] == 1) and (self.npPriceInfo['현재가'] > df['EL'][0]) and (PriceInfo['현재가'] <= df['EL'][0]):
                    Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt_exit, PriceInfo['현재가'])   # 매수 청산
                    df.loc[0, 'MP'] = 0
                    self.logger.info('ExitLong %s amount ordered', self.amt_exit)
                if (df['MP'][1] == -1) and (self.npPriceInfo['현재가'] < df['ES'][0]) and (PriceInfo['현재가'] >= df['ES'][0]):
                    Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt_exit, PriceInfo['현재가'])   # 매도 청산
                    df.loc[0, 'MP'] = 0
                    self.logger.info('ExitShort %s amount ordered', self.amt_exit)

                # Stop loss
                if df['MP'][1] == 1:
                    if (self.npPriceInfo['현재가'] > df['SL'][1]) and (PriceInfo['현재가'] <= df['SL'][1]):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt_exit, PriceInfo['현재가'])   # 매수 청산
                        df.loc[0, 'MP'] = 0
                        self.logger.info('StopLong %s amount ordered', self.amt_exit)
                if df['MP'][1] == -1:
                    if (self.npPriceInfo['현재가'] < df['SL'][1]) and (PriceInfo['현재가'] >= df['SL'][1]):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt_exit, PriceInfo['현재가'])   # 매도 청산
                        df.loc[0, 'MP'] = 0
                        self.logger.info('StopShort %s amount ordered', self.amt_exit)

            self.npPriceInfo = PriceInfo.copy()