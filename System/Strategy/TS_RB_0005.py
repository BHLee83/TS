from System.strategy import Strategy

import pandas as pd



class TS_RB_0005():
    def __init__(self, info) -> None:
        super().__init__()
    
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
        self.boolNewHigh = False
        self.boolNewLow = False
        self.nTrend = 0
        self.strLastSwingPt = ''
        self.lstHighPts = [9999.9, 9999.9]
        self.lstLowPts = [0.0, 0.0]
        self.dHighStop = 0.0
        self.dLowStop = 0.0


    # 과거 데이터 생성
    def createHistData(self, instInterface):
        for i, v in enumerate(self.lstProductNCode):
            if Strategy.getHistData(v, self.lstTimeFrame[i]) == False:
                instInterface.price.rqHistData(v, self.lstProductCode[i], self.lstTimeWnd[i], self.lstTimeIntrvl[i], Strategy.strStartDate, Strategy.strEndDate, Strategy.strRqCnt)
                instInterface.event_loop.exec_()


    # 과거 데이터 로드
    def getHistData(self):
        data = Strategy.getHistData(self.lstProductCode[self.ix], self.lstTimeFrame[self.ix])
        if type(data) == bool:
            if data == False:
                return pd.DataFrame(None)
        
        data = Strategy.convertNPtoDF(data)
        return data


    # 전략 적용
    def applyChart(self):   # Strategy apply on historical chart
        df = self.lstData[self.ix].sort_index(ascending=False).reset_index()
        df['MP'] = 0
        for i in df.index:
            if i > 0:
                self.nTrend = 0
                self.boolNewHigh = False
                self.boolNewLow = False
                df.loc[i, 'MP'] = df['MP'][i-1]
                
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
                if df['MP'][i] != 1 and df['고가'][i] >= self.dHighStop:
                    df.loc[i, 'MP'] = 1
                if df['MP'][i] != -1 and df['저가'][i] <= self.dLowStop:
                    df.loc[i, 'MP'] = -1

        df = df.sort_index(ascending=False).reset_index()
        self.lstData[self.ix]['MP'] = df['MP']


    # 전략
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행인 경우에만
            self.lstData[self.ix] = self.getHistData()
            if self.lstData[self.ix].empty:
                return False
            else:
                self.applyChart()
                try:
                    self.nPosition = Strategy.dfPosition['POSITION'][Strategy.dfPosition['STRATEGY_ID']==__class__.__name__ \
                                        and Strategy.dfPosition['ASSET_NAME']==self.lstAssetCode[self.ix] \
                                        and Strategy.dfPosition['ASSET_TYPE']==self.lstAssetType[self.ix]].values[0]
                except:
                    self.nPosition = 0
                self.amt_entry = abs(self.nPosition) + self.lstTrUnit[self.ix] * self.fWeight
                self.amt_exit = abs(self.nPosition)
        else:
            df = self.lstData[self.ix]
            if self.npPriceInfo == None:
                if df['MP'][1] == 0:    # Entry
                    if PriceInfo['현재가'] == PriceInfo['시가']:
                        if PriceInfo['현재가'] >= self.dHighStop:
                            Strategy.setOrder(self, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 시초가 매수
                            df.loc[0, 'MP'] = 1
                        if PriceInfo['현재가'] <= self.dLowStop:
                            Strategy.setOrder(self, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])   # 시초가 매도
                            df.loc[0, 'MP'] = -1
            else:
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

                if (df['MP'][1] != 1) and (df['MP'][0] != 1):
                    if (self.npPriceInfo['현재가'] < self.dHighStop) and (self.npPriceInfo['현재가'] >= self.dHighStop):
                        Strategy.setOrder(self, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 매수
                        df.loc[0, 'MP'] = 1
                if (df['MP'][1] != -1) and (df['MP'][0] != -1):
                    if (self.npPriceInfo['현재가'] > self.dLowStop) and (self.npPriceInfo['현재가'] <= self.dLowStop):
                        Strategy.setOrder(self, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])   # 매도
                        df.loc[0, 'MP'] = -1

            self.npPriceInfo = PriceInfo.copy()