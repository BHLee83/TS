# 타임프레임: 5분봉
# - Setup
#   
# - Entry
#   
# - Exit
#   
# 수수료: 0.0006%
# 슬리피지: 0.05pt

from System.strategy import Strategy

import pandas as pd
import datetime as dt
import logging


class TS_RB_0044():
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
        self.fR = 0.3
        self.nEntryLimit = 153000
        self.dtEntryLimit = dt.time(15,30)
        self.nDaysSinceEntry = 0
        self.nDayIn = 3

        self.fDayOpen = 0.0
        self.fDayHigh = 0.0
        self.fDayLow = 0.0
        self.fDayRange_t1 = 0.0
        self.fBuyPrice = 0.0
        self.fSellPrice = 0.0
        self.fEL_TS = 0.0
        self.fES_TS = 0.0
        self.nEntryIdx = 0
        self.strEntryDate = ''
        self.strExitName = ''
        self.strExitDate = ''
        self.bCond1 = False
        self.bCond2 = False
        self.bCond3 = False
        self.bCond4 = False
        

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

        # dfSignal = pd.DataFrame(None, columns=df.columns)
        for i in df.index-1:
            if i < 80:
                continue

            df.loc[i, 'MP'] = df['MP'][i-1]
            df.loc[i, 'EntryLv'] = df['EntryLv'][i-1]
            df.loc[i, 'ExitLv'] = df['ExitLv'][i-1]
            
            # Setup
            self.bCond1 = False
            self.bCond2 = False
            self.bCond3 = False
            self.bCond4 = False
            if df['일자'][i] != df['일자'][i-1]:
                self.fDayOpen = df['시가'][i]
                self.fDayHigh = df['고가'][i]
                self.fDayLow = df['저가'][i]
                dayHigh_t1 = df[df['일자'] == df['일자'][i-1]]['고가'].max()
                dayLow_t1 = df[df['일자'] == df['일자'][i-1]]['저가'].min()
                self.fDayRange_t1 = dayHigh_t1 - dayLow_t1
                self.fBuyPrice = self.fDayOpen + self.fDayRange_t1 * self.fR
                self.fSellPrice = self.fDayOpen - self.fDayRange_t1 * self.fR
                # Profitable Open Stop
                if df['MP'][i] != 0:
                    self.nDaysSinceEntry = self.nDaysSinceEntry + 1
                    if self.nDaysSinceEntry == self.nDayIn:
                        if (df['MP'][i] > 0) and (df['EntryLv'][i] < self.fDayOpen):
                            df.loc[i, 'ExitLv'] = df['시가'][i]
                            df.loc[i, 'MP'] = 0
                            self.strEntryDate = df['일자'][self.nEntryIdx]
                            self.strExitDate = df['일자'][i]
                            self.strExitName = 'EL_OS'
                            self.fEL_TS = 0.0
                        if (df['MP'][i] < 0) and (df['EntryLv'][i] > self.fDayOpen):
                            df.loc[i, 'ExitLv'] = df['시가'][i]
                            df.loc[i, 'MP'] = 0
                            self.strEntryDate = df['일자'][self.nEntryIdx]
                            self.strExitDate = df['일자'][i]
                            self.strExitName = 'ES_OS'
                            self.fES_TS = 0.0
                
            if self.fDayOpen == 0:
                continue

            if df['MP'][i] == 0:
                self.nDaysSinceEntry = 0
            if self.fDayHigh < df['고가'][i]:
                self.fDayHigh = df['고가'][i]
            if self.fDayLow > df['저가'][i]:
                self.fDayLow = df['저가'][i]
                
            if (self.strExitName == 'EL_TS') and (self.strExitDate == df['일자'][i]) and (self.strEntryDate == df['일자'][i]) and (df['ExitLv'][i] > (self.fDayOpen + self.fDayRange_t1 * self.fR)):
                self.bCond1 = True
            if (self.strExitName == 'ES_TS') and (self.strExitDate == df['일자'][i]) and (self.strEntryDate == df['일자'][i]) and (df['ExitLv'][i] < (self.fDayOpen - self.fDayRange_t1 * self.fR)):
                self.bCond2 = True
            if (self.strExitName == 'EL_TS') and (self.strExitDate == df['일자'][i]) and (self.strEntryDate != df['일자'][i]) and (df['ExitLv'][i] > (self.fDayOpen + self.fDayRange_t1 * self.fR)):
                self.bCond3 = True
            if (self.strExitName == 'ES_TS') and (self.strExitDate == df['일자'][i]) and (self.strEntryDate != df['일자'][i]) and (df['ExitLv'][i] < (self.fDayOpen - self.fDayRange_t1 * self.fR)):
                self.bCond4 = True

            if df['일자'][i] == df['일자'][i+1]:
                # Entry
                if int(df['시간'][i]) <= self.nEntryLimit:
                    if (self.bCond1 == False) and (self.bCond3 == False) and (df['MP'][i] <= 0): # setup
                        if self.fBuyPrice != 0.0 and df['고가'][i] >= self.fBuyPrice:    # execution
                            df.loc[i, 'MP'] = 1
                            df.loc[i, 'EntryLv'] = max(self.fBuyPrice, df['시가'][i])
                            self.strEntryDate = df['일자'][self.nEntryIdx]
                            if df['MP'][i-1] != 0:
                                self.strExitDate = df['일자'][i]
                                self.strExitName = 'EL_RS'
                            self.nEntryIdx = i
                    if (self.bCond2 == False) and (self.bCond4 == False) and (df['MP'][i] >= 0):
                        if self.fSellPrice != 0.0 and df['저가'][i] <= self.fSellPrice:
                            df.loc[i, 'MP'] = -1
                            df.loc[i, 'EntryLv'] = min(self.fSellPrice, df['시가'][i])
                            self.strEntryDate = df['일자'][self.nEntryIdx]
                            if df['MP'][i-1] != 0:
                                self.strExitDate = df['일자'][i]
                                self.strExitName = 'ES_RS'
                            self.nEntryIdx = i
                    
            else:
                # Exit - end of day
                if df['MP'][i-1] == 1:
                    if (df['EntryLv'][i-1] > df['종가'][i-1]) or (self.fDayLow < (self.fDayOpen - self.fDayRange_t1 * self.fR1)):
                        df.loc[i, 'ExitLv'] = df['종가'][i]
                        df.loc[i, 'MP'] = 0
                        self.strEntryDate = df['일자'][self.nEntryIdx]
                        self.strExitDate = df['일자'][i]
                        self.strExitName = 'EL_ED'
                if df['MP'][i-1] == -1:
                    if (df['EntryLv'][i-1] < df['종가'][i-1]) or (self.fDayHigh > (self.fDayOpen + self.fDayRange_t1 * self.fR1)):
                        df.loc[i, 'ExitLv'] = df['종가'][i]
                        df.loc[i, 'MP'] = 0
                        self.strEntryDate = df['일자'][self.nEntryIdx]
                        self.strExitDate = df['일자'][i]
                        self.strExitName = 'ES_ED'

            # Position check
        #     if df['MP'][i] != df['MP'][i-1]:
        #         dfSignal.loc[len(dfSignal)] = df.loc[i, :]

        # print(dfSignal)


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
            if df.iloc[-2]['일자'] != df.iloc[-1]['일자']:
                self.fDayOpen = PriceInfo['현재가']
                dayHigh_t1 = df[df['일자'] == df.iloc[-2]['일자']]['고가'].max()
                dayLow_t1 = df[df['일자'] == df.iloc[-2]['일자']]['저가'].min()
                self.fDayRange_t1 = dayHigh_t1 - dayLow_t1
                self.fBuyPrice = self.fDayOpen + self.fDayRange_t1 * self.fR
                self.fSellPrice = self.fDayOpen - self.fDayRange_t1 * self.fR
                # Profitable Open Stop
                if self.nDaysSinceEntry == self.nDayIn-1:
                    if (self.nPosition > 0) and (df.iloc[-1]['MP'] > 0) and (df.iloc[-2]['EntryLv'] < self.fDayOpen):
                        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'EL', self.amt_exit, PriceInfo['현재가'])
                        df.loc[len(df)-1, 'MP'] = 0
                        self.logger.info('ExitLong %s amount ordered', self.amt_exit)
                        self.chkPos(-self.amt_exit)
                    if (self.nPosition < 0) and (df.iloc[-1]['MP'] < 0) and (df.iloc[-2]['EntryLv'] > self.fDayOpen):
                        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'ES', self.amt_exit, PriceInfo['현재가'])
                        df.loc[len(df)-1, 'MP'] = 0
                        self.logger.info('ExitShort %s amount ordered', self.amt_exit)
                        self.chkPos(self.amt_exit)

        # 분봉 업데이트 시
        if (str(self.npPriceInfo['체결시간'])[4:6] != str(PriceInfo['체결시간'])[4:6]) and \
        (int(str(PriceInfo['체결시간'])[4:6]) % int(self.lstTimeIntrvl[self.ix]) == 0):
            self.common()
            df.loc[len(df)-1, 'MP'] = df.iloc[-2]['MP']
            

        # Entry
        if dt.datetime.now().time() < self.dtEntryLimit:
            if self.fBuyPrice != 0.0:
                if (self.nPosition <= 0) and (df.iloc[-1]['MP'] <= 0):
                    if (self.npPriceInfo['현재가'] <= self.fBuyPrice) and (PriceInfo['현재가'] >= self.fBuyPrice):
                        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])
                        df.loc[len(df)-1, 'MP'] = 1
                        self.logger.info('Buy %s amount ordered', self.amt_entry)
                        self.chkPos(self.amt_entry)
            if self.fSellPrice != 0.0:
                if (self.nPosition >= 0) and (df.iloc[-1]['MP'] >= 0):
                    if (self.npPriceInfo['현재가'] >= self.fSellPrice) and (PriceInfo['현재가'] <= self.fSellPrice):
                        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])
                        df.loc[len(df)-1, 'MP'] = -1
                        self.logger.info('Sell %s amount ordered', self.amt_entry)
                        self.chkPos(-self.amt_entry)

        self.npPriceInfo = PriceInfo.copy()


    def lastProc(self):
        if self.isON == False:    # 당일 종가 청산
            df = self.lstData[self.ix]
            if (self.nPosition > 0) and (df.iloc[-1]['MP'] > 0):
                if (df.iloc[-2]['EntryLv'] > df.iloc[-2]['종가']) or (self.fDayLow < (self.fDayOpen - self.fDayRange_t1 * self.fR)):
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_exit, 0)
                    df.loc[len(df)-1, 'MP'] = 0
                    self.logger.info('ExitLong %s amount ordered', self.amt_exit)
                    self.chkPos(-self.amt_exit)
            if (self.nPosition < 0) and (df.iloc[-1]['MP'] < 0):
                if (df.iloc[-2]['EntryLv'] < df.iloc[-2]['종가']) or (self.fDayHigh > (self.fDayOpen + self.fDayRange_t1 * self.fR)):
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_exit, 0)
                    df.loc[len(df)-1, 'MP'] = 0
                    self.logger.info('ExitShort %s amount ordered', self.amt_exit)
                    self.chkPos(self.amt_exit)