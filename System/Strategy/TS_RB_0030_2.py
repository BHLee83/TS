# 타임프레임: 5분봉
# - Setup: 장 시작 후 InitMin 부터 15시 이전 까지만 거래
# - Entry
#   Long(Short): InitMin 이내의 고(저)가 돌파시
# - Exit
#   EL(ES): 매수(매도) 진입 이후 매도(매수) 조건 만족시 / 당일 종가 청산
# 수수료: 0.0006%
# 슬리피지: 0.05pt

from System.strategy import Strategy

import pandas as pd
import logging



class TS_RB_0030_2():
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
        self.fSetHigh = 0.0
        self.fSetLow = 0.0
        self.bBflag = False
        self.bSflag = False
        self.fBuyPrice = 0.0
        self.fSellPrice = 0.0
        self.fStopPrice = 0.0
        self.nLoc = 0


    # 공통 프로세스
    def common(self):
        # Data load & apply
        self.lstData[self.ix] = Strategy.getHistData(self.lstProductCode[self.ix], self.lstAssetType[self.ix], self.lstTimeFrame[self.ix], int(400/int(self.lstTimeIntrvl[self.ix]))*10)
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
        dfSignal = pd.DataFrame(None, columns=df.columns)
        for i in df.index-1:
            if i < 2:
                continue

            df.loc[i, 'MP'] = df['MP'][i-1]
            
            # Exit
            if df['MP'][i] == 1:
                if df['저가'][i] <= self.fSetLow:
                    df.loc[i, 'MP'] = 0
                self.bBflag = False
            if df['MP'][i] == -1:
                if df['고가'][i] >= self.fSetHigh:
                    df.loc[i, 'MP'] = 0
                self.bSflag = False
            
            # Setup
            t = int(df['시간'][i])
            if t <= 103000:
                if df['일자'][i] != df['일자'][i-1]:
                    self.fSetHigh = df['고가'][i]
                    self.fSetLow = df['저가'][i]
                    self.bBflag = True
                    self.bSflag = True
                else:
                    if df['고가'][i] > self.fSetHigh:
                        self.fSetHigh = df['고가'][i]
                    if df['저가'][i] < self.fSetLow:
                        self.fSetLow = df['저가'][i]
            else:
                # Entry
                if t <= 150000:
                    if self.bBflag and df['종가'][i-2] <= self.fSetHigh and df['종가'][i-1] >= self.fSetHigh:
                        df.loc[i, 'MP'] = 1
                    if self.bSflag and df['종가'][i-2] >= self.fSetLow and df['종가'][i-1] <= self.fSetLow:
                        df.loc[i, 'MP'] = -1

            if t == 154500:   # End of day exit
                df.loc[i, 'MP'] = 0

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
        
        df = self.lstData[self.ix]
        if (self.npPriceInfo == None) or (self.npPriceInfo['시가'] == 0):    # 첫 데이터 수신시
            if self.npPriceInfo == None:
                self.npPriceInfo = PriceInfo.copy()
                self.npPriceInfo['시가'] = df.iloc[-2]['시가']  # 전봉 정보 세팅
                self.npPriceInfo['고가'] = df.iloc[-2]['고가']
                self.npPriceInfo['저가'] = df.iloc[-2]['저가']
                self.npPriceInfo['현재가'] = df.iloc[-2]['종가']

        # 분봉 업데이트 시
        if (str(self.npPriceInfo['체결시간'])[4:6] != str(PriceInfo['체결시간'])[4:6]) and \
        (int(str(PriceInfo['체결시간'])[4:6]) % int(self.lstTimeIntrvl[self.ix]) == 0):
            self.common()
            df = self.lstData[self.ix]
            df.loc[len(df)-1, 'MP'] = df.iloc[-2]['MP']
            
            # Entry
            t = int(PriceInfo['체결시간'].decode())
            if (t > 103000) and (t < 150000):
                if (self.nPosition <= 0) and (df.iloc[-1]['MP'] <= 0):
                    if self.bBflag and (PriceInfo['현재가'] >= self.fSetHigh):
                        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 상품코드, 매수/매도, 계약수, 가격
                        df.loc[len(df)-1, 'MP'] = 1
                        self.logger.info('Buy %s amount ordered', self.amt_entry)
                        self.chkPos(self.amt_entry)
                if (self.nPosition >= 0) and (df.iloc[-1]['MP'] >= 0):
                    if self.bSflag and (PriceInfo['현재가'] <= self.fSetLow):
                        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])
                        df.loc[len(df)-1, 'MP'] = -1
                        self.logger.info('Sell %s amount ordered', self.amt_entry)
                        self.chkPos(-self.amt_entry)

        # Exit
        if (self.nPosition > 0) and (df.iloc[-1]['MP'] > 0):
            if (self.npPriceInfo['현재가'] >= self.fSetLow) and (PriceInfo['현재가'] <= self.fSetLow):
                Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_exit, PriceInfo['현재가'])
                df.loc[len(df)-1, 'MP'] = 0
                self.logger.info('ExitLong %s amount ordered', self.amt_exit)
                self.chkPos(-self.amt_exit)
        if (self.nPosition < 0) and (df.iloc[-1]['MP'] < 0):
            if (self.npPriceInfo['현재가'] <= self.fSetHigh) and (PriceInfo['현재가'] >= self.fSetHigh):
                Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_exit, PriceInfo['현재가'])
                df.loc[len(df)-1, 'MP'] = 0
                self.logger.info('ExitShort %s amount ordered', self.amt_exit)
                self.chkPos(self.amt_exit)

        self.npPriceInfo = PriceInfo.copy()


    def lastProc(self):
        if self.isON == False:    # 당일 종가 청산
            if self.nPosition > 0:
                Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'EL', self.amt_exit, 0)
                self.logger.info('ExitLong %s amount ordered', self.amt_exit)
                self.chkPos(-self.amt_exit)
            if self.nPosition < 0:
                Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'ES', self.amt_exit, 0)
                self.logger.info('ExitShort %s amount ordered', self.amt_exit)
                self.chkPos(self.amt_exit)