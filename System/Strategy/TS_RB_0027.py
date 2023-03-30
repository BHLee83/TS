# 타임프레임: 5분봉
# - Entry
#   Long: CCI 이평이 -100보다 크고 EntryBar봉 이내에 -100을 골든 크로스한 적 있을 때, HiLoLen 기간중 고점+1 에서
#   Short: CCI 이평이 100보다 작고 EntryBar봉 이내에 100을 데드 크로스한 적 있을 때, HiLoLen 기간중 저점-1 에서
# - Exit
#   EL(ES): 진입후 고(저)점을 높일(낮출) 때마다 관찰 봉수 줄여가며 해당 관찰 봉 이내의 저(고)점-1(+1)에서 추적 청산
# 수수료: 0.006%
# 슬리피지: 0.05pt

from System.strategy import Strategy
from System.indicator import Indicator

import pandas as pd
import logging



class TS_RB_0027():
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
        self.nCCILen = 130
        self.nAvgLen = 80
        self.nHiLoLen = 30
        self.nEntryBar = 12
        self.nTrailStop = 0
        
        self.fBuyPrice = 0.0
        self.fSellPrice = 0.0
        self.fStopPrice = 0.0
        self.nLoc = 0


    # 공통 프로세스
    def common(self):
        # Data load & apply
        self.lstData[self.ix] = Strategy.getHistData(self.lstProductCode[self.ix], self.lstTimeFrame[self.ix], self.nCCILen+self.nAvgLen)
        if self.lstData[self.ix].empty:
            self.logger.warning('과거 데이터 로드 실패. 전략이 실행되지 않습니다.')
            return False
        self.applyChart()   # 전략 적용


    def chkPos(self):
        # Position check & amount setup
        self.nPosition = Strategy.getPosition(self.strName, self.lstAssetCode[self.ix], self.lstAssetType[self.ix])    # 포지션 확인 및 수량 지정
        self.amt_entry = abs(self.nPosition) + self.lstTrUnit[self.ix] * self.fWeight
        self.amt_exit = abs(self.nPosition)


    # 전략 적용
    def applyChart(self):   # Strategy apply on historical chart
        df = self.lstData[self.ix]

        cci = Indicator.CCI(df['고가'], df['저가'], df['종가'], self.nCCILen)
        df['AvgCCI'] = cci.rolling(window=self.nAvgLen).mean()

        df['MP'] = 0
        df['EntryLv'] = 0
        df['ExitLv'] = 0
        df['BuySetup'] = 0
        df['SellSetup'] = 0
        dfSignal = pd.DataFrame(None, columns=df.columns)
        for i in df.index:
            if i < self.nCCILen:
                continue

            df.loc[i, 'MP'] = df['MP'][i-1]
            df.loc[i, 'EntryLv'] = df['EntryLv'][i-1]
            df.loc[i, 'ExitLv'] = df['ExitLv'][i-1]
            
            # Setup
            if (df['AvgCCI'][i-1] <= -100) and (df['AvgCCI'][i] > -100):
                df.loc[i, 'BuySetup'] = 1
            if (df['AvgCCI'][i-1] >= 100) and (df['AvgCCI'][i] < 100):
                df.loc[i, 'SellSetup'] = 1

            if df['시간'][i].startswith('1545'):
                continue

            # Entry
            if self.fBuyPrice != 0.0:
                if df['MP'][i] != 1 and df['고가'][i] >= self.fBuyPrice:    # execution
                    df.loc[i, 'MP'] = 1
                    self.nLoc = i
                    if df['시가'][i] > self.fBuyPrice:
                        df.loc[i, 'EntryLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'EntryLv'] = self.fBuyPrice
                    self.fBuyPrice = 0.0
            if self.fSellPrice != 0.0:
                if df['MP'][i] != -1 and df['저가'][i] <= self.fSellPrice:
                    df.loc[i, 'MP'] = -1
                    self.nLoc = i
                    if df['시가'][i] < self.fSellPrice:
                        df.loc[i, 'EntryLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'EntryLv'] = self.fSellPrice
                    self.fSellPrice = 0.0

            if (df['AvgCCI'][i] > -100) and (df['BuySetup'][i-self.nEntryBar+1:i+1].sum() > 0):    # setup
                self.fBuyPrice = max(df['고가'][i-self.nHiLoLen:i]) + 1
            if (df['AvgCCI'][i] < 100) and (df['SellSetup'][i-self.nEntryBar+1:i+1].sum() > 0):
                self.fSellPrice = min(df['저가'][i-self.nHiLoLen:i]) - 1
                
            # Exit
            if self.fStopPrice != 0.0:    # execution
                if df['MP'][i-1] == 1 and df['저가'][i] <= self.fStopPrice:
                    if df['시가'][i] < self.fStopPrice:
                        df.loc[i, 'ExitLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'ExitLv'] = self.fStopPrice
                    df.loc[i, 'MP'] = 0
                    self.fStopPrice = 0.0
                if df['MP'][i-1] == -1 and df['고가'][i] >= self.fStopPrice:
                    if df['시가'][i] > self.fStopPrice:
                        df.loc[i, 'ExitLv'] = df['시가'][i]
                    else:
                        df.loc[i, 'ExitLv'] = self.fStopPrice
                    df.loc[i, 'MP'] = 0
                    self.fStopPrice = 0.0

            if df['MP'][i] == 1 and df['고가'][i] > max(df['고가'][i-self.nHiLoLen:i]):    # setup
                if self.nTrailStop > 1:
                    self.nTrailStop -= 1
            if df['MP'][i] == -1 and df['저가'][i] < min(df['저가'][i-self.nHiLoLen:i]):
                if self.nTrailStop > 1:
                    self.nTrailStop -= 1
            if i == self.nLoc:
                self.nTrailStop = self.nHiLoLen
            if df['MP'][i] == 1:
                self.fStopPrice = min(df['저가'][i-self.nTrailStop+1:i+1]) - 1
            if df['MP'][i] == -1:
                self.fStopPrice = max(df['고가'][i-self.nTrailStop+1:i+1]) + 1

            # Position check
            if df['MP'][i] != df['MP'][i-1]:
                dfSignal.loc[len(dfSignal)] = df.loc[i, :]


    # 전략 실행
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행시
            return self.common()
        
        if self.npPriceInfo == None:    # 첫 데이터 수신시
            self.npPriceInfo = PriceInfo.copy()

        # 분봉 업데이트 시
        if (str(self.npPriceInfo['체결시간'])[4:6] != str(PriceInfo['체결시간'])[4:6]) and \
        (int(str(PriceInfo['체결시간'])[4:6]) % int(self.lstTimeIntrvl[self.ix]) == 0):
            self.common()
            
        self.chkPos()
        df = self.lstData[self.ix]

        # Entry
        if self.fBuyPrice != 0.0:
            if (self.nPosition <= 0) and (df.iloc[-1]['MP'] != 1):
                if self.npPriceInfo['현재가'] < self.fBuyPrice and PriceInfo['현재가'] >= self.fBuyPrice:
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] = 1
                    self.fBuyPrice = 0.0
                    self.logger.info('Buy %s amount ordered', self.amt_entry)
        if self.fSellPrice != 0.0:
            if (self.nPosition >= 0) and (df.iloc[-1]['MP'] != -1):
                if self.npPriceInfo['현재가'] > self.fSellPrice and PriceInfo['현재가'] <= self.fSellPrice:
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] = -1
                    self.fSellPrice = 0.0
                    self.logger.info('Sell %s amount ordered', self.amt_entry)

        # Exit
        if self.fStopPrice != 0.0:
            if (self.nPosition > 0) and (df.iloc[-1]['MP'] == 1):
                if self.npPriceInfo['현재가'] > self.fStopPrice and PriceInfo['현재가'] <= self.fStopPrice:
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'EL', self.amt_exit, PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] = 0
                    self.fStopPrice = 0.0
                    self.logger.info('ExitLong %s amount ordered', self.amt_exit)
            if (self.nPosition < 0) and (df.iloc[-1]['MP'] == -1):
                if self.npPriceInfo['현재가'] < self.fStopPrice and PriceInfo['현재가'] >= self.fStopPrice:
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'ES', self.amt_exit, PriceInfo['현재가'])
                    df.loc[len(df)-1, 'MP'] = 0
                    self.fStopPrice = 0.0
                    self.logger.info('ExitShort %s amount ordered', self.amt_exit)

        self.npPriceInfo = PriceInfo.copy()