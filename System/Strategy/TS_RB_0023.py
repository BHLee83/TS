# - Timeframe: 일봉
# - Entry
#   Long: Disp봉 전의 볼린저 밴드 상단 돌파시 / Short: Disp봉 전의 볼린저 밴드 하단 이탈시
# - Exit: 없음

from System.strategy import Strategy

import pandas as pd
import logging



class TS_RB_0023():
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
        self.nAvgLen = 3
        self.nDisp = 16
        self.nSDLen = 12
        self.nSDev = 2


    # 공통 프로세스
    def common(self):
        # Data load & apply
        self.lstData[self.ix] = Strategy.getHistData(self.lstProductCode[self.ix], self.lstTimeFrame[self.ix], self.nAvgLen+self.nDisp)
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
        AvgVal = df['종가'].rolling(window=self.nAvgLen).mean()
        SDmult = df['종가'].rolling(window=self.nSDLen).std() * self.nSDev
        DispTop = AvgVal.shift(self.nDisp) + SDmult
        DispBottom = AvgVal.shift(self.nDisp) - SDmult
        df.insert(len(df.columns), "DispTop", DispTop)
        df.insert(len(df.columns), "DispBottom", DispBottom)
        df['MP'] = 0
        for i in df.index-1:
            if i < self.nDisp:
                continue

            df.loc[i, 'MP'] = df['MP'][i-1]
            if df['고가'][i] >= df['DispTop'][i-1]:
                df.loc[i, 'MP'] = 1
                
            if df['저가'][i] <= df['DispBottom'][i-1]:
                df.loc[i, 'MP'] = -1


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
            if PriceInfo['현재가'] == PriceInfo['시가']:    # 시초가인 경우
                self.npPriceInfo['체결시간'] = df.iloc[-2]['시간']  # 전봉 정보 세팅
                self.npPriceInfo['시가'] = df.iloc[-2]['시가']
                self.npPriceInfo['고가'] = df.iloc[-2]['고가']
                self.npPriceInfo['저가'] = df.iloc[-2]['저가']
                self.npPriceInfo['현재가'] = df.iloc[-2]['종가']


        # Entry
        if (df.iloc[-1]['MP'] != 1) and (self.nPosition <= 0):
            if (self.npPriceInfo['현재가'] <= df.iloc[-2]['DispTop']) and (PriceInfo['현재가'] >= df.iloc[-2]['DispTop']):
                Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 상품코드, 매수/매도, 계약수, 가격
                df.loc[len(df)-1, 'MP'] = 1
                self.logger.info('Buy %s amount ordered', self.amt_entry)
                self.chkPos(self.amt_entry)
        if (df.iloc[-1]['MP'] != -1) and (self.nPosition >= 0):
            if (self.npPriceInfo['현재가'] >= df.iloc[-2]['DispBottom']) and (PriceInfo['현재가'] <= df.iloc[-2]['DispBottom']):
                Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])
                df.loc[len(df)-1, 'MP'] = -1
                self.logger.info('Sell %s amount ordered', self.amt_entry)
                self.chkPos(-self.amt_entry)

        self.npPriceInfo = PriceInfo.copy()