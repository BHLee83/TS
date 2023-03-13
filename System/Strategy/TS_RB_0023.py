# - Timeframe: 일봉
# - Entry
#   Long: Disp봉 전의 볼린저 밴드 상단 돌파시 / Short: Disp봉 전의 볼린저 밴드 하단 이탈시
# - Exit: 없음

from System.strategy import Strategy

import pandas as pd
import datetime as dt
import logging



class TS_RB_0023():
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
        self.nAvgLen = 3
        self.nDisp = 16
        self.nSDLen = 12
        self.nSDev = 2


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
        
        AvgVal = df['종가'].rolling(window=self.nAvgLen).mean()
        SDmult = df['종가'].rolling(window=self.nSDLen).std() * self.nSDev
        DispTop = AvgVal.shift(self.nDisp) + SDmult
        DispBottom = AvgVal.shift(self.nDisp) - SDmult
        df.insert(len(df.columns), "DispTop", DispTop)
        df.insert(len(df.columns), "DispBottom", DispBottom)
        df['MP'] = 0
        for i in df.index:
            if i < self.nDisp:
                continue
            df.loc[i, 'MP'] = df['MP'][i-1]
            if df['고가'][i] >= df['DispTop'][i]:
                df.loc[i, 'MP'] = 1
                
            if df['저가'][i] <= df['DispBottom'][i]:
                df.loc[i, 'MP'] = -1
                            
        df = df.sort_index(ascending=False).reset_index()
        self.lstData[self.ix]['MP'] = df['MP']
        self.lstData[self.ix]['DispTop'] = df['DispTop']
        self.lstData[self.ix]['DispBottom'] = df['DispBottom']


    # 전략 실행
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행
            self.lstData[self.ix] = self.getHistData()  # 과거 데이터 수신
            if self.lstData[self.ix].empty:
                self.logger.warning('과거 데이터 로드 실패. 전략이 실행되지 않습니다.')
                return False
            else:
                self.applyChart()   # 전략 적용
        else:
            self.nPosition = Strategy.getPosition(self.dfInfo['NAME'], self.lstAssetCode[self.ix], self.lstAssetType[self.ix])    # 포지션 확인 및 수량 지정
            self.amt_entry = abs(self.nPosition) + self.lstTrUnit[self.ix] * self.fWeight
            self.amt_exit = abs(self.nPosition)

            df = self.lstData[self.ix]
            if self.npPriceInfo == None:    # 첫 데이터 수신시
                if df['MP'][0] != 1:
                    if (df['종가'][0] < df['DispTop'][0]) and (PriceInfo['현재가'] >= df['DispTop'][0]):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 상품코드, 매수/매도, 계약수, 가격
                        df.loc[0, 'MP'] = 1
                        self.logger.info('Buy %s amount ordered', self.amt_entry)
                if df['MP'][0] != -1:
                    if (df['종가'][0] > df['DispBottom'][0]) and (PriceInfo['현재가'] <= df['DispBottom'][0]):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])
                        df.loc[0, 'MP'] = -1
                        self.logger.info('Sell %s amount ordered', self.amt_entry)
            else:
                if df['MP'][0] != 1:
                    if (self.npPriceInfo['현재가'] < df['DispTop'][0]) and (PriceInfo['현재가'] >= df['DispTop'][0]):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 상품코드, 매수/매도, 계약수, 가격
                        df.loc[0, 'MP'] = 1
                        self.logger.info('Buy %s amount ordered', self.amt_entry)
                if df['MP'][0] != -1:
                    if (self.npPriceInfo['현재가'] > df['DispBottom'][0]) and (PriceInfo['현재가'] <= df['DispBottom'][0]):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])
                        df.loc[0, 'MP'] = -1
                        self.logger.info('Sell %s amount ordered', self.amt_entry)

            self.npPriceInfo = PriceInfo.copy()