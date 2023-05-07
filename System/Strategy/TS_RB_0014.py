# 타임프레임: 5분봉
# - Entry
#   Long: 12시 이전에 시초가에서 5ATR만큼 상승 / Short: 12시 이전에 시초가에서 5ATR만큼 하락
# - Exit
#   당일종가청산

from System.strategy import Strategy

import pandas as pd
import datetime as dt
import logging



class TS_RB_0014():
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
        self.isON = bool(int(self.dfInfo['OVERNIGHT']))
        self.lstTrUnit = list(map(int, self.dfInfo['TR_UNIT'].split(',')))
        self.fWeight = self.dfInfo['WEIGHT']

        self.lstProductNCode = list(map(lambda x: 'KRDRVFU'+x, self.lstUnderId))    # for SHi-indi spec. 연결선물 코드
        self.lstProductCode = Strategy.setProductCode(self.lstUnderId)
        self.lstTimeFrame_tmp = Strategy.setTimeFrame(self.lstTimeFrame)  # for SHi-indi spec.
        self.lstTimeWnd = self.lstTimeFrame_tmp[0]
        self.lstTimeIntrvl = self.lstTimeFrame_tmp[1]
        self.ix = 0 # 대상 상품의 인덱스
        self.nPosition = 0
        self.amt = 0

        # Local setting variables
        self.lstData = [pd.DataFrame(None)] * len(self.lstAssetCode)
        self.nMult = 5
        self.nTrailBar = 30
        self.fDayOpen = 0.0


    # 과거 데이터 생성 (인디로 수신시 일봉은 연결선물, 분봉은 근월물 코드로 생성)
    def createHistData(self, instInterface):
        # for i, v in enumerate(self.lstProductNCode):
        for i, v in enumerate(self.lstProductCode):
            data = Strategy.getHistData(v, self.lstTimeFrame[i])
            if type(data) == bool:
                if data == False:
                    instInterface.price.rqHistData(v, self.lstTimeWnd[i], self.lstTimeIntrvl[i], Strategy.strStartDate, Strategy.strEndDate, Strategy.strRqCnt)
                    instInterface.event_loop.exec_()


    # 과거 데이터 로드
    def getHistData(self):
        data = Strategy.getHistData(self.lstProductCode[self.ix], self.lstTimeFrame[self.ix])
        if type(data) == bool:
            if data == False:
                return pd.DataFrame(None)
        
        return data


    # 전략 적용
    def applyChart(self):   # Strategy apply on historical chart
        today = dt.datetime.today().strftime('%Y%m%d')  # 전일자 동시호가 데이터 제거
        t = 1535+int(self.lstTimeFrame[self.ix])
        for i in self.lstData[self.ix].index:
            if self.lstData[self.ix]['일자'][i] != today:
                if self.lstData[self.ix]['시간'][i] != '1545':
                    if int(self.lstData[self.ix]['시간'][i]) >= t:
                        self.lstData[self.ix] = self.lstData[self.ix].drop(i)
                    else:
                        break
        self.lstData[self.ix] = self.lstData[self.ix].reset_index()
        self.lstData[self.ix] = self.lstData[self.ix].drop('index', axis=1)

        df = self.lstData[self.ix].sort_index(ascending=False).reset_index()
        df['Range'] = (df['고가']-df['저가']).rolling(window=30).mean()
        df['MP'] = 0
        for i in df.index:
            if i > 30:
                df.loc[i, 'MP'] = df['MP'][i-1]
                if df['일자'][i] != df['일자'][i-1]:
                    self.fDayOpen = df['시가'][i]

                if self.fDayOpen == 0:
                    continue

                chUp = self.fDayOpen + df['Range'][i] * self.nMult
                chDn = self.fDayOpen - df['Range'][i] * self.nMult
                t = int(df['시간'][i])
                if t < 1205:   # Entry
                    if df['MP'][i] != 1:
                        if df['고가'][i] >= chUp:
                            df.loc[i, 'MP'] = 1
                    if df['MP'][i] != -1:
                        if df['저가'][i] <= chDn:
                            df.loc[i, 'MP'] = -1
                                
                if i > self.nTrailBar:  # Trailing Stops
                    if df['MP'][i] == 1:
                        if df['저가'][i] <= min(df['저가'][i-self.nTrailBar:i-1]):
                            df.loc[i, 'MP'] = 0
                    if df['MP'][i] == -1:
                        if df['고가'][i] >= max(df['고가'][i-self.nTrailBar:i-1]):
                            df.loc[i, 'MP'] = 0

                if t == 1545:   # End of day exit
                    df.loc[i, 'MP'] = 0

        df = df.sort_index(ascending=False).reset_index()
        self.lstData[self.ix]['MP'] = df['MP']
        self.lstData[self.ix]['Range'] = df['Range']


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
            if self.npPriceInfo == None:    # 첫 데이터 수신시
                self.fDayOpen = PriceInfo['시가']
            else:
                if (str(self.npPriceInfo['체결시간'])[4:6] != str(PriceInfo['체결시간'])[4:6]) and \
                    (int(str(PriceInfo['체결시간'])[4:6]) % int(self.lstTimeIntrvl[self.ix]) == 0):    # 분봉 업데이트 시
                    self.lstData[self.ix] = self.getHistData()  # 과거 데이터 수신
                    if self.lstData[self.ix].empty:
                        self.logger.warning('과거 데이터 로드 실패. 전략이 실행되지 않습니다.')
                        return False
                    else:
                        self.applyChart()   # 전략 적용

                self.nPosition = Strategy.getPosition(self.dfInfo['NAME'], self.lstAssetCode[self.ix], self.lstAssetType[self.ix])    # 포지션 확인 및 수량 지정
                self.amt_entry = abs(self.nPosition) + self.lstTrUnit[self.ix] * self.fWeight
                self.amt_exit = abs(self.nPosition)

                df = self.lstData[self.ix]
                if int(str(PriceInfo['체결시간'])[2:4]) < 12: # Entry
                    chUp = self.fDayOpen + df['Range'][0] * self.nMult  # FIXME: Range에 현재가격(PriceInfo) 반영되게 수정해야할 듯. 데이터 확인 필요
                    chDn = self.fDayOpen - df['Range'][0] * self.nMult
                    if df['MP'][0] != 1:
                        if (self.npPriceInfo['현재가'] < chUp) and (PriceInfo['현재가'] >= chUp):
                            Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 상품코드, 매수/매도, 계약수, 가격
                            df.loc[0, 'MP'] = 1
                            self.logger.info('Buy %s amount ordered', self.amt_entry)
                    if df['MP'][0] != -1:
                        if (self.npPriceInfo['현재가'] > chDn) and (PriceInfo['현재가'] <= chDn):
                            Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])
                            df.loc[0, 'MP'] = -1
                            self.logger.info('Sell %s amount ordered', self.amt_entry)

                if df['MP'][0] == 1:    # Trailing Stops
                    low = min(df['저가'][1:self.nTrailBar+1])
                    if (self.npPriceInfo['현재가'] > low) and (PriceInfo['현재가'] <= low):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt_exit, PriceInfo['현재가'])
                        df.loc[0, 'MP'] = 0
                        self.logger.info('ExitLong %s amount ordered', self.amt_exit)
                if df['MP'][0] == -1:
                    high = max(df['고가'][1:self.nTrailBar+1])
                    if (self.npPriceInfo['현재가'] < high) and (PriceInfo['현재가'] >= high):
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt_exit, PriceInfo['현재가'])
                        df.loc[0, 'MP'] = 0
                        self.logger.info('ExitShort %s amount ordered', self.amt_exit)

            self.npPriceInfo = PriceInfo.copy()


    def lastProc(self):
        if self.isON == False:    # 당일 종가 청산
            df = self.lstData[self.ix]
            if df['MP'][0] == 1:
                Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt_exit, 0)
                self.logger.info('ExitLong %s amount ordered', self.amt_exit)
            if df['MP'][0] == -1:
                Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt_exit, 0)
                self.logger.info('ExitShort %s amount ordered', self.amt_exit)