# 타임프레임: 5분봉
# - Entry
#   Long: 오전 중 스토캐스틱 값이 상단 레벨을 3번 돌파하면 / Short: 오전 중 스토캐스틱 값이 하단 레벨을 3번 이탈하면
# - Exit
#   당일종가청산

from System.strategy import Strategy

import pandas as pd
import datetime as dt
import logging



class TS_RB_0016_1():
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
        self.amt = 0

        # Local setting variables
        self.lstData = [pd.DataFrame(None)] * len(self.lstAssetCode)
        self.nP1 = 10
        self.nP2 = 40
        self.fOBL = 70
        self.fOSL = 30


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
        
        data = Strategy.convertNPtoDF(data)
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
        # Fast %K = ((현재가 - n기간 중 최저가) / (n기간 중 최고가 - n기간 중 최저가)) * 100
        fastK = ((df['종가'] - df['저가'].rolling(self.nP1).min()) / (df['고가'].rolling(self.nP1).max() - df['저가'].rolling(self.nP1).min())) * 100
        # Slow %K = Fast %K의 m기간 이동평균(SMA)
        df['slowK'] = fastK.rolling(window=self.nP2).mean()
        df['MP'] = 0
        for i in df.index:
            if i > self.nP2:
                df.loc[i, 'MP'] = df['MP'][i-1]
                t = int(df['시간'][i])
                if t == 1545:
                    df.loc[i, 'MP'] = 0
                if (t >= 905) and (t < 1205):   # Entry
                    if self.fOBL < min(df['slowK'][i-3+1:i+1]):
                        if df['MP'][i] != -1:
                            df.loc[i, 'MP'] = 1
                    if self.fOSL > max(df['slowK'][i-3+1:i+1]):
                        if df['MP'][i] != 1:
                            df.loc[i, 'MP'] = -1
                            
        df = df.sort_index(ascending=False).reset_index()
        self.lstData[self.ix]['MP'] = df['MP']


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
                # Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', 1, 0)    # test
                pass
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
                        if (df['MP'][1] == 0) and (df['MP'][0] == 1):
                            Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 상품코드, 매수/매도, 계약수, 가격
                            df.loc[0, 'MP'] = 1
                            self.logger.info('Buy %s amount ordered', self.amt_entry)
                        if (df['MP'][1] == 0) and (df['MP'][0] == -1):
                            Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])
                            df.loc[0, 'MP'] = -1
                            self.logger.info('Sell %s amount ordered', self.amt_entry)

            self.npPriceInfo = PriceInfo.copy()


    def lastProc(self):
        if self.boolON == False:    # 당일 종가 청산
            df = self.lstData[self.ix]
            if df['MP'][0] == 1:
                Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt_exit, 0)
                self.logger.info('ExitLong %s amount ordered', self.amt_exit)
            if df['MP'][0] == -1:
                Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt_exit, 0)
                self.logger.info('ExitShort %s amount ordered', self.amt_exit)