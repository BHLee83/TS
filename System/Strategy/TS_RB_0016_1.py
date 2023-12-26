# 타임프레임: 5분봉
# - Entry
#   Long: 오전 중 스토캐스틱 값이 상단 레벨을 3번 돌파하면 / Short: 오전 중 스토캐스틱 값이 하단 레벨을 3번 이탈하면
# - Exit
#   당일종가청산

from System.strategy import Strategy

import pandas as pd
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
        self.nP1 = 10
        self.nP2 = 40
        self.fOBL = 70
        self.fOSL = 30


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
        
        # Fast %K = ((현재가 - n기간 중 최저가) / (n기간 중 최고가 - n기간 중 최저가)) * 100
        fastK = ((df['종가'] - df['저가'].rolling(self.nP1).min()) / (df['고가'].rolling(self.nP1).max() - df['저가'].rolling(self.nP1).min())) * 100
        # Slow %K = Fast %K의 m기간 이동평균(SMA)
        df['slowK'] = fastK.rolling(window=self.nP2).mean()
        
        df['MP'] = 0
        dfSignal = pd.DataFrame(None, columns=df.columns)
        for i in df.index-1:
            if i < (self.nP1 + self.nP2):
                continue

            df.loc[i, 'MP'] = df['MP'][i-1]
            t = int(str(df['시간'][i])[:4])
            if (t >= 905) and (t < 1205):   # Entry
                if min(df['slowK'][i-3+1:i+1]) > self.fOBL:
                    if df['MP'][i] != -1:
                        df.loc[i, 'MP'] = 1
                if max(df['slowK'][i-3+1:i+1]) < self.fOSL:
                    if df['MP'][i] != 1:
                        df.loc[i, 'MP'] = -1
            elif t == 1545:
                df.loc[i, 'MP'] = 0

            if df['MP'][i] != df['MP'][i-1]:
                dfSignal.loc[len(dfSignal)] = df.loc[i, :]


    # 전략 실행
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행
            self.common()
            self.chkPos()
            return
        
        if (self.npPriceInfo == None) or (self.npPriceInfo['시가'] == 0):    # 첫 데이터 수신시
            self.npPriceInfo = PriceInfo.copy()

        # 분봉 업데이트 시
        if (str(self.npPriceInfo['체결시간'])[4:6] != str(PriceInfo['체결시간'])[4:6]) and \
        (int(str(PriceInfo['체결시간'])[4:6]) % int(self.lstTimeIntrvl[self.ix]) == 0):
            if int(str(PriceInfo['체결시간'])[2:4]) < 12: # Entry
                self.common()
                # self.lstData[self.ix].loc[len(self.lstData[self.ix])-1, 'MP'] = self.lstData[self.ix].iloc[-2]['MP']
                
                df = self.lstData[self.ix]
                if df.iloc[-3]['MP'] != df.iloc[-2]['MP']:  # 포지션 변동시
                    if self.nPosition == 0:
                        if df.iloc[-2]['MP'] == 1:
                            Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 상품코드, 매수/매도, 계약수, 가격
                            self.logger.info('Buy %s amount ordered', self.amt_entry)
                            self.chkPos(self.amt_entry)
                        if df.iloc[-2]['MP'] == -1:
                            Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])
                            self.logger.info('Sell %s amount ordered', self.amt_entry)
                            self.chkPos(-self.amt_entry)

        self.npPriceInfo = PriceInfo.copy()


    def lastProc(self):
        if self.nPosition > 0:  # 포지션 청산
            Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'EL', self.amt_exit, 0)
            self.logger.info('ExitLong %s amount ordered', self.amt_exit)
            self.chkPos(-self.amt_exit)
        if self.nPosition < 0:
            Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'ES', self.amt_exit, 0)
            self.logger.info('ExitShort %s amount ordered', self.amt_exit)
            self.chkPos(self.amt_exit)