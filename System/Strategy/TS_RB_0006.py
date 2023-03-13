import pandas as pd

from System.strategy import Strategy
import logging



class TS_RB_0006():
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
        self.nWeek = 4


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
        df['dt'] = pd.to_datetime(df['일자'])
        df['woy'] = list(map(lambda x: x.weekofyear, df['dt']))
        df['MP'] = 0
        df['chUpper'] = 0.0
        df['chLower'] = 0.0
        for i in df.index:
            if i > (self.nWeek + 1) * 5:
                df.loc[i, 'MP'] = df['MP'][i-1]
                df.loc[i, 'chUpper'] = df['chUpper'][i-1]
                df.loc[i, 'chLower'] = df['chLower'][i-1]
                    
                cnt = 0 # nWeek 주 가격 탐색
                for j in range(i, 1, -1):
                    if df['woy'][j] != df['woy'][j-1]:
                        cnt += 1
                        if cnt == 1:
                            ixEnd = j
                        if cnt == self.nWeek+1:
                            ixStart = j
                            break
                df.loc[i, 'chUpper'] = df['고가'][ixStart:ixEnd].max()  # nWeek 주 고가
                df.loc[i, 'chLower'] = df['저가'][ixStart:ixEnd].min()  # nWeek 주 저가
                if df['고가'][i] >= df['chUpper'][i]:   # 채널 상단 돌파 매수
                    df.loc[i, 'MP'] = 1
                if df['저가'][i] <= df['chLower'][i]:   # 채널 하단 돌파 매도
                    df.loc[i, 'MP'] = -1
        df = df.sort_index(ascending=False).reset_index()
        self.lstData[self.ix]['MP'] = df['MP']
        self.lstData[self.ix]['chUpper'] = df['chUpper']
        self.lstData[self.ix]['chLower'] = df['chLower']


    # 전략
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행인 경우에만
            self.lstData[self.ix] = self.getHistData()
            if self.lstData[self.ix].empty:
                return False
            else:
                self.applyChart()
                self.nPosition = Strategy.getPosition(self.dfInfo['NAME'], self.lstAssetCode[self.ix], self.lstAssetType[self.ix])    # 포지션 확인 및 수량 지정
                self.amt_entry = abs(self.nPosition) + self.lstTrUnit[self.ix] * self.fWeight
        else:
            df = self.lstData[self.ix]
            if self.npPriceInfo != None:
                if df['MP'][0] < 0:
                    if (self.npPriceInfo['현재가'] < df['chUpper'][0]) and (PriceInfo['현재가'] >= df['chUpper'][0]): # 채널 상단 터치시
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 매수
                        df.loc[0, 'MP'] = 1
                        self.logger.info('Buy %s amount ordered', self.amt_entry)
                if df['MP'][0] > 0:
                    if (self.npPriceInfo['현재가'] > df['chLower'][0]) and (PriceInfo['현재가'] <= df['chLower'][0]): # 채널 하단 터치시
                        Strategy.setOrder(self.dfInfo['NAME'], self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])   # 매도
                        df.loc[0, 'MP'] = -1
                        self.logger.info('Sell %s amount ordered', self.amt_entry)
            self.npPriceInfo = PriceInfo.copy()