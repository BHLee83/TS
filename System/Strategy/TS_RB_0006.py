from System.strategy import Strategy

import pandas as pd
import logging



class TS_RB_0006():
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
        self.nWeek = 4


    # 공통 프로세스
    def common(self):
        # Data load & apply
        self.lstData[self.ix] = Strategy.getHistData(self.lstProductCode[self.ix], self.lstTimeFrame[self.ix], self.nWeek*7)
        if self.lstData[self.ix].empty:
            self.logger.warning('과거 데이터 로드 실패. 전략이 실행되지 않습니다.')
            return False
        self.applyChart()   # 전략 적용


    # Position check & amount setup
    def chkPos(self):
        self.nPosition = Strategy.getPosition(self.strName, self.lstAssetCode[self.ix], self.lstAssetType[self.ix])    # 포지션 확인 및 수량 지정
        self.amt_entry = abs(self.nPosition) + self.lstTrUnit[self.ix] * self.fWeight
        self.amt_exit = abs(self.nPosition)
        
        
    # 전략 적용
    def applyChart(self):   # Strategy apply on historical chart
        df = self.lstData[self.ix]
        df['dt'] = pd.to_datetime(df['일자'])
        df['woy'] = list(map(lambda x: x.weekofyear, df['dt']))
        df['MP'] = 0
        df['chUpper'] = 0.0
        df['chLower'] = 0.0
        for i in df.index:
            if i < (self.nWeek + 1) * 5:
                continue

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
            
            if i == df.last_valid_index:
                continue

            if df['고가'][i] >= df['chUpper'][i]:   # 채널 상단 돌파 매수
                df.loc[i, 'MP'] = 1
            if df['저가'][i] <= df['chLower'][i]:   # 채널 하단 돌파 매도
                df.loc[i, 'MP'] = -1


    # 전략
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행시
            return self.common()
            
        df = self.lstData[self.ix]
        if self.npPriceInfo == None:    # 첫 데이터 수신시
            self.npPriceInfo = PriceInfo.copy()
            if PriceInfo['현재가'] == PriceInfo['시가']:    # 시초가인 경우
                self.npPriceInfo['체결시간'] = df.iloc[-2]['시간']  # 전봉 정보 세팅
                self.npPriceInfo['시가'] = df.iloc[-2]['시가']
                self.npPriceInfo['고가'] = df.iloc[-2]['고가']
                self.npPriceInfo['저가'] = df.iloc[-2]['저가']
                self.npPriceInfo['현재가'] = df.iloc[-2]['종가']

        self.chkPos()

        # Entry
        if df.iloc[-1]['MP'] != 1:
            if (self.npPriceInfo['현재가'] <= df.iloc[-1]['chUpper']) and (PriceInfo['현재가'] >= df.iloc[-1]['chUpper']): # 채널 상단 터치시
                Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 매수
                df.loc[len(df)-1, 'MP'] = 1
                self.logger.info('Buy %s amount ordered', self.amt_entry)
        if df.iloc[-1]['MP'] != -1:
            if (self.npPriceInfo['현재가'] >= df.iloc[-1]['chLower']) and (PriceInfo['현재가'] <= df.iloc[-1]['chLower']): # 채널 하단 터치시
                Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])   # 매도
                df.loc[len(df)-1, 'MP'] = -1
                self.logger.info('Sell %s amount ordered', self.amt_entry)
                
        self.npPriceInfo = PriceInfo.copy()