import pandas as pd

from System.strategy import Strategy
import logging



class TS_RB_0007():
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
        self.nWeek_entry = 4    # nWeek_entry >= nWeek_exit. otherwise, have to remove 'break' at for j in range(i, 1, -1):
        self.nWeek_exit = 2
        self.isFirstTry = True
        self.fEntryPrice = 0.0
        self.fPL = 0.0
        self.fBuyP = 0.0
        self.fSellP = 0.0

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
        df['MP_v'] = 0
        df['chUpper_entry'] = 99999.9
        df['chLower_entry'] = 0.0
        df['chUpper_exit'] = 99999.9
        df['chLower_exit'] = 0.0
        for i in df.index:
            if i >= (self.nWeek_entry + 1) * 5:
                df.loc[i, 'MP'] = df['MP'][i-1]
                df.loc[i, 'MP_v'] = df['MP_v'][i-1]
                df.loc[i, 'chUpper_entry'] = df['chUpper_entry'][i-1]
                df.loc[i, 'chLower_entry'] = df['chLower_entry'][i-1]
                df.loc[i, 'chUpper_exit'] = df['chUpper_exit'][i-1]
                df.loc[i, 'chLower_exit'] = df['chLower_exit'][i-1]
                
                if df['woy'][i] != df['woy'][i-1]:  # nWeek 주 가격 탐색
                    cnt = 0
                    for j in range(i, 1, -1):
                        if df['woy'][j] != df['woy'][j-1]:
                            cnt += 1
                            if cnt == 1:
                                ixEnd = j
                            if cnt == self.nWeek_exit+1:
                                ixStart_exit = j
                            if cnt == self.nWeek_entry+1:
                                ixStart_entry = j
                                break
                    df.loc[i, 'chUpper_entry'] = df['고가'][ixStart_entry:ixEnd].max()  # 진입용 고/저 채널
                    df.loc[i, 'chLower_entry'] = df['저가'][ixStart_entry:ixEnd].min()
                    df.loc[i, 'chUpper_exit'] = df['고가'][ixStart_exit:ixEnd].max()    # 청산용 고/저 채널
                    df.loc[i, 'chLower_exit'] = df['저가'][ixStart_exit:ixEnd].min()

                if self.isFirstTry:
                    if df['고가'][i] >= df['chUpper_entry'][i]:   # nWeek_entry 채널 상단 돌파 매수
                        df.loc[i, 'MP'] = 1
                        self.isFirstTry = False
                    if df['저가'][i] <= df['chLower_entry'][i]:   # nWeek_entry 채널 하단 돌파 매도
                        df.loc[i, 'MP'] = -1
                        self.isFirstTry = False
                else:
                    # virtual transactions
                    if df['고가'][i] >= df['chUpper_entry'][i-1]:
                        df.loc[i, 'MP_v'] = 1
                    if df['저가'][i] <= df['chLower_entry'][i-1]:
                        df.loc[i, 'MP_v'] = -1

                    if (df['MP_v'][i] == 1) and (df['저가'][i] <= df['chLower_exit'][i-1]):
                        df.loc[i, 'MP_v'] = 0
                    if (df['MP_v'][i] == -1) and (df['고가'][i] >= df['chUpper_exit'][i-1]):
                        df.loc[i, 'MP_v'] = 0

                    if (df['MP_v'][i] == 1) and (df['MP_v'][i-1] != 1):
                        self.fBuyP = df['chUpper_entry'][i]
                    if (df['MP_v'][i] == -1) and (df['MP_v'][i-1] != -1):
                        self.fSellP = df['chLower_entry'][i]

                    if (df['MP_v'][i] != 1) and (df['MP_v'][i-1] == 1):
                        self.fPL = df['chLower_exit'][i] - self.fBuyP
                    if (df['MP_v'][i] != -1) and (df['MP_v'][i-1] == -1):
                        self.fPL = self.fSellP - df['chUpper_exit'][i]

                    if self.fPL < 0:
                        if df['MP'][i] != 1:
                            if df['고가'][i] >= df['chUpper_entry'][i]:   # nWeek_entry 채널 상단 돌파 매수
                                # self.fEntryPrice = df['chUpper_entry'][i]
                                df.loc[i, 'MP'] = 1
                        if df['MP'][i] != -1:
                            if df['저가'][i] <= df['chLower_entry'][i]:   # nWeek_entry 채널 하단 돌파 매도
                                # self.fEntryPrice = df['chLower_entry'][i]
                                df.loc[i, 'MP'] = -1

                    if df['MP'][i] == 1:
                        if df['저가'][i] <= df['chLower_exit'][i]:   # nWeek_exit 채널 하단 돌파시 청산
                            # self.fPL = df['MP'][i] * (df['chLower_exit'][i] - self.fEntryPrice)
                            df.loc[i, 'MP'] = 0
                    if df['MP'][i] == -1:
                        if df['고가'][i] >= df['chUpper_exit'][i]:   # nWeek_exit 채널 상단 돌파시 청산
                            # self.fPL = df['MP'][i] * (df['chUpper_exit'][i] - self.fEntryPrice)
                            df.loc[i, 'MP'] = 0

        df = df.sort_index(ascending=False).reset_index()
        self.lstData[self.ix]['MP'] = df['MP']
        self.lstData[self.ix]['chUpper_entry'] = df['chUpper_entry']
        self.lstData[self.ix]['chLower_entry'] = df['chLower_entry']
        self.lstData[self.ix]['chUpper_exit'] = df['chUpper_exit']
        self.lstData[self.ix]['chLower_exit'] = df['chLower_exit']


    # 전략
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행인 경우에만
            self.lstData[self.ix] = self.getHistData()
            if self.lstData[self.ix].empty:
                return False
            else:
                self.applyChart()
                if Strategy.dfPosition.empty:   # 포지션 확인 및 수량 지정
                    self.nPosition = 0
                else:
                    try:
                        self.nPosition = Strategy.dfPosition['POSITION'][Strategy.dfPosition['STRATEGY_ID']==__class__.__name__ \
                                            and Strategy.dfPosition['ASSET_NAME']==self.lstAssetCode[self.ix] \
                                            and Strategy.dfPosition['ASSET_TYPE']==self.lstAssetType[self.ix]].values[0]
                    except:
                        self.nPosition = 0
                self.amt_entry = abs(self.nPosition) + self.lstTrUnit[self.ix] * self.fWeight
                self.amt_exit = abs(self.nPosition)
        else:
            if self.npPriceInfo != None:
                df = self.lstData[self.ix]
                if self.isFirstTry:
                    if df['MP'][0] != 1:
                        if (self.npPriceInfo['현재가'] < df['chUpper_entry'][0]) and (PriceInfo['현재가'] >= df['chUpper_entry'][0]): # 채널 상단 터치시
                            Strategy.setOrder(self, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 매수
                            df['MP'][0] = 1
                            self.isFirstTry = False
                            self.logger.info('Buy %s amount ordered', self.amt_entry)
                    if df['MP'][0] != -1:
                        if (self.npPriceInfo['현재가'] > df['chLower_entry'][0]) and (PriceInfo['현재가'] <= df['chLower_entry'][0]): # 채널 하단 터치시
                            Strategy.setOrder(self, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])   # 매도
                            df['MP'][0] = -1
                            self.isFirstTry = False
                            self.logger.info('Sell %s amount ordered', self.amt_entry)
                else:
                    if self.fPL < 0:
                        if df['MP'][0] != 1:
                            if (self.npPriceInfo['현재가'] < df['chUpper_entry'][0]) and (PriceInfo['현재가'] >= df['chUpper_entry'][0]): # nWeek_entry 채널 상단 터치시
                                Strategy.setOrder(self, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 매수
                                df['MP'][0] = 1
                                self.logger.info('Buy %s amount ordered', self.amt_entry)
                        if df['MP'][0] != -1:
                            if (self.npPriceInfo['현재가'] > df['chLower_entry'][0]) and (PriceInfo['현재가'] <= df['chLower_entry'][0]): # nWeek_entry 채널 하단 터치시
                                Strategy.setOrder(self, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])   # 매도
                                df['MP'][0] = -1
                                self.logger.info('Sell %s amount ordered', self.amt_entry)
                    
                    if df['MP'][0] == 1:
                        if (self.npPriceInfo['현재가'] > df['chLower_exit'][0]) and (PriceInfo['현재가'] <= df['chLower_exit'][0]):   # nWeek_exit 채널 하단 터치시
                            Strategy.setOrder(self, self.lstProductCode[self.ix], 'S', self.amt_exit, PriceInfo['현재가'])   # 매수 청산
                            df.loc[0, 'MP'] = 0
                            self.logger.info('ExitLong %s amount ordered', self.amt_exit)
                    if df['MP'][0] == -1:
                        if (self.npPriceInfo['현재가'] < df['chUpper_exit'][0]) and (PriceInfo['현재가'] >= df['chUpper_exit'][0]):   # nWeek_exit 채널 상단 터치시
                            Strategy.setOrder(self, self.lstProductCode[self.ix], 'B', self.amt_exit, PriceInfo['현재가'])   # 매도 청산
                            df.loc[0, 'MP'] = 0
                            self.logger.info('ExitShort %s amount ordered', self.amt_exit)

            self.npPriceInfo = PriceInfo.copy()