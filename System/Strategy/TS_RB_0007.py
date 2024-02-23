from System.strategy import Strategy

import pandas as pd
import logging



class TS_RB_0007():
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
        self.nWeek_entry = 4    # nWeek_entry >= nWeek_exit. otherwise, have to remove 'break' at for j in range(i, 1, -1):
        self.nWeek_exit = 2
        self.isFirstTry = True
        self.fEntryPrice = 0.0
        self.fPL = 0.0
        self.fBuyP = 0.0
        self.fSellP = 0.0


    # 공통 프로세스
    def common(self):
        # Data load & apply
        self.lstData[self.ix] = Strategy.getHistData(self.lstProductCode[self.ix], self.lstAssetType[self.ix], self.lstTimeFrame[self.ix], self.nWeek_entry*50)
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
        df['dt'] = pd.to_datetime(df['일자'])
        df['woy'] = list(map(lambda x: x.weekofyear, df['dt']))
        df['MP'] = 0
        df['MP_v'] = 0
        df['chUpper_entry'] = 99999.9
        df['chLower_entry'] = 0.0
        df['chUpper_exit'] = 99999.9
        df['chLower_exit'] = 0.0
        for i in df.index:
            if i < (self.nWeek_entry + 1) * 5:
                continue

            df.loc[i, 'MP'] = df['MP'][i-1]
            df.loc[i, 'MP_v'] = df['MP_v'][i-1]
            df.loc[i, 'chUpper_entry'] = df['chUpper_entry'][i-1]
            df.loc[i, 'chLower_entry'] = df['chLower_entry'][i-1]
            df.loc[i, 'chUpper_exit'] = df['chUpper_exit'][i-1]
            df.loc[i, 'chLower_exit'] = df['chLower_exit'][i-1]
            
            if df['woy'][i] != df['woy'][i-1]:  # nWeek 주 가격 탐색
                cnt = 0
                for j in range(i, 0, -1):
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

            if i == df.last_valid_index():
                continue

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


    # 전략
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:  # 최초 실행인 경우에만
            self.common()
            self.chkPos()
            return
            
        df = self.lstData[self.ix]
        if (self.npPriceInfo == None) or (self.npPriceInfo['시가'] == 0):    # 첫 데이터 수신시
            if self.npPriceInfo == None:
                self.npPriceInfo = PriceInfo.copy()
                self.npPriceInfo['체결시간'] = df.iloc[-2]['시간']  # 전봉 정보 세팅
                self.npPriceInfo['시가'] = df.iloc[-2]['시가']
                self.npPriceInfo['고가'] = df.iloc[-2]['고가']
                self.npPriceInfo['저가'] = df.iloc[-2]['저가']
                self.npPriceInfo['현재가'] = df.iloc[-2]['종가']

        # Entry
        if self.fPL < 0:
            if df.iloc[-1]['MP'] != 1:
                if (self.npPriceInfo['현재가'] <= df.iloc[-1]['chUpper_entry']) and (PriceInfo['현재가'] >= df.iloc[-1]['chUpper_entry']): # nWeek_entry 채널 상단 터치시
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])   # 매수
                    df.loc[len(df)-1, 'MP'] = 1
                    self.logger.info('Buy %s amount ordered', self.amt_entry)
                    self.chkPos(self.amt_entry)
            if df.iloc[-1]['MP'] != -1:
                if (self.npPriceInfo['현재가'] >= df.iloc[-1]['chLower_entry']) and (PriceInfo['현재가'] <= df.iloc[-1]['chLower_entry']): # nWeek_entry 채널 하단 터치시
                    Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])   # 매도
                    df.loc[len(df)-1, 'MP'] = -1
                    self.logger.info('Sell %s amount ordered', self.amt_entry)
                    self.chkPos(-self.amt_entry)
        
        # Exit
        if df.iloc[-1]['MP'] == 1:
            if (self.npPriceInfo['현재가'] >= df.iloc[-1]['chLower_exit']) and (PriceInfo['현재가'] <= df.iloc[-1]['chLower_exit']):   # nWeek_exit 채널 하단 터치시
                Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'EL', self.amt_exit, PriceInfo['현재가'])   # 매수 청산
                df.loc[len(df)-1, 'MP'] = 0
                self.logger.info('ExitLong %s amount ordered', self.amt_exit)
                self.chkPos(-self.amt_entry)
        if df.iloc[-1]['MP'] == -1:
            if (self.npPriceInfo['현재가'] <= df.iloc[-1]['chUpper_exit']) and (PriceInfo['현재가'] >= df.iloc[-1]['chUpper_exit']):   # nWeek_exit 채널 상단 터치시
                Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'ES', self.amt_exit, PriceInfo['현재가'])   # 매도 청산
                df.loc[len(df)-1, 'MP'] = 0
                self.logger.info('ExitShort %s amount ordered', self.amt_exit)
                self.chkPos(self.amt_entry)

        self.npPriceInfo = PriceInfo.copy()