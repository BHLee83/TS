# 타임프레임: 5분봉
# - Setup
#   
# - Entry
#   
# - Exit
#   
# 수수료: 0.0006%
# 슬리피지: 0.05pt

from System.strategy import Strategy

import pandas as pd
import logging


class TS_RB_0044_N():
    def __init__(self, info) -> None:
        self.logger = logging.getLogger(__class__.__name__)  # 로그 생성
        self.logger.info('Init. start')

        # 기본 설정 초기화
        self._initialize(info)

    def _initialize(self, info):
        """ 초기 설정 값들을 처리하는 메서드 """
        # General info
        self.npPriceInfo = None

        # 전략 정보 초기화
        self.strName = info['NAME']
        self.lstAssetCode = info['ASSET_CODE'].split(',') # 거래대상은 여러개일 수 있음
        self.lstAssetType = info['ASSET_TYPE'].split(',')
        self.lstUnderId = info['UNDERLYING_ID'].split(',')
        self.lstTimeFrame = info['TIMEFRAME'].split(',')
        self.isON = bool(int(info['OVERNIGHT']))
        self.isPyramid = bool(int(info['PYRAMID']))
        self.lstTrUnit = list(map(int, info['TR_UNIT'].split(',')))
        self.fWeight = info['WEIGHT']

        # 상품 정보 및 시간 관련 설정
        self.lstProductCode = Strategy.setProductCode(self.lstUnderId)
        # self.lstProductNCode = list(map(lambda x: 'KRDRVFU'+x, self.lstUnderId))    # for SHi-indi spec. 연결선물 코드
        self.lstProductNCode = [f'KRDRVFU{x}' for x in self.lstUnderId]    # for SHi-indi spec. 연결선물 코드
        self.lstTimeFrame_tmp = Strategy.setTimeFrame(self.lstTimeFrame)  # for SHi-indi spec.
        self.lstTimeWnd, self.lstTimeIntrvl = self.lstTimeFrame_tmp
        self.ix = 0 # 대상 상품의 인덱스
        self.nPosition = 0
        self.amt_entry = self.amt_exit = 0

        # 로컬 변수 초기화
        self.lstData = [pd.DataFrame(None)] * len(self.lstAssetCode)
        self._initialize_local_variables()


    def _initialize_local_variables(self):
        """ 로컬 변수 초기화 메서드 """
        self.fR = 0.3
        self.strToday = Strategy.strToday
        self.strT_1 = Strategy.strT_1
        self.nEntryLimit = 153000
        # self.dtEntryLimit = dt.time(15,30)
        self.nDaysSinceEntry = 0
        self.nDayIn = 1

        self.fDayOpen = self.fDayHigh = self.fDayLow = 0.0
        self.fDayRange_t1 = 0.0
        self.fBuyPrice = self.fSellPrice = 0.0
        self.fEntryLv = 0.0
        

    def _dataLoad(self):
        """ 공통 데이터 로드 """
        self.lstData[self.ix] = Strategy.getHistData(
            self.lstProductCode[self.ix],
            self.lstAssetType[self.ix],
            self.lstTimeFrame[self.ix],
            int(400 / int(self.lstTimeIntrvl[self.ix]) * 10)
        )

        if self.lstData[self.ix].empty:
            self.logger.warning('과거 데이터 로드 실패. 전략이 실행되지 않습니다.')
            return False
        return True


    def _chkPos(self, amt=0):
        """ 포지션 확인 및 수량 업데이트 """
        if amt == 0:
            self.nPosition = Strategy.getPosition(self.strName, self.lstAssetCode[self.ix], self.lstAssetType[self.ix])
        else:
            self.nPosition += amt
        self.amt_entry = abs(self.nPosition) + self.lstTrUnit[self.ix] * self.fWeight
        self.amt_exit = abs(self.nPosition)


    def _applyChart(self):
        """ Strategy apply on historical chart """
        df = self.lstData[self.ix]
        MP = 0

        for i in range(0, len(df)-1):
            if i < 80:
                continue

            # Setup
            if df['일자'][i] != df['일자'][i-1]:
                self._setup_variables(df, i)
                # Profitable Open Stop
                MP = self._check_profitable_open_stop(MP, df, i)

            if self.fDayOpen == 0:
                continue

            if df['일자'][i] == df['일자'][i+1]:
                # Entry
                MP = self._check_entry_conditions(MP, df, i)
            else:
                # Exit - end of day
                MP = self._check_exit_conditions(MP, df, i)


    def _setup_variables(self, df, i):
        """ 장시작 변수 설정 """
        self.fDayOpen = df['시가'][i]
        dayHigh_t1 = df[df['일자'] == df['일자'][i-1]]['고가'].max()
        dayLow_t1 = df[df['일자'] == df['일자'][i-1]]['저가'].min()
        self.fDayRange_t1 = dayHigh_t1 - dayLow_t1
        self.fBuyPrice = self.fDayOpen + self.fDayRange_t1 * self.fR
        self.fSellPrice = self.fDayOpen - self.fDayRange_t1 * self.fR
        self.fDayHigh = df[df['일자'] == df['일자'][i]]['고가'].max()
        self.fDayLow = df[df['일자'] == df['일자'][i]]['저가'].min()
                

    def _check_profitable_open_stop(self, MP, df, i):
        """ Profitable Open Stop 조건 확인 및 처리 """
        if MP != 0:
            self.nDaysSinceEntry += 1
            if self.nDaysSinceEntry >= self.nDayIn:
                if (MP > 0) and (self.fEntryLv < self.fDayOpen):
                    MP = 0
                    self.nDaysSinceEntry = 0
                if (MP < 0) and (self.fEntryLv > self.fDayOpen):
                    MP = 0
                    self.nDaysSinceEntry = 0
        return MP
                
            
    def _check_entry_conditions(self, MP, df, i):
        """ 진입 조건 확인 및 처리 """
        if int(df['시간'][i]) <= self.nEntryLimit:
            if MP <= 0:
                if self.fBuyPrice and df['고가'][i] >= self.fBuyPrice:    # execution
                    self.fEntryLv = max(self.fBuyPrice, df['시가'][i])
                    MP = 1
                    self.nDaysSinceEntry = 0
            if MP >= 0:
                if self.fSellPrice and df['저가'][i] <= self.fSellPrice:
                    self.fEntryLv = min(self.fSellPrice, df['시가'][i])
                    MP = -1
                    self.nDaysSinceEntry = 0
        return MP


    def _check_exit_conditions(self, MP, df, i):
        """ 청산 조건 확인 및 처리 """
        if MP == 1:
            if (self.fEntryLv > df['종가'][i-1]) or (self.fDayLow < (self.fDayOpen - self.fDayRange_t1 * self.fR)):
                MP = 0
        if MP == -1:
            if (self.fEntryLv < df['종가'][i-1]) or (self.fDayHigh > (self.fDayOpen + self.fDayRange_t1 * self.fR)):
                MP = 0
        return MP


    def execute(self, PriceInfo):
        """ 전략 실행 """
        if isinstance(PriceInfo, int):  # 최초 실행시
            if not self._dataLoad():
                return  # 데이터 로드 실패 시 종료
            self._chkPos()
            self._applyChart()   # 최초 1회 실행
            return
        
        if self.npPriceInfo is None:
            self.npPriceInfo = PriceInfo.copy()
            return

        if self.npPriceInfo['시가'] == 0:   # 첫 데이터 수신시
            self._initialize_first_data(self.lstData[self.ix], PriceInfo)
            
        
        if int(PriceInfo['체결시간']) < self.nEntryLimit:
            self._handle_entry_exit(PriceInfo['현재가'])
        
        self.npPriceInfo = PriceInfo.copy()


    def _initialize_first_data(self, df, PriceInfo):
        """ 시가 수신 시 처리 """
        self.npPriceInfo['시가'] = df.iloc[-2]['시가']  # 전봉 정보 세팅
        self.npPriceInfo['고가'] = df.iloc[-2]['고가']
        self.npPriceInfo['저가'] = df.iloc[-2]['저가']
        self.npPriceInfo['현재가'] = df.iloc[-2]['종가']
        self._process_on_first_data(df, PriceInfo)


    def _process_on_first_data(self, df, PriceInfo):
        dfT_1 = df[df['일자'] == self.strT_1]
        self.fDayOpen = PriceInfo['시가']
        dayHigh_t1 = dfT_1['고가'].max()
        dayLow_t1 = dfT_1['저가'].min()
        self.fDayRange_t1 = dayHigh_t1 - dayLow_t1
        self.fBuyPrice = self.fDayOpen + self.fDayRange_t1 * self.fR
        self.fSellPrice = self.fDayOpen - self.fDayRange_t1 * self.fR
        # Profitable Open Stop
        if self.nDaysSinceEntry >= self.nDayIn-1:
            if (self.nPosition > 0) and (self.fEntryLv < self.fDayOpen):
                self._place_order('EL', self.amt_exit, self.fDayOpen)
            if (self.nPosition < 0) and (self.fEntryLv > self.fDayOpen):
                self._place_order('ES', self.amt_exit, self.fDayOpen)


    def _handle_entry_exit(self, curPrice):
        """ 진입/청산 로직 처리 """
        if self.fBuyPrice and self.nPosition <= 0 and self._is_buy_condition_met(curPrice, self.fBuyPrice):
            self._place_order('B', self.amt_entry, curPrice)
        if self.fSellPrice and self.nPosition >= 0 and self._is_sell_condition_met(curPrice, self.fSellPrice):
            self._place_order('S', self.amt_entry, curPrice)
        

    def _is_buy_condition_met(self, curPrice, buy_price):
        """ 매수 조건 확인 """
        return self.npPriceInfo['현재가'] <= buy_price and curPrice >= buy_price


    def _is_sell_condition_met(self, curPrice, sell_price):
        """ 매도 조건 확인 """
        return self.npPriceInfo['현재가'] >= sell_price and curPrice <= sell_price
    

    def _place_order(self, order_type, amount, curPrice):
        """ 주문 실행 및 로그 출력 """
        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], order_type, amount, curPrice)
        self.logger.info(f"{order_type} {amount} amount ordered at {curPrice}")
        self._chkPos(amount if (order_type == 'B' or order_type == 'ES') else -amount)
        if order_type in ['B', 'S']:
            self.fEntryLv = curPrice


    def lastProc(self):
        if not self.isON:   # 당일 종가 청산
            last_row = self.lstData[self.ix].iloc[-2]
            if self.nPosition and self._check_exit_condition(last_row):
                order_type = 'EL' if self.nPosition > 0 else 'ES'
                self._place_order(order_type, self.amt_exit, 0)
    

    def _check_exit_condition(self, last_row):
        return (((self.fEntryLv > last_row['종가']) or (last_row['저가'] < (self.fDayOpen - self.fDayRange_t1 * self.fR))) or
                ((self.fEntryLv < last_row['종가']) or (last_row['고가'] > (self.fDayOpen + self.fDayRange_t1 * self.fR))))