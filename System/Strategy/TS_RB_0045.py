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


class TS_RB_0045_N():
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
        self.strT_1 = Strategy.strT_1
        self.fR = 0.3
        self.nEntryLimit = 153000

        self.fDayOpen = 0.0
        self.fDayHigh_t1 = self.fDayLow_t1 = self.fDayClose_t1 = 0.0
        self.fBuyPrice = self.fSellPrice = 0.0
        self.fDemarkLine1 = self.fDemarkLine2 = 0.0
        

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
            self.nPosition = Strategy.getPosition(self.strName, self.lstAssetCode[self.ix], self.lstAssetType[self.ix])    # 포지션 확인 및 수량 지정
        else:
            self.nPosition += amt
        self.amt_entry = abs(self.nPosition) + self.lstTrUnit[self.ix] * self.fWeight
        self.amt_exit = abs(self.nPosition)


    def execute(self, PriceInfo):
        """ 전략 실행 """
        if isinstance(PriceInfo, int):  # 최초 실행시
            if not self._dataLoad():
                return  # 데이터 로드 실패 시 종료
            self._chkPos()
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
        self.fDayHigh_t1 = dfT_1['고가'].max()
        self.fDayLow_t1 = dfT_1['저가'].min()
        self.fDayClose_t1 = dfT_1['종가'].iloc[-1]

        if self.fDayOpen > self.fDayClose_t1:
            self.fDemarkLine1 = (self.fDayHigh_t1 + self.fDayClose_t1 + 2 * self.fDayLow_t1) / 2 - self.fDayLow_t1
            self.fDemarkLine2 = (self.fDayHigh_t1 + self.fDayClose_t1 + 2 * self.fDayLow_t1) / 2 - self.fDayHigh_t1
        elif self.fDayOpen < self.fDayClose_t1:
            self.fDemarkLine1 = (2 * self.fDayHigh_t1 + self.fDayClose_t1 + self.fDayLow_t1) / 2 - self.fDayLow_t1
            self.fDemarkLine2 = (2 * self.fDayHigh_t1 + self.fDayClose_t1 + self.fDayLow_t1) / 2 - self.fDayHigh_t1
        else:
            self.fDemarkLine1 = (self.fDayHigh_t1 + 2 * self.fDayClose_t1 + self.fDayLow_t1) / 2 - self.fDayLow_t1
            self.fDemarkLine2 = (self.fDayHigh_t1 + 2 * self.fDayClose_t1 + self.fDayLow_t1) / 2 - self.fDayHigh_t1
        
        if self.fDayOpen > self.fDemarkLine1:
            self.fBuyPrice = self.fDayOpen + (self.fDemarkLine1 - self.fDemarkLine2) * self.fR
        if self.fDayOpen < self.fDemarkLine2:
            self.fBuyPrice = self.fDemarkLine2
        if self.fDayOpen > self.fDemarkLine1:
            self.fSellPrice = self.fDemarkLine1
        if self.fDayOpen < self.fDemarkLine2:
            self.fSellPrice = self.fDayOpen - (self.fDemarkLine1 - self.fDemarkLine2) * self.fR

        
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
        self._chkPos(amount if order_type == 'B' else -amount)