# 타임프레임: 5분봉
# - Setup: 
# 수수료: 0.006%
# 슬리피지: 0.05pt

from System.strategy import Strategy

import pandas as pd
import math
import talib as ta
import logging



class TS_RB_0033_N():
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
        
        # 로컬 변수 초기화
        self.lstData = [pd.DataFrame(None)] * len(self.lstAssetCode)
        self._initialize_local_variables()

    def _initialize_local_variables(self):
        """ 로컬 변수 초기화 메서드 """
        self.nFLen = 5
        self.nSLen = 30
        self.nADXLen = 12
        self.bLSetup = self.bSSetup = False
        self.bBFlag = self.bSFlag = False
        self.bADXSetup = False
        self.fBuyPrice1 = self.fBuyPrice2 = 0.0
        self.fSellPrice1 = self.fSellPrice2 = 0.0
        self.fEL = self.fES = 0.0
        self.amt_entry = self.amt_exit = 0
        

    def _dataLoad(self):
        """ 공통 데이터 로드 """
        self.lstData[self.ix] = Strategy.getHistData(
            self.lstProductCode[self.ix],
            self.lstAssetType[self.ix],
            self.lstTimeFrame[self.ix],
            int(400 / int(self.lstTimeIntrvl[self.ix])) * self.nSLen + 1
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


    def _calcParam(self):
        """ 파라미터 계산 및 매수/매도 조건 설정 """
        df = self.lstData[self.ix][-(self.nSLen+1):-1]  # 가능한 최소한의 데이터만 사용
        MP = self.nPosition / (self.lstTrUnit[self.ix] * self.fWeight)

        # 파라미터 계산
        FLen, SLen = math.ceil((self.nFLen+1)*0.5), math.ceil((self.nSLen+1)*0.5)
        df['TRIA1'] = ta.MA(ta.MA(df['종가'], FLen), FLen)
        df['TRIA2'] = ta.MA(ta.MA(df['종가'], SLen), SLen)
        df['ADXVal'] = ta.ADX(df['고가'], df['저가'], df['종가'], self.nADXLen)

        # Setup 설정
        last_row = df.iloc[-1]  # 마지막 행 캐싱
        self._reset_flags()
        self._update_flags(last_row, df)
        self._calculate_entry_exit_prices(last_row, MP)


    def _reset_flags(self):
        """ 초기 플래그 값 리셋 """
        self.bLSetup = self.bSSetup = False
        self.bBFlag = self.bSFlag = False
        self.bADXSetup = False
        self.fBuyPrice1 = self.fBuyPrice2 = 0.0
        self.fSellPrice1 = self.fSellPrice2 = 0.0


    def _update_flags(self, last_row, df):
        """ 플래그 업데이트 """
        if last_row['TRIA1'] > last_row['TRIA2']:
            self.bLSetup = True
        if last_row['TRIA1'] < last_row['TRIA2']:
            self.bSSetup = True
        if last_row['ADXVal'] > df['ADXVal'].iloc[-self.nFLen-1]:
            self.bADXSetup = True
        if self.bLSetup and self.bADXSetup and last_row['고가'] < last_row['TRIA1']:
            self.bBFlag = True
        if self.bSSetup and self.bADXSetup and last_row['저가'] > last_row['TRIA1']:
            self.bSFlag = True


    def _calculate_entry_exit_prices(self, last_row, MP):
        """ 진입/청산 가격 계산 """
        if (self.bLSetup and MP < 1) or (self.bBFlag and abs(MP) < 3):  # Entry setup
            self.fBuyPrice1, self.fBuyPrice2 = (last_row['고가'] + 1, 0.0) if MP < 1 else (0.0, last_row['TRIA1'] + 1)
        if (self.bSSetup and MP > -1) or (self.bSFlag and abs(MP) < 3):
            self.fSellPrice1, self.fSellPrice2 = (last_row['저가'] - 1, 0.0) if MP > -1 else (0.0, last_row['TRIA1'] - 1)

        if MP > 0:    # Exit setup
            self.fEL = last_row['TRIA2'] - 1
        elif MP < 0:
            self.fES = last_row['TRIA2'] + 1


    def execute(self, PriceInfo):
        """ 전략 실행 """
        if isinstance(PriceInfo, int):  # 최초 실행시
            if not self._dataLoad():
                return  # 데이터 로드 실패 시 종료
            self._chkPos()
            self._calcParam()    # 파라미터 계산
            return
        
        if self.npPriceInfo is None:
            self.npPriceInfo = PriceInfo.copy()
            return

        if self.npPriceInfo['시가'] == 0:   # 첫 데이터 수신시
            self._initialize_first_data(self.lstData[self.ix])

        if self._should_update_candle(PriceInfo):
            if not self._dataLoad():
                return  # 데이터 로드 실패 시 종료
            self._calcParam()

        self._handle_entry_exit(PriceInfo)
        self.npPriceInfo = PriceInfo.copy()


    def _initialize_first_data(self, df):
        """ 시가 수신 시 처리 """
        self.npPriceInfo['시가'] = df.iloc[-2]['시가']  # 전봉 정보 세팅
        self.npPriceInfo['고가'] = df.iloc[-2]['고가']
        self.npPriceInfo['저가'] = df.iloc[-2]['저가']
        self.npPriceInfo['현재가'] = df.iloc[-2]['종가']


    def _should_update_candle(self, PriceInfo):
        """ 분봉 업데이트 여부 확인 """
        prev_time = int(str(self.npPriceInfo['체결시간'])[4:6])
        curr_time = int(str(PriceInfo['체결시간'])[4:6])
        return prev_time != curr_time and curr_time % int(self.lstTimeIntrvl[self.ix]) == 0
    

    def _handle_entry_exit(self, PriceInfo):
        """ 진입/청산 로직 처리 """
        self._process_buy_orders(PriceInfo)
        self._process_sell_orders(PriceInfo)
        self._process_exit_orders(PriceInfo)


    def _process_buy_orders(self, PriceInfo):
        """ 매수 주문 처리 """
        if self.fBuyPrice1 and self.nPosition <= 0 and self._is_buy_condition_met(PriceInfo, self.fBuyPrice1):
            self._place_order('B', self.amt_entry, PriceInfo, 'B1')
        elif self.fBuyPrice2 and self.nPosition > 0 and self._is_buy_condition_met(PriceInfo, self.fBuyPrice2):
            self._place_order('B', self.lstTrUnit[self.ix], PriceInfo, 'B2')


    def _process_sell_orders(self, PriceInfo):
        """ 매도 주문 처리 """
        if self.fSellPrice1 and self.nPosition >= 0 and self._is_sell_condition_met(PriceInfo, self.fSellPrice1):
            self._place_order('S', self.amt_entry, PriceInfo, 'S1')
        elif self.fSellPrice2 and self.nPosition < 0 and self._is_sell_condition_met(PriceInfo, self.fSellPrice2):
            self._place_order('S', self.lstTrUnit[self.ix], PriceInfo, 'S2')


    def _process_exit_orders(self, PriceInfo):
        """ 청산 주문 처리 """
        if self.fEL and self.nPosition > 0 and self._is_sell_condition_met(PriceInfo, self.fEL):
            self._place_order('EL', self.amt_exit, PriceInfo, 'EL')
        elif self.fES and self.nPosition < 0 and self._is_buy_condition_met(PriceInfo, self.fES):
            self._place_order('ES', self.amt_exit, PriceInfo, 'ES')


    def _is_buy_condition_met(self, PriceInfo, buy_price):
        """ 매수 조건 확인 """
        return self.npPriceInfo['현재가'] <= buy_price and PriceInfo['현재가'] >= buy_price


    def _is_sell_condition_met(self, PriceInfo, sell_price):
        """ 매도 조건 확인 """
        return self.npPriceInfo['현재가'] >= sell_price and PriceInfo['현재가'] <= sell_price


    def _place_order(self, order_type, amount, PriceInfo, price_label):
        """ 주문 실행 및 로그 출력 """
        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], order_type, amount, PriceInfo['현재가'])
        self.logger.info(f"{price_label} {amount} amount ordered at {PriceInfo['현재가']}")
        # setattr(self, price_label, 0.0) # 주문 완료 후 가격을 0으로 초기화
        self._chkPos(amount if (order_type == 'B' or order_type == 'ES') else -amount)