# 타임프레임: 5분봉
# - Entry
#   Long: 12시 이전에 시초가에서 5ATR만큼 상승 / Short: 12시 이전에 시초가에서 5ATR만큼 하락
# - Exit
#   당일종가청산

from System.strategy import Strategy

import pandas as pd
import logging



class TS_RB_0014_N():
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
        self.nMult = 5
        self.nTrailBar = 30
        self.fDayOpen = 0.0
        self.chUp = self.chDn = 0.0
        self.fStopHigh = self.fStopLow = 0.0
        

    def _dataLoad(self):
        """ 공통 데이터 로드 """
        self.lstData[self.ix] = Strategy.getHistData(
            self.lstProductCode[self.ix],
            self.lstAssetType[self.ix],
            self.lstTimeFrame[self.ix],
            int(400 / int(self.lstTimeIntrvl[self.ix]) * self.nTrailBar)
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


    def _calcParam(self):
        """ 파라미터 계산 및 매수/매도 조건 설정 """
        df = self.lstData[self.ix][-(self.nTrailBar+1):-1]  # 가능한 최소한의 데이터만 사용

        # 파라미터 계산
        rngMA = (df['고가'] - df['저가']).shift().rolling(window=self.nTrailBar).mean()
        self.chUp = self.fDayOpen + rngMA.iloc[-1] * self.nMult
        self.chDn = self.fDayOpen - rngMA.iloc[-1] * self.nMult
        self.fStopLow = df['저가'][-self.nTrailBar:].min()
        self.fStopHigh = df['고가'][-self.nTrailBar:].max()


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
            self.fDayOpen = PriceInfo['시가']

        if self._should_update_candle(PriceInfo):
            if not self._dataLoad():
                return  # 데이터 로드 실패 시 종료
            self._calcParam()

        self._handle_entry_exit(PriceInfo)
        self.npPriceInfo = PriceInfo.copy()


    def _should_update_candle(self, PriceInfo):
        """ 분봉 업데이트 여부 확인 """
        prev_time = int(str(self.npPriceInfo['체결시간'])[4:6])
        curr_time = int(str(PriceInfo['체결시간'])[4:6])
        return prev_time != curr_time and curr_time % int(self.lstTimeIntrvl[self.ix]) == 0
    
    
    def _handle_entry_exit(self, PriceInfo):
        t = int(PriceInfo['체결시간'].decode())
        if (t > 90500) and (t < 120500): # Entry
            if self.nPosition <= 0 and self._is_buy_condition_met(PriceInfo['현재가'], self.chUp):
                self._place_order('B', self.amt_entry, PriceInfo['현재가'])
            if self.nPosition >= 0 and self._is_sell_condition_met(PriceInfo['현재가'], self.chDn):
                self._place_order('S', self.amt_entry, PriceInfo['현재가'])

        if self.nPosition > 0 and self._is_sell_condition_met(PriceInfo['현재가'], self.fStopLow):  # Trailing Stops
            self._place_order('EL', self.amt_exit, PriceInfo['현재가'])
        if self.nPosition < 0 and self._is_buy_condition_met(PriceInfo['현재가'], self.fStopHigh):
            self._place_order('ES', self.amt_exit, PriceInfo['현재가'])


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


    def lastProc(self):
        if not self.isON:   # 당일 종가 청산
            if self.nPosition:
                order_type = 'EL' if self.nPosition > 0 else 'ES'
                self._place_order(order_type, self.amt_exit, 0)