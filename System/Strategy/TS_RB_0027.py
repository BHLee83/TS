# 타임프레임: 5분봉
# - Entry
#   Long: CCI 이평이 -100보다 크고 EntryBar봉 이내에 -100을 골든 크로스한 적 있을 때, HiLoLen 기간중 고점+1 에서
#   Short: CCI 이평이 100보다 작고 EntryBar봉 이내에 100을 데드 크로스한 적 있을 때, HiLoLen 기간중 저점-1 에서
# - Exit
#   EL(ES): 진입후 고(저)점을 높일(낮출) 때마다 관찰 봉수 줄여가며 해당 관찰 봉 이내의 저(고)점-1(+1)에서 추적 청산
# 수수료: 0.006%
# 슬리피지: 0.05pt

from System.strategy import Strategy

import pandas as pd
import talib as ta
import logging



class TS_RB_0027_N():
    def __init__(self, info) -> None:
        self.logger = logging.getLogger(__class__.__name__)  # 로그 생성
        self.logger.info('Init. start')

        # 기본 설정 초기화
        self._initialize(info)

    
    def _initialize(self, info):
        """ 초기 설정 값들을 처리하는 메서드 """
        # General info
        self.npPriceInfo = None

        # Global setting variables
        # self.dfInfo = info
        self.strName = info['NAME']
        self.lstAssetCode = info['ASSET_CODE'].split(',') # 거래대상은 여러개일 수 있음
        self.lstAssetType = info['ASSET_TYPE'].split(',')
        self.lstUnderId = info['UNDERLYING_ID'].split(',')
        self.lstTimeFrame = info['TIMEFRAME'].split(',')
        self.isON = bool(int(info['OVERNIGHT']))
        self.isPyramid = bool(int(info['PYRAMID']))
        self.lstTrUnit = list(map(int, info['TR_UNIT'].split(',')))
        self.fWeight = info['WEIGHT']

        self.lstProductCode = Strategy.setProductCode(self.lstUnderId)
        # self.lstProductNCode = list(map(lambda x: 'KRDRVFU'+x, self.lstUnderId))    # for SHi-indi spec. 연결선물 코드
        self.lstProductNCode = [f'KRDRVFU{x}' for x in self.lstUnderId]    # for SHi-indi spec. 연결선물 코드
        self.lstTimeFrame_tmp = Strategy.setTimeFrame(self.lstTimeFrame)  # for SHi-indi spec.
        self.lstTimeWnd, self.lstTimeIntrvl = self.lstTimeFrame_tmp
        self.ix = 0 # 대상 상품의 인덱스
        self.nPosition = 0
        self.amt_entry = 0
        self.amt_exit = 0

        # Local setting variables
        self.lstData = [pd.DataFrame(None)] * len(self.lstAssetCode)
        self.nCCILen = 130
        self.nAvgLen = 80
        self.nHiLoLen = 30
        self.nEntryBar = 12
        self.nTrailStop = 0
        
        self.fBuyPrice = 0.0
        self.fSellPrice = 0.0
        self.fStopPrice = 0.0


    def _dataLoad(self):
        """ 공통 데이터 로드 """
        self.lstData[self.ix] = Strategy.getHistData(
            self.lstProductCode[self.ix],
            self.lstAssetType[self.ix],
            self.lstTimeFrame[self.ix],
            int(400 / int(self.lstTimeIntrvl[self.ix])) * self.nCCILen + self.nEntryBar
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
        df = self.lstData[self.ix][-(self.nCCILen+1):-1]  # 가능한 최소한의 데이터만 사용
        MP = self.nPosition / (self.lstTrUnit[self.ix] * self.fWeight)

        cci = ta.CCI(df['고가'], df['저가'], df['종가'], self.nCCILen)
        df['AvgCCI'] = cci.rolling(window=self.nAvgLen).mean()

        df['BuySetup'] = (df['AvgCCI'] > -100) & (df['AvgCCI'].shift(1) <= -100)
        df['SellSetup'] = (df['AvgCCI'] < 100) & (df['AvgCCI'].shift(1) >= 100)

        # 기존 슬라이싱 후 다시 열 참조하는 것을 미리 캐싱
        high_price = max(df['고가'][-self.nHiLoLen-1:-1])
        low_price = min(df['저가'][-self.nHiLoLen-1:-1])
        if (df.iloc[-1]['AvgCCI'] > -100) and (df['BuySetup'].iloc[-self.nEntryBar:].any()):    # Entry setup
            self.fBuyPrice = high_price + 1
        if (df.iloc[-1]['AvgCCI'] < 100) and (df.iloc[-self.nEntryBar:]['SellSetup'].sum() > 0):
            self.fSellPrice = low_price - 1
            
        if MP == 1 and df.iloc[-1]['고가'] > high_price:    # Exit setup
            self.nTrailStop = max(self.nTrailStop - 1, 1)
        if MP == -1 and df.iloc[-1]['저가'] < low_price:
            self.nTrailStop = max(self.nTrailStop - 1, 1)
        if MP == 1:
            self.fStopPrice = min(df['저가'][-self.nTrailStop:]) - 1
        if MP == -1:
            self.fStopPrice = max(df['고가'][-self.nTrailStop:]) + 1


    def execute(self, PriceInfo):
        """ 전략 실행 """
        if isinstance(PriceInfo, int):  # 최초 실행시
            if not self._dataLoad():
                return  # 데이터 로드 실패 시 종료
            self._chkPos()
            return
        
        # if PriceInfo['단축코드'] == 'USD':
        #     return

        if self.npPriceInfo is None:
            self.npPriceInfo = PriceInfo.copy()
            return
        
        if self.npPriceInfo['시가'] == 0:    # 첫 데이터 수신시
            self._initialize_first_data(self.lstData[self.ix])

        if self._should_update_candle(PriceInfo):
            self._dataLoad()
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
        if self.fBuyPrice and self.nPosition <= 0 and self._check_buy_condition(PriceInfo):
            self._place_order('B', PriceInfo)

        if self.fSellPrice and self.nPosition >= 0 and self._check_sell_condition(PriceInfo):
            self._place_order('S', PriceInfo)

        if self.fStopPrice:
            self._handle_stop_loss(PriceInfo)

    
    def _check_buy_condition(self, PriceInfo):
        """ 매수 조건 확인 """
        return (self.npPriceInfo['현재가'] <= self.fBuyPrice and PriceInfo['현재가'] >= self.fBuyPrice)


    def _check_sell_condition(self, PriceInfo):
        """ 매도 조건 확인 """
        return (self.npPriceInfo['현재가'] >= self.fSellPrice and PriceInfo['현재가'] <= self.fSellPrice)


    def _place_order(self, order_type, PriceInfo):
        """ 주문 실행 """
        Strategy.setOrder(self.strName, self.lstProductCode[self.ix], order_type, self.amt_entry, PriceInfo['현재가'])
        self.logger.info(f"{order_type} {self.amt_entry} amount ordered")
        self._chkPos(self.amt_entry if order_type == 'B' else -self.amt_entry)
        self.fBuyPrice = 0.0
        self.fSellPrice = 0.0
        self.nTrailStop = self.nHiLoLen


    def _handle_stop_loss(self, PriceInfo):
        """ 손절 처리 """
        order_type = 'EL' if self.nPosition > 0 else 'ES'
        if self._check_stop_condition(PriceInfo):
            Strategy.setOrder(self.strName, self.lstProductCode[self.ix], order_type, self.amt_exit, PriceInfo['현재가'])
            self.logger.info(f"{order_type} {self.amt_exit} amount ordered at {PriceInfo['현재가']}")
            self._chkPos(-self.amt_exit if order_type == 'EL' else self.amt_exit)
            self.fStopPrice = 0.0


    def _check_stop_condition(self, PriceInfo):
        """ 손절 조건 확인 """
        return ((self.nPosition > 0 and self.npPriceInfo['현재가'] >= self.fStopPrice and PriceInfo['현재가'] <= self.fStopPrice) or
                (self.nPosition < 0 and self.npPriceInfo['현재가'] <= self.fStopPrice and PriceInfo['현재가'] >= self.fStopPrice))