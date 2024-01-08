# 타임프레임: 1분봉
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
import datetime as dt
import logging


class TS_RB_0048():
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
        self.strTarget = '픽싱 스퀘어'
        

    # 전략 실행
    def execute(self, PriceInfo):
        if type(PriceInfo) == int:
            return

        if (self.npPriceInfo == None) or (self.npPriceInfo['시가'] == 0):    # 첫 데이터 수신시
            self.npPriceInfo = PriceInfo.copy()
            # return
            
        # 분봉 업데이트 시
        if (str(self.npPriceInfo['체결시간'])[4:6] != str(PriceInfo['체결시간'])[4:6]) and \
        (int(str(PriceInfo['체결시간'])[4:6]) % int(self.lstTimeIntrvl[self.ix]) == 0):
            if (not Strategy.dfArticle.empty) and (self.nPosition == 0):
                strTitle = Strategy.dfArticle[Strategy.dfArticle['Title'].str.contains(self.strTarget)]['Title']
                if not strTitle.empty:
                    strFixing = strTitle.str.extract('([-+]?\d+\.?\d*)')
                    if strFixing.notnull():
                        fLevel = float(strFixing[0].values[0])
                        if fLevel > 0:
                            Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'B', self.amt_entry, PriceInfo['현재가'])
                            self.logger.info('Buy %s amount ordered', self.amt_entry)
                            self.chkPos(self.amt_entry)
                        elif fLevel < 0:
                            Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'S', self.amt_entry, PriceInfo['현재가'])
                            self.logger.info('Buy %s amount ordered', self.amt_entry)
                            self.chkPos(self.amt_entry)

        self.npPriceInfo = PriceInfo.copy()


    def lastProc(self):
        if self.isON == False:    # 당일 종가 청산
            if self.nPosition > 0:
                Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'EL', self.amt_exit, 0)
                self.logger.info('ExitLong %s amount ordered', self.amt_exit)
                self.chkPos(-self.amt_exit)
            elif self.nPosition < 0:
                Strategy.setOrder(self.strName, self.lstProductCode[self.ix], 'ES', self.amt_exit, 0)
                self.logger.info('ExitShort %s amount ordered', self.amt_exit)
                self.chkPos(self.amt_exit)