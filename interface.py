from PyQt5.QAxContainer import *
from SHi_indi.account import Account
from SHi_indi.price import Price
from SHi_indi.priceRT import PriceRT
from Strategy.strategy import *


class Interface():
    def __init__(self, IndiWindow):
        self.wndIndi = IndiWindow

        self.lstObj_Strategy = []

        self.userEnv = Account(self.wndIndi)
        if self.userEnv.userLogin():    # 로그인
            
            self.userEnv.setAccount()   # 계좌 세팅
            
            self.price = Price(self.wndIndi)    # 시세 정보(Historical)
            self.priceRT = PriceRT(self.wndIndi, self.lstObj_Strategy)    # 시세 정보(RT)

            # ComboBox에서 종목 변경시
            # self.wndIndi.cbProductCode.currentIndexChanged.connect(self.priceRT.stopTR)   # 수신중 시세 중지

            # 시세 요청 버튼 클릭시
            # self.wndIndi.pbRqPrice.clicked.connect(self.btn_Price_Request)

            # 전략 실행 버튼 클릭시
            self.wndIndi.pbRunStrategy.clicked.connect(self.btn_Run_Strategy)
    
    def btn_Price_Request(self):
        self.wndIndi.twProductInfo.setRowCount(0)   # QtableWidget 기존 내용 삭제
        self.priceRT.startTR()    # 시세 수신

    def btn_Run_Strategy(self):
        # 1. 전략 생성
        strProductCode = self.wndIndi.cbProductCode.currentText()
        strTimeFrame = 'D'
        strTimeIntrv = '1'
        strStartDate = None
        strEndDate = None

        ## param: 종목코드, 
        self.lstObj_Strategy = []
        self.lstObj_Strategy.append(TS_RB_0001(strProductCode, strTimeFrame, strTimeIntrv, strStartDate, strEndDate))
        # self.lstObj_Strategy.append(TS_RB_0002(strProductCode, strTimeFrame, strTimeIntrv, strStartDate, strEndDate))
        for i in self.lstObj_Strategy:
            self.price.rqHistoricalData(i)

        self.btn_Price_Request()    # 2. 실시간 시세 수신