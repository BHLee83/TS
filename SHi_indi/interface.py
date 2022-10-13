from PyQt5.QAxContainer import *
from price import Price
from account import Account


class Interface():
    def __init__(self, IndiWindow):
        self.wndIndi = IndiWindow

        self.userEnv = Account(self.wndIndi)    # 계좌 정보
        self.price = Price(self.wndIndi)    # 시세 정보
        if self.userEnv.userLogin():
            # ComboBox에서 종목 선택시
            # self.wndIndi.cbProductCode.currentIndexChanged.connect(lambda: self.price.stopTR())
            self.wndIndi.cbProductCode.currentIndexChanged.connect(self.price.stopTR)   # 기존 수신중 시세 중지

            # 시세수신 버튼 클릭시
            self.wndIndi.pbRqPrice.clicked.connect(self.btn_Price_Request)
    
    def btn_Price_Request(self):
        # 기존 QtableWidget 내용 삭제 (clear메서드가 확실하지만, column명도 다 날라가서 다시 세팅하기 귀찮음;; Memory leak 발생하는지 확인 필요.)
        self.wndIndi.twProductInfo.setRowCount(0)
        self.price.startTR()    # 시세 수신