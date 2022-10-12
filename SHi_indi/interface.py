from PyQt5.QAxContainer import *
from PyQt5.QtWidgets import QLineEdit
from PyQt5.QtWidgets import QPushButton
from price import Price
from account import Account


class Interface():
    def __init__(self, IndiWindow):
        self.wndIndi = IndiWindow
        self.dfAccInfo = ""

        # QT 타이틀
        self.wndIndi.setWindowTitle("Shinhan-i Indi TradingSystem")

        self.userEnv = Account()
        self.price = Price()
        if self.userEnv.userLogin():
            self.dfAccInfo = self.userEnv.getAccount()

            # PyQt5를 통해 화면을 그려주는 코드입니다.
            self.MainSymbol = "165SC"
            self.edSymbol = QLineEdit(self.wndIndi)
            self.edSymbol.setGeometry(20, 20, 60, 20)
            self.edSymbol.setText(self.MainSymbol)

            # PyQt5를 통해 버튼만들고 함수와 연결시킵니다.
            btnResearch = QPushButton("시세수신", self.wndIndi)
            btnResearch.setGeometry(85, 20, 60, 20)
            btnResearch.clicked.connect(self.btn_Request) # 버튼을 누르면 'btn_Request' 함수가 실행됩니다.
    
    def btn_Request(self):
        self.price.request(self.edSymbol.text())
