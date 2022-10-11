import sys
from PyQt5.QtGui import *
from PyQt5.QtCore import *
from PyQt5.QAxContainer import *
from PyQt5.QtWidgets import QApplication
from PyQt5.QtWidgets import QMainWindow
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtWidgets import QLineEdit
import pandas as pd
import numpy as np


class IndiWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        # QT 타이틀
        self.setWindowTitle("IndiTest")

        # 인디의 TR을 처리할 변수를 생성합니다.
        # self.IndiTR = QAxWidget("GIEXPERTCONTROL64.GiExpertControl64Ctrl.1")
        self.IndiTR = QAxWidget("GIEXPERTCONTROL.GiExpertControlCtrl.1")
        self.IndiReal = QAxWidget("GIEXPERTCONTROL.GiExpertControlCtrl.1")

        # 신한i Indi 자동로그인
        # while True:
        #     login = self.IndiTR.StartIndi('forheart', 'password', 'authpassword', 'C:\SHINHAN-i\indi\giexpertstarter.exe')
        #     print(login)
        #     if login == True :
        #         break

        # Indi API event
        self.IndiTR.ReceiveData.connect(self.ReceiveData)
        self.IndiReal.ReceiveRTData.connect(self.ReceiveRTData)
        self.IndiTR.ReceiveSysMsg.connect(self.ReceiveSysMsg)

        self.rqidD = {} # TR 관리를 위해 사전 변수를 하나 생성합니다.
        PriceInfodt = np.dtype([('단축코드', 'S10'), ('한글종목명', 'S40'), ('현재가', 'f'), ('시가', 'f'), ('고가', 'f'), ('저가', 'f'),
                       ('상한가', 'f'), ('하한가', 'f'), ('전일종가', 'f'), ('매도1호가', 'f'), ('매수1호가', 'f'),
                       ('체결시간', 'S6'), ('누적거래량', 'u4'), ('단위체결량', 'u4'), ('매도1호가수량', 'u4'), ('매수1호가수량', 'u4')])
        self.PriceInfo = np.empty([1], dtype=PriceInfodt)

        Historicaldt = np.dtype([('일자', 'S8'), ('시간', 'S6'), ('시가', 'f'), ('고가', 'f'), ('저가', 'f'), ('종가', 'f'), ('단위거래량', 'u4')])
        self.Historical = np.empty([75], dtype=Historicaldt)

        # PyQt5를 통해 화면을 그려주는 코드입니다.
        self.MainSymbol = ""
        self.edSymbol = QLineEdit(self)
        self.edSymbol.setGeometry(20, 20, 60, 20)
        self.edSymbol.setText("165SC")

        # PyQt5를 통해 버튼만들고 함수와 연결시킵니다.
        btnResearch = QPushButton("시세수신", self)
        btnResearch.setGeometry(85, 20, 60, 20)
        btnResearch.clicked.connect(self.btn_Request) # 버튼을 누르면 'btn_Request' 함수가 실행됩니다.


    def btn_Request(self):
        Symbol = self.edSymbol.text().upper()

        # 기존종목 실시간 해제
        if self.MainSymbol != "":
            self.IndiReal.dynamicCall("UnRequestRTReg(QString, QString)", "MB", self.MainSymbol)
            self.IndiReal.dynamicCall("UnRequestRTReg(QString, QString)", "MC", self.MainSymbol)
            self.IndiReal.dynamicCall("UnRequestRTReg(QString, QString)", "MH", self.MainSymbol)

        self.MainSymbol = Symbol
        
        # cfut_mst : 상품선물종목코드조회(전종목)
        # 해당 TR의 필드입력형식에 맞춰서 TR을 날리면 됩니다.
        # 데이터 요청 형식은 다음과 같습니다.
        ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "cfut_mst")
        rqid = self.IndiTR.dynamicCall("RequestData()") # 데이터 요청
        self.rqidD[rqid] =  "cfut_mst"

        # 차트조회
        ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "TR_CFCHART")
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, self.MainSymbol)    # 단축코드
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, "D")    # 그래프 종류 (1:분데이터, D:일데이터, W:주데이터, M:월데이터)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, "1")    # 시간간격 (분데이터일 경우: 1 – 30, 일/주/월데이터일 경우: 1)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 3, "00000000") # 시작일 (YYYYMMDD, 분 데이터 요청시 : “00000000”)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 4, "99999999") # 종료일 (YYYYMMDD, 분 데이터 요청시 : “99999999”)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 5, "100")  # 조회갯수 (1-9999)
        rqid = self.IndiTR.dynamicCall("RequestData()")
        self.rqidD[rqid] =  "TR_CFCHART"

        # 종목 기본정보 조회
        ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "MB")
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, self.MainSymbol)
        rqid = self.IndiTR.dynamicCall("RequestData()")
        self.rqidD[rqid] = "MB"

        # 종목 현재가 조회
        ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "MC")
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, self.MainSymbol)
        rqid = self.IndiTR.dynamicCall("RequestData()")
        self.rqidD[rqid] = "MC"

        # 종목 호가 조회
        ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "MH")
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, self.MainSymbol)
        rqid = self.IndiTR.dynamicCall("RequestData()")
        self.rqidD[rqid] = "MH"


    # 요청한 TR로 부터 데이터를 받는 함수입니다.
    def ReceiveData(self, rqid):
        # TR을 날릴때 ID를 통해 TR이름을 가져옵니다.
        TRName = self.rqidD[rqid]
        code = self.edSymbol.text().upper()

        # cfut_mst를 요청했었습니다.
        if TRName == "cfut_mst" :
            # GetMultiRowCount()는 TR 결과값의 multi row 개수를 리턴합니다.
            nCnt = self.IndiTR.dynamicCall("GetMultiRowCount()")

            # 받을 열만큼 과거 데이터를 받도록 합니다.
            for i in range(0, nCnt):
                if code == self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1):
                    # 데이터 양식
                    dictCFutMst = {}

                    # 데이터 받기
                    dictCFutMst['표준코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 0)
                    dictCFutMst['단축코드'] = code
                    dictCFutMst['파생상품ID'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 2)
                    dictCFutMst['한글종목명'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 3)
                    dictCFutMst['기초자산ID'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 4)
                    dictCFutMst['스프레드근월물표준코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 5)
                    dictCFutMst['스프레드원월물표준코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 6)
                    dictCFutMst['최종거래일'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 7)
                    dictCFutMst['기초자산종목코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 8)
                    dictCFutMst['거래단위'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 9)
                    dictCFutMst['거래승수'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 10)

                    print(dictCFutMst)
                    break

        # 차트수신
        elif TRName == "TR_CFCHART" :
            nCnt = self.IndiTR.dynamicCall("GetMultiRowCount()")
            np.reshape(self.Historical, nCnt)
            for i in range(0, nCnt):
                self.Historical[i]['일자'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 0)
                self.Historical[i]['시간'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)
                self.Historical[i]['시가'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 2)
                self.Historical[i]['고가'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 3)
                self.Historical[i]['저가'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 4)
                self.Historical[i]['종가'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 5)
                self.Historical[i]['단위거래량'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 9)
            print(self.Historical)

        # 종목 기본정보 수신
        elif TRName == "MB":
            self.PriceInfo[0]['한글종목명'] = self.IndiTR.dynamicCall("GetSingleData(int)", 5).encode('utf8')
            self.PriceInfo[0]['상한가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 13)
            self.PriceInfo[0]['하한가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 14)
            self.PriceInfo[0]['전일종가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 38)
            print(self.PriceInfo[0])
            # 실시간 등록
            ret = self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", "MB", self.MainSymbol)
        
        # 종목 현재가 수신
        elif TRName == "MC":
            self.PriceInfo[0]['단축코드'] = self.IndiTR.dynamicCall("GetSingleData(int)", 1)
            self.PriceInfo[0]['체결시간'] = self.IndiTR.dynamicCall("GetSingleData(int)", 2)
            self.PriceInfo[0]['현재가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 4)
            self.PriceInfo[0]['누적거래량'] = self.IndiTR.dynamicCall("GetSingleData(int)", 8)
            self.PriceInfo[0]['단위체결량'] = self.IndiTR.dynamicCall("GetSingleData(int)", 10)
            self.PriceInfo[0]['시가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 12)
            self.PriceInfo[0]['고가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 13)
            self.PriceInfo[0]['저가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 14)
            # 실시간 등록
            ret = self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", "MC", self.MainSymbol)

        elif TRName == "MH":
            self.PriceInfo[0]['매도1호가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 3)
            self.PriceInfo[0]['매수1호가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 4)
            self.PriceInfo[0]['매도1호가수량'] = self.IndiTR.dynamicCall("GetSingleData(int)", 5)
            self.PriceInfo[0]['매수1호가수량'] = self.IndiTR.dynamicCall("GetSingleData(int)", 6)
            # 실시간 등록
            ret = self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", "MH", self.MainSymbol)

        self.rqidD.__delitem__(rqid)


    def ReceiveRTData(self, RealType):
        # 종목 기본정보 실시간 수신
        if RealType == "MB":
            self.PriceInfo[0]['한글종목명'] = self.IndiReal.dynamicCall("GetSingleData(int)", 5).decode('utf-8')
            self.PriceInfo[0]['상한가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 13)
            self.PriceInfo[0]['하한가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 14)
            self.PriceInfo[0]['전일종가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 38)

        elif RealType == "MC":
            self.PriceInfo[0]['단축코드'] = self.IndiReal.dynamicCall("GetSingleData(int)", 1)
            self.PriceInfo[0]['체결시간'] = self.IndiReal.dynamicCall("GetSingleData(int)", 2)
            self.PriceInfo[0]['현재가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 4)
            self.PriceInfo[0]['누적거래량'] = self.IndiReal.dynamicCall("GetSingleData(int)", 8)
            self.PriceInfo[0]['단위체결량'] = self.IndiReal.dynamicCall("GetSingleData(int)", 10)
            self.PriceInfo[0]['시가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 12)
            self.PriceInfo[0]['고가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 13)
            self.PriceInfo[0]['저가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 14)
            
        elif RealType == "MH":
            self.PriceInfo[0]['매도1호가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 3)
            self.PriceInfo[0]['매수1호가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 4)
            self.PriceInfo[0]['매도1호가수량'] = self.IndiReal.dynamicCall("GetSingleData(int)", 5)
            self.PriceInfo[0]['매수1호가수량'] = self.IndiReal.dynamicCall("GetSingleData(int)", 6)

        print(self.PriceInfo[0])


    # 시스템 메시지를 받은 경우 출력합니다.
    def ReceiveSysMsg(self, MsgID):
        print("System Message Received = ", MsgID)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    IndiWindow = IndiWindow()
    IndiWindow.show()
    app.exec_()