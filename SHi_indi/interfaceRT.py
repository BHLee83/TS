from PyQt5.QtWidgets import *
import numpy as np

class InterfaceRT():
    def __init__(self, IndiWindow):
        self.wndIndi = IndiWindow

        # QtableWidget settings
        column_headers = ['영문종목명', '체결시간', '상한가', '하한가', '전일종가', '시가', '고가', '저가', '현재가', '단위체결량', '누적거래량']
        self.wndIndi.twProductInfo.setHorizontalHeaderLabels(column_headers)
        
    # PriceInfo: '영문종목명', '상한가', '하한가', '전일종가', '단축코드', '체결시간', '시가', '고가', '저가', '현재가', '단위체결량', '누적거래량', '매도1호가', '매도1호가수량', '매수1호가', '매수1호가수량'
    def setTableWidgetData(self, PriceInfo):

        nRowCnt = self.wndIndi.twProductInfo.rowCount()
        self.wndIndi.twProductInfo.insertRow(nRowCnt)

        t = str(PriceInfo[5])
        self.wndIndi.twProductInfo.setItem(nRowCnt, 0, QTableWidgetItem(str(PriceInfo[0]).split("'")[1]))   # 영문종목명
        self.wndIndi.twProductInfo.setItem(nRowCnt, 1, QTableWidgetItem(t[2:4] + ":" + t[4:6] + ":" + t[6:8]))  # 체결시간
        self.wndIndi.twProductInfo.setItem(nRowCnt, 2, QTableWidgetItem(str(PriceInfo[1]))) # 상한가
        self.wndIndi.twProductInfo.setItem(nRowCnt, 3, QTableWidgetItem(str(PriceInfo[2]))) # 히힌기
        self.wndIndi.twProductInfo.setItem(nRowCnt, 4, QTableWidgetItem(str(PriceInfo[3]))) # 전일종가
        self.wndIndi.twProductInfo.setItem(nRowCnt, 5, QTableWidgetItem(str(PriceInfo[6]))) # 시가
        self.wndIndi.twProductInfo.setItem(nRowCnt, 6, QTableWidgetItem(str(PriceInfo[7]))) # 고가
        self.wndIndi.twProductInfo.setItem(nRowCnt, 7, QTableWidgetItem(str(PriceInfo[8]))) # 저가
        self.wndIndi.twProductInfo.setItem(nRowCnt, 8, QTableWidgetItem(str(PriceInfo[9]))) # 현재가
        self.wndIndi.twProductInfo.setItem(nRowCnt, 9, QTableWidgetItem(str(PriceInfo[10])))    # 단위체결량
        self.wndIndi.twProductInfo.setItem(nRowCnt, 10, QTableWidgetItem(str(PriceInfo[11])))   # 누적거래량

        self.wndIndi.twProductInfo.AlignRight

        if len(t) == 9: # b'hhmmss'
            self.wndIndi.twProductInfo.resizeColumnsToContents()
        #     self.wndIndi.twProductInfo.resizeRowsToContents()

        self.wndIndi.twProductInfo.scrollToItem(self.wndIndi.twProductInfo.item(nRowCnt, 0))    # Scroll to end row
    
    def setSysMsgOnStatusBar(self, MsgID, moduleName):
        # 재연결 실패, 재접속 실패, 공지 등의 수동으로 재접속이 필요할 경우에는 이벤트는 발생하지 않고 신한i Expert Main에서 상태를 표현해준다.
        if MsgID == 3:
            MsgStr = "체결통보 데이터 재조회 필요(" + moduleName +")"
        elif MsgID == 7:
            MsgStr = "통신 실패 후 재접속 성공(" + moduleName +")"
        elif MsgID == 10:
            MsgStr = "시스템이 종료됨(" + moduleName +")"
        elif MsgID == 11:
            MsgStr = "시스템이 시작됨(" + moduleName +")"
        else:
            MsgStr = "System Message Received in module '" + moduleName + "' = " + str(MsgID)
        # print(MsgStr)
        self.wndIndi.statusbar = MsgStr