from PyQt5.QtWidgets import *

class InterfaceRT():
    def __init__(self, IndiWindow):
        self.wndIndi = IndiWindow

        # QtableWidget settings
        column_headers = ['체결시간', '상한가', '하한가', '전일종가', '시가', '고가', '저가', '현재가', '단위체결량', '누적거래량']
        self.wndIndi.twProductInfo.setHorizontalHeaderLabels(column_headers)
        self.wndIndi.twProductInfo.setEditTriggers(QAbstractItemView.NoEditTriggers)    # 테이블 직접 편집 불가
        
    def setTableWidgetData(self, PriceInfo):

        nRowCnt = self.wndIndi.twProductInfo.rowCount()
        self.wndIndi.twProductInfo.insertRow(nRowCnt)

        t = str(PriceInfo['체결시간'])
        t = t[2:4] + ":" + t[4:6] + ":" + t[6:8]
        self.wndIndi.twProductInfo.setItem(nRowCnt, 0, QTableWidgetItem(t))
        self.wndIndi.twProductInfo.setItem(nRowCnt, 1, QTableWidgetItem(PriceInfo['상한가']))
        self.wndIndi.twProductInfo.setItem(nRowCnt, 2, QTableWidgetItem(PriceInfo['하한가']))
        self.wndIndi.twProductInfo.setItem(nRowCnt, 3, QTableWidgetItem(PriceInfo['전일종가']))
        self.wndIndi.twProductInfo.setItem(nRowCnt, 4, QTableWidgetItem(PriceInfo['시가']))
        self.wndIndi.twProductInfo.setItem(nRowCnt, 5, QTableWidgetItem(PriceInfo['고가']))
        self.wndIndi.twProductInfo.setItem(nRowCnt, 6, QTableWidgetItem(PriceInfo['저가']))
        self.wndIndi.twProductInfo.setItem(nRowCnt, 7, QTableWidgetItem(PriceInfo['현재가']))
        self.wndIndi.twProductInfo.setItem(nRowCnt, 8, QTableWidgetItem(PriceInfo['단위체결량']))
        self.wndIndi.twProductInfo.setItem(nRowCnt, 9, QTableWidgetItem(PriceInfo['누적거래량']))

        # if len(t) == 8:
        #     self.wndIndi.twProductInfo.resizeColumnsToContents()
        #     self.wndIndi.twProductInfo.resizeRowsToContents()
    
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