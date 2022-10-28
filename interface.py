import importlib
from ssl import DER_cert_to_PEM_cert
import pandas as pd
import time
import datetime as dt

# from PyQt5.QAxContainer import *
from PyQt5.QtWidgets import *
# from PyQt5.QtCore import Qt

from SHi_indi.account import Account
from SHi_indi.price import Price
from SHi_indi.priceRT import PriceRT
from SHi_indi.order import Order
from SHi_indi.balance import Balance
from System.strategy import Strategy



class Interface():

    def __init__(self, IndiWindow):
        
        # Global settings
        self.wndIndi = IndiWindow
        self.strStrategyPath = 'System.Strategy'
        self.dtToday = dt.datetime.now().date()
        self.strToday = format(self.dtToday, '%Y%m%d')

        # Local settings
        self.lstObj_Strategy = []
        self.dfAcntInfo = pd.DataFrame(None)
        self.strAcntCode = ''

        # Init. proc.
        self.userEnv = Account(self)
        if self.userEnv.userLogin():    # 로그인
            while self.wndIndi.cbAcntCode.currentText() == '':
                time.sleep(1)   # 로그인 처리 시간 걸림
                self.userEnv.setAccount()   # 계좌 세팅
            self.dfAcntInfo = self.userEnv.getAccount(self.wndIndi.cbAcntCode.currentText())

            self.price = Price(self)    # 정보(Historical) 시세 조회용
            self.priceRT = PriceRT(self)    # 실시간(RT) 시세 조회용
            self.objOrder = Order(self) # 주문
            self.objBalance = Balance(self) # 잔고 조회
            
            # 상품 선물 마스터 조회
            while self.wndIndi.cbProductCode.currentText() == '':
                self.price.rqProductMstInfo("cfut_mst") # 상품선물 전종목 정보 (-> setNearMonth)
                time.sleep(1)

            self.initAcntInfo()
            self.initStrategyInfo() # 초기 전략 세팅
            
            # Events
            self.wndIndi.cbAcntCode.currentIndexChanged.connect(self.initAcntInfo)    # 종목코드 변경
            self.wndIndi.cbProductCode.currentIndexChanged.connect(self.initStrategyInfo)    # 종목코드 변경
            # self.wndIndi.pbRqPrice.clicked.connect(self.pbRqPrice)    # 시세 요청 버튼 클릭
            self.wndIndi.pbRunStrategy.clicked.connect(self.pbRunStrategy)   # 전략 실행 버튼 클릭


    def initAcntInfo(self):
        self.strAcntCode = self.wndIndi.cbAcntCode.currentText()
        self.dfAcntInfo = self.userEnv.getAccount(self.strAcntCode)

        self.objBalance.rqBalance(self.strAcntCode, self.dfAcntInfo['Acnt_Pwd'][0])


    def setNearMonth(self):
        df = Strategy.dfCFutMst.drop_duplicates(subset=['단축코드'])
        for i in df['단축코드']:
            self.wndIndi.cbProductCode.addItem(i)


    def initStrategyInfo(self):
        Strategy.setStrategyInfo(self.wndIndi.cbProductCode.currentText())
        if len(Strategy.dfStrategyInfo) != 0:
            self.setTwStrategyInfoUI()  # 전략 세팅값 확인(UI)


    def pbRunStrategy(self):
        self.createStrategy()   # 1. 전략 생성 & 실행(최초 1회)
        self.pbRqPrice()    # 2. 실시간 시세 수신


    # 1. 전략 생성
    def createStrategy(self):
        self.lstObj_Strategy = []
        for i in Strategy.dfStrategyInfo.index:
            if Strategy.dfStrategyInfo['USE'][i] == '1':    # 실행 여부 True인 전략만
                # 동적 import
                name = Strategy.dfStrategyInfo['NAME'][i]
                module = importlib.import_module(self.strStrategyPath + '.' + name, name)
                globals().update(module.__dict__)
                my_class = globals()[name]   # 전략 이름의 클래스 지정 (Reflection)
                self.lstObj_Strategy.append(my_class(Strategy.dfStrategyInfo.loc[i]))   # 클래스 생성 & 초기화

        for i in self.lstObj_Strategy:  # 전략별 과거 데이터 세팅
            i.createHistData(self.price)

        for i in self.lstObj_Strategy:  # 전략 실행 (최초 1회)
            i.execute(0)

        self.orderStrategy()    # 접수된 주문 실행


    # 2. 실시간 시세 수신
    def pbRqPrice(self):
        self.wndIndi.twProductInfo.setRowCount(0)   # 기존 내용 삭제
        self.priceRT.startTR()    # 시세 수신


    # 3. 전략 실행 (실시간)
    def executeStrategy(self, PriceInfo):
        Strategy.chkPrice(self.price, PriceInfo)  # 분봉 완성 check
        for i in self.lstObj_Strategy:
            i.execute(PriceInfo) # 3. 전략 실행

        self.orderStrategy(PriceInfo)    # 4. 접수된 주문 실행


    # 주문 실행
    def orderStrategy(self, PriceInfo=None):
        self.dfAcntInfo = self.userEnv.getAccount(self.wndIndi.cbAcntCode.currentText()) # 계좌 정보
        Strategy.executeOrder(self, self.dfAcntInfo['Acnt_Code'][0], self.dfAcntInfo['Acnt_Pwd'][0], PriceInfo)

        self.objOrder.iqrySettle(self.strAcntCode, self.dfAcntInfo['Acnt_Pwd'][0], self.strToday)   # 체결/미체결 조회
        self.objBalance.rqBalance(self.strAcntCode, self.dfAcntInfo['Acnt_Pwd'][0])    # 계좌 잔고 조회

        Strategy.lstOrderInfo = []  # 주문내역 초기화
        Strategy.lstOrderInfo_Net = []

    
    # 체결가격 전략별 할당
    def allocSettlePrice(self, DATA):
        if DATA['미체결수량'] == 0:
            for i in Strategy.lstOrderInfo:
                if i['PRODUCT_CODE'] == DATA['종목코드']:
                    i['SETTLE_PRICE'] == DATA['체결단가']
                    break
            if i == Strategy.lstOrderInfo[-1]:
                self.writeOrder2DB()
    

    # 전략별 거래내역 DB에 기록
    def writeOrder2DB(self):
        strQuery = 'INSERT INTO ... VALUES()'
        


    # 이하 출력부분
    def setTwBalanceInfoUI(self, DATA):
        for i in DATA:
            nRowCnt = self.wndIndi.twBalanceInfo.rowCount()
            self.wndIndi.twBalanceInfo.insertRow(nRowCnt)
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 0, QTableWidgetItem('test'))


    def setTwOrderInfoUI(self, DATA=None):
        if DATA == None:
            for i in Strategy.lstOrderInfo:
                if i['QUANTITY'] > 0:
                    d = '매수'
                elif i['QUANTITY'] < 0:
                    d = '매도'
                nRowCnt = self.wndIndi.twOrderInfo.rowCount()
                self.wndIndi.twOrderInfo.insertRow(nRowCnt)
                self.wndIndi.twOrderInfo.setItem(nRowCnt, 0, QTableWidgetItem('요청'))
                self.wndIndi.twOrderInfo.setItem(nRowCnt, 1, QTableWidgetItem(str(i['OCCUR_TIME'])))
                self.wndIndi.twOrderInfo.setItem(nRowCnt, 2, QTableWidgetItem(i['STRATEGY_NAME']))
                self.wndIndi.twOrderInfo.setItem(nRowCnt, 3, QTableWidgetItem(i['PRODUCT_CODE']))
                self.wndIndi.twOrderInfo.setItem(nRowCnt, 4, QTableWidgetItem(d))
                self.wndIndi.twOrderInfo.setItem(nRowCnt, 5, QTableWidgetItem(str(abs(i['QUANTITY']))))
                self.wndIndi.twOrderInfo.setItem(nRowCnt, 6, QTableWidgetItem(str(i['ORDER_PRICE'])))
        else:
            for i in Strategy.lstOrderInfo_Net:
                if i['QUANTITY'] > 0:
                    d = '매수'
                elif i['QUANTITY'] < 0:
                    d = '매도'
                nRowCnt = self.wndIndi.twSettleInfo.rowCount()
                self.wndIndi.twOrderInfo.insertRow(nRowCnt)
                self.wndIndi.twOrderInfo.setItem(nRowCnt, 0, QTableWidgetItem('주문'))
                self.wndIndi.twOrderInfo.setItem(nRowCnt, 1, QTableWidgetItem(str(i['OCCUR_TIME'])))
                self.wndIndi.twOrderInfo.setItem(nRowCnt, 2, QTableWidgetItem(DATA['주문번호']))
                self.wndIndi.twOrderInfo.setItem(nRowCnt, 3, QTableWidgetItem(i['PRODUCT_CODE']))
                self.wndIndi.twOrderInfo.setItem(nRowCnt, 4, QTableWidgetItem(d))
                self.wndIndi.twOrderInfo.setItem(nRowCnt, 5, QTableWidgetItem(str(abs(i['QUANTITY']))))
                self.wndIndi.twOrderInfo.setItem(nRowCnt, 6, QTableWidgetItem(''))
                
        self.wndIndi.twOrderInfo.resizeColumnsToContents()


    def setTwSettleInfoUI(self, DATA):
        for i in DATA:
            nRowCnt = self.wndIndi.twSettleInfo.rowCount()
            self.wndIndi.twSettleInfo.insertRow(nRowCnt)
            self.wndIndi.twSettleInfo.setItem(nRowCnt, 0, QTableWidgetItem('체결'))
            self.wndIndi.twSettleInfo.setItem(nRowCnt, 1, QTableWidgetItem(i['체결시간']))
            self.wndIndi.twSettleInfo.setItem(nRowCnt, 2, QTableWidgetItem(i['주문번호']))
            self.wndIndi.twSettleInfo.setItem(nRowCnt, 3, QTableWidgetItem(i['종목코드']))
            if i['매매구분'] == '01':
                d = '매도'
            elif i['매매구분'] == '02':
                d = '매수'
            self.wndIndi.twSettleInfo.setItem(nRowCnt, 4, QTableWidgetItem(d))
            self.wndIndi.twSettleInfo.setItem(nRowCnt, 5, QTableWidgetItem(i['체결수량']))
            self.wndIndi.twSettleInfo.setItem(nRowCnt, 6, QTableWidgetItem(i['체결단가']))

        self.wndIndi.twSettleInfo.resizeColumnsToContents()


    def setTwStrategyInfoUI(self):
        self.wndIndi.twStrategyInfo.setRowCount(0)  # 기존 내용 삭제
        for i in Strategy.dfStrategyInfo.index:
            nRowCnt = self.wndIndi.twStrategyInfo.rowCount()
            self.wndIndi.twStrategyInfo.insertRow(nRowCnt)

            chBox = QCheckBox()
            chBox.setCheckState(int(Strategy.dfStrategyInfo['USE'][i] * 2)) # 2: Checked, 0: Not Checked
            chBox.setEnabled(False) # 일단 비활성화

            self.wndIndi.twStrategyInfo.setItem(nRowCnt, 0, QTableWidgetItem(Strategy.dfStrategyInfo['NAME'][i]))
            self.wndIndi.twStrategyInfo.setCellWidget(nRowCnt, 1, chBox)
            self.wndIndi.twStrategyInfo.setItem(nRowCnt, 2, QTableWidgetItem(Strategy.dfStrategyInfo['TIMEFRAME'][i]))
            self.wndIndi.twStrategyInfo.setItem(nRowCnt, 3, QTableWidgetItem(str(Strategy.dfStrategyInfo['TR_UNIT'][i])))

        self.wndIndi.twStrategyInfo.resizeColumnsToContents()

    
    def setTwProductInfoUI(self, PriceInfo):

        nRowCnt = self.wndIndi.twProductInfo.rowCount()
        self.wndIndi.twProductInfo.insertRow(nRowCnt)

        t = str(PriceInfo['체결시간'])
        # self.wndIndi.twProductInfo.setItem(nRowCnt, 0, QTableWidgetItem(str(PriceInfo['영문종목명']).split("'")[1]))   # 영문종목명
        self.wndIndi.twProductInfo.setItem(nRowCnt, 0, QTableWidgetItem(t[2:4] + ":" + t[4:6] + ":" + t[6:8]))  # 체결시간
        # self.wndIndi.twProductInfo.setItem(nRowCnt, 2, QTableWidgetItem(str(PriceInfo['상한가']))) # 상한가
        # self.wndIndi.twProductInfo.setItem(nRowCnt, 3, QTableWidgetItem(str(PriceInfo['하한가']))) # 히힌기
        # self.wndIndi.twProductInfo.setItem(nRowCnt, 4, QTableWidgetItem(str(PriceInfo['전일종가]))) # 전일종가
        self.wndIndi.twProductInfo.setItem(nRowCnt, 1, QTableWidgetItem(str(PriceInfo['시가']))) # 시가
        self.wndIndi.twProductInfo.setItem(nRowCnt, 2, QTableWidgetItem(str(PriceInfo['고가']))) # 고가
        self.wndIndi.twProductInfo.setItem(nRowCnt, 3, QTableWidgetItem(str(PriceInfo['저가']))) # 저가
        self.wndIndi.twProductInfo.setItem(nRowCnt, 4, QTableWidgetItem(str(PriceInfo['현재가']))) # 현재가
        self.wndIndi.twProductInfo.setItem(nRowCnt, 5, QTableWidgetItem(str(PriceInfo['단위체결량'])))    # 단위체결량
        self.wndIndi.twProductInfo.setItem(nRowCnt, 6, QTableWidgetItem(str(PriceInfo['누적거래량'])))   # 누적거래량

        if len(t) == 9: # b'hhmmss'
            self.wndIndi.twProductInfo.resizeColumnsToContents()
        #     self.wndIndi.twProductInfo.resizeRowsToContents()

        self.wndIndi.twProductInfo.scrollToItem(self.wndIndi.twProductInfo.item(nRowCnt, 0))    # Scroll to end row


    def setSysMsgOnStatusBar(self, MsgID, moduleName):
        # 재연결 실패, 재접속 실패, 공지 등의 수동으로 재접속이 필요할 경우에는 이벤트는 발생하지 않고 신한i Expert Main에서 상태를 표현해준다.
        moduleName = moduleName.split('\\')[-1]
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
        self.wndIndi.statusbar.showMessage(MsgStr)
        self.wndIndi.statusbar.repaint()


    def setErrMsgOnStatusBar(self, ErrState, ErrCode, ErrMsg, moduleName):
        if ErrState == 0:
            strMsg = '정상'
        elif ErrState == 1:
            strMsg = '통신 오류'
        elif ErrState == 2:
            strMsg = '업무 오류'

        self.wndIndi.statusbar.showMessage('TR상태: ' + strMsg + ' / 에러코드: ' + ErrCode + ' / 메시지: ' + ErrMsg + ' / 모듈: ' + moduleName.split('\\')[-1])
        self.wndIndi.statusbar.repaint()