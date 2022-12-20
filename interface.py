import importlib
import pandas as pd
import datetime as dt
import time
import logging
from datetime import timedelta

# from PyQt5.QAxContainer import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import QEventLoop
from PyQt5.QtCore import QTime, QTimer

from SHi_indi.account import Account
from SHi_indi.price import Price
from SHi_indi.priceRT import PriceRT
from SHi_indi.order import Order
from SHi_indi.balance import Balance

from DB.dbconn import oracleDB
from System.strategy import Strategy



class Interface():

    def __init__(self, IndiWindow):
        
        # Global settings
        self.wndIndi = IndiWindow
        self.boolSysReady = False
        self.event_loop = QEventLoop()
        self.strStrategyPath = 'System.Strategy'
        self.strStrategyClass = 'System'
        self.strSettleCrncy = 'KRW'
        self.instDB = oracleDB('oraDB1')
        self.dtToday = dt.datetime.now().date()
        self.strToday = self.dtToday.strftime('%Y%m%d')
        self.dtT_1 = None
        self.strT_1 = ''

        # Local settings
        self.lstObj_Strategy = []
        self.dfAcntInfo = pd.DataFrame(None)
        self.strAcntCode = ''
        self.strProductCode = ''
        self.dfPositionT_1 = pd.DataFrame(None)
        self.lstChkBox = []

        # Log
        logger = logging.getLogger()    # 로그 생성
        logger.setLevel(logging.INFO)   # 로그의 출력 기준 설정
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')   # log 출력 형식
        stream_handler = logging.StreamHandler()    # log 출력
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        file_handler = logging.FileHandler('Log\\' + self.strToday + '.log')    # log를 파일에 출력
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Init. proc.
        self.userEnv = Account(self)
        self.price = Price(self)    # 정보(Historical) 시세 조회용
        self.priceRT = PriceRT(self)    # 실시간(RT) 시세 조회용
        self.objOrder = Order(self) # 주문
        self.objBalance = Balance(self) # 잔고 조회
        if self.userEnv.userLogin():    # 로그인
            if not self.boolSysReady:
                self.event_loop.exec_()
            
            self.userEnv.setAccount()
            self.price.rqProductMstInfo("cfut_mst") # 상품선물 전종목 정보 (-> setNearMonth)
            
            Strategy.__init__()
            self.initDate()
            self.initAcntInfo()
            self.initStrategyInfo() # 초기 전략 세팅
            
            # Events
            self.wndIndi.cbAcntCode.currentIndexChanged.connect(self.initAcntInfo)    # 종목코드 변경
            self.wndIndi.cbProductCode.currentIndexChanged.connect(self.initStrategyInfo)    # 종목코드 변경
            # self.wndIndi.pbRqPrice.clicked.connect(self.pbRqPrice)    # 시세 요청 버튼 클릭
            self.wndIndi.pbRunStrategy.clicked.connect(self.pbRunStrategy)   # 전략 실행 버튼 클릭

        # Scheduling
        self.qtTarget = QTime(15, 44, 0)
        self.timer = QTimer()
        self.timer.timeout.connect(self.lastProc)
        self.timer.start(1000)


    def initDate(self):
        strQuery = 'SELECT DISTINCT base_date FROM  market_data ORDER BY base_date DESC'
        df = self.instDB.query_to_df(strQuery, 2)
        if df['BASE_DATE'][0].date() == self.dtToday:
            self.dtT_1 = df['BASE_DATE'][1].date()
        else:
            self.dtT_1 = df['BASE_DATE'][0].date()
        self.strT_1 = self.dtT_1.strftime('%Y%m%d')
        Strategy.strToday = self.strToday
        Strategy.strT_1 = self.strT_1


    def initAcntInfo(self):
        self.wndIndi.twPositionInfo.setRowCount(0)  # 기존 내용 삭제
        self.wndIndi.twStrategyInfo.setRowCount(0)
        if self.wndIndi.cbAcntCode.currentText() == '':
            self.userEnv.setAccount()
            self.event_loop.exec_()
        self.strAcntCode = self.wndIndi.cbAcntCode.currentText()
        self.dfAcntInfo = self.userEnv.getAccount(self.strAcntCode)

        self.objBalance.rqBalance(self.strAcntCode, self.dfAcntInfo['Acnt_Pwd'][0])


    def initStrategyInfo(self):
        self.wndIndi.twStrategyInfo.setRowCount(0)  # 기존 내용 삭제
        self.lstChkBox = []
        if self.wndIndi.cbProductCode.currentText() == '':
            self.price.rqProductMstInfo("cfut_mst") # 상품선물 전종목 정보 (-> setNearMonth)
            self.event_loop.exec_()
        self.strProductCode = self.wndIndi.cbProductCode.currentText()
        Strategy.setStrategyInfo(self.strProductCode)
        if len(Strategy.dfStrategyInfo) != 0:
            for i in Strategy.dfStrategyInfo.index:
                self.lstChkBox.append(QCheckBox())
                self.lstChkBox[i].setCheckState(int(Strategy.dfStrategyInfo['USE'][i] * 2)) # 2: Checked, 0: Not checked
                self.lstChkBox[i].toggled.connect(self.chkbox_toggled)
            self.setTwStrategyInfoUI()  # 전략 세팅값 확인(UI)
            self.initPosition()


    def chkbox_toggled(self):
        for i, v in enumerate(self.lstChkBox):
            if v.isChecked():
                Strategy.dfStrategyInfo.loc[i, 'USE'] = '1'
            else:
                Strategy.dfStrategyInfo.loc[i, 'USE'] = '0'


    def setNearMonth(self):
        df = Strategy.dfCFutMst.drop_duplicates(subset=['단축코드'])
        for i in df['단축코드']:
            self.wndIndi.cbProductCode.addItem(i)


    def initPosition(self):
        if len(Strategy.dfStrategyInfo) != 0:
            lstAssetCode = []
            for i in Strategy.dfStrategyInfo['ASSET_CODE']: # 전략에 사용된 자산 리스트
                tmp = i.split(',')
                for j in tmp:
                    lstAssetCode.append(j.strip())
            strAssetCode = str(set(lstAssetCode))
            strAssetCode = strAssetCode.replace('{', '(')
            strAssetCode = strAssetCode.replace('}', ')')
            strQuery = f"SELECT position.*, pos_direction*pos_amount AS position FROM position WHERE base_date = '{self.dtT_1}' AND asset_code IN {strAssetCode}"
            self.dfPositionT_1 = self.instDB.query_to_df(strQuery, 100)
            if len(self.dfPositionT_1) != 0:
                Strategy.dfPosition = self.dfPositionT_1.copy()
                self.setTwPositionInfoUI()


    def pbRunStrategy(self):
        self.createStrategy()   # 1. 전략 생성 & 실행(최초 1회)
        self.pbRqPrice()    # 2. 실시간 시세 수신

        # self.objBalance.rqBalanceRT()   # 실시간 잔고 요청


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
            i.createHistData(self)

        start = time.process_time()
        for i in self.lstObj_Strategy:  # 전략 실행 (최초 1회)
            i.execute(0)
        end = time.process_time()
        logging.info('Time elapsed(1st run): %s', timedelta(seconds=end-start))

        self.orderStrategy()    # 접수된 주문 실행


    # 2. 실시간 시세 수신
    def pbRqPrice(self):
        self.wndIndi.twProductInfo.setRowCount(0)   # 기존 내용 삭제
        self.priceRT.startTR()    # 시세 수신


    # 3. 전략 실행 (실시간)
    def executeStrategy(self, PriceInfo):
        Strategy.chkPrice(self.price, PriceInfo)  # 분봉 완성 check

        start = time.process_time()
        for i in self.lstObj_Strategy:
            i.execute(PriceInfo) # 3. 전략 실행
        end = time.process_time()
        # logging.info('Time elapsed: %s', timedelta(seconds=end-start))
        
        self.orderStrategy(PriceInfo)    # 4. 접수된 주문 실행


    def lastProc(self): # 종가 주문 등 당일 마지막 처리
        now = QTime.currentTime()
        if now >= self.qtTarget:
            for i in self.lstObj_Strategy:
                try:
                    i.lastProc()
                except:
                    pass

            self.orderStrategy()    # 접수된 주문 실행
            self.timer.stop()


    # 주문 실행
    def orderStrategy(self, PriceInfo=None):
        if Strategy.lstOrderInfo != []:    # 주문할게 있으면
            Strategy.executeOrder(self, PriceInfo)  # 주문하고
            self.objOrder.startSettleRT(self.strAcntCode)  # 실시간 체결 수신
            self.objBalance.startBalanceRT(self.strAcntCode)    # 실시간 잔고 요청
            
            # 임시
            Strategy.lstOrderInfo = []  # 주문내역 초기화
            Strategy.lstOrderInfo_Net = []


    # 실시간 체결정보 확인
    def setSettleInfo(self, DATA):
        for i in Strategy.lstOrderInfo_Net:
            if i['PRODUCT_CODE'] == DATA['종목코드']:
                i['AVG_PRICE'] = (i['AVG_PRICE'] * i['SETTLE_QTY'] + DATA['체결가격'] * DATA['체결수량']) / (i['SETTLE_QTY'] + DATA['체결수량'])
                i['SETTLE_QTY'] += DATA['체결수량']
                self.setTwSettleInfoUI(DATA)

        if DATA['미체결수량'] == 0: # 전량 체결 완료시
            for i in Strategy.lstOrderInfo: # 전략별
                for j in Strategy.lstOrderInfo_Net: # 체결가격 할당
                    if i['PRODUCT_CODE'] == j['PRODUCT_CODE']:
                        i['SETTLE_PRICE'] == j['AVG_PRICE']
                        break
                
                for k in Strategy.dfPosition.index: # 포지션 현황 업데이트
                    if (i['STRATEGY_NAME'], i['ASSET_CODE'], i['MATURITY']) == \
                        (Strategy.dfPosition['STRATEGY_ID'][k], Strategy.dfPosition['ASSET_CODE'][k], Strategy.dfPosition['MATURITY'][k]):  # 전략명, 자산코드, 만기 일치
                        qty = Strategy.dfPosition['POS_DIRECTION'][k] * Strategy.dfPosition['POS_AMOUNT'][k] + i['QUANTITY']
                        Strategy.dfPosition['POS_DIRECTION'][k] = int(qty / abs(qty))
                        Strategy.dfPosition['POS_AMOUNT'][k] = abs(qty)
                        break

                    if k == Strategy.dfPosition.last_valid_index(): # 없으면 신규 추가
                        l = len(Strategy.dfPosition)
                        Strategy.dfPosition.loc[l]['BASE_DATE'] = self.dtToday
                        Strategy.dfPosition.loc[l]['STRATEGY_CLASS'] = self.strStrategyClass
                        Strategy.dfPosition.loc[l]['STRATEGY_ID'] = i['STRATEGY_NAME']
                        Strategy.dfPosition.loc[l]['ASSET_CLASS'] = i['ASSET_CLASS']
                        Strategy.dfPosition.loc[l]['ASSET_NAME'] = i['ASSET_NAME']
                        Strategy.dfPosition.loc[l]['ASSET_TYPE'] = i['ASSET_TYPE']
                        Strategy.dfPosition.loc[l]['MATURITY'] = i['MATURITY']
                        Strategy.dfPosition.loc[l]['SETTLE_CURNCY'] = self.strSettleCrncy
                        if DATA['매도매수구분'] == '02':
                            d = 1
                        elif DATA['매도매수구분'] == '01':
                            d = -1
                        Strategy.dfPosition.loc[l]['POS_DIRECTION'] = d
                        Strategy.dfPosition.loc[l]['POS_AMOUNT'] = i['SETTLE_QTY']

                self.inputOrder2DB(i)    # DB에 쓰기

            self.setTwPositionInfoUI()  # 포지션 현황 출력
            
            Strategy.lstOrderInfo = []  # 주문내역 초기화
            Strategy.lstOrderInfo_Net = []

    

    # 전략별 거래내역 DB에 기록
    def inputOrder2DB(self, orderInfo):
        strQuery = f"SELECT COUNT(*) FROM transactions WHERE base_datetime LIKE '{self.dtToday}%'"
        ret = Strategy.instDB.query_to_df(strQuery, 1)
        strTRnum = self.strStrategyClass[:1]    # ex) 'S'
        strTRnum += self.strToday   # Syyyymmdd ex) 'S20221103'
        strTRnum += '_' + format(int(ret[0][0])+1, '04')    # Syyyymmdd_xxxx ex) 'S20221103_0012'

        dictTrInfo = {}
        dictTrInfo['BASE_DATETIME'] = self.dtToday + ' ' + orderInfo['OCCUR_TIME']
        dictTrInfo['STRATEGY_CLASS'] = self.strStrategyClass
        dictTrInfo['TR_NUMBER'] = strTRnum
        dictTrInfo['STRATEGY_ID'] = orderInfo['STRATEGY_NAME']
        dictTrInfo['ASSET_CLASS'] = orderInfo['ASSET_CLASS']
        dictTrInfo['ASSET_NAME'] = orderInfo['ASSET_NAME']
        dictTrInfo['ASSET_TYPE'] = orderInfo['ASSET_TYPE']
        dictTrInfo['MATURITY'] = orderInfo['MATURITY']
        dictTrInfo['UNDERLYING_ID'] = orderInfo['UNDERLYING_ID']
        dictTrInfo['SETTLE_CURNCY'] = self.strSettleCrncy
        q = orderInfo['QUANTITY']
        dictTrInfo['TR_DIRECTION'] = int(q / abs(q))
        dictTrInfo['TR_AMOUNT'] = q
        dictTrInfo['TR_PRICE'] = orderInfo['SETTLE_PRICE']
        dictTrInfo['TR_COST'] = 0
        dictTrInfo['FUND_CODE'] = self.strAcntCode

        # strQuery = "INSERT INTO transactions VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        strQuery = "INSERT INTO transactions VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15)"
        Strategy.instDB.executemany(strQuery, list(dictTrInfo.values()))
        Strategy.instDB.commit()
        
        # 입력값 확인
        strQuery = f"SELECT * FROM transactions WHERE tr_number='{strTRnum}'"
        print(Strategy.instDB.query_to_df(strQuery, 1))


    # 이하 출력부분
    def setTwBalanceInfoUI(self, DATA):
        for i in DATA:
            nRowCnt = self.wndIndi.twBalanceInfo.rowCount()
            self.wndIndi.twBalanceInfo.insertRow(nRowCnt)
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 0, QTableWidgetItem('종목코드'))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 1, QTableWidgetItem('종목명'))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 2, QTableWidgetItem('매수매도구분'))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 3, QTableWidgetItem('당일잔고'))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 3, QTableWidgetItem('청산가능수량'))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 4, QTableWidgetItem('평균단가'))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 5, QTableWidgetItem('미체결수량'))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 6, QTableWidgetItem('평가손익'))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 7, QTableWidgetItem('수수료'))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 8, QTableWidgetItem('세금'))

        self.wndIndi.twBalanceInfo.resizeColumnsToContents()


    def setTwOrderInfoUI(self, DATA=None):
        if DATA == None:
            for i in Strategy.lstOrderInfo: # 전략별 주문 요청 내역
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
            for i in Strategy.lstOrderInfo_Net: # 실주문 내역
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
            self.wndIndi.twStrategyInfo.setItem(nRowCnt, 0, QTableWidgetItem(Strategy.dfStrategyInfo['NAME'][i]))
            self.wndIndi.twStrategyInfo.setCellWidget(nRowCnt, 1, self.lstChkBox[i])
            self.wndIndi.twStrategyInfo.setItem(nRowCnt, 2, QTableWidgetItem(Strategy.dfStrategyInfo['TIMEFRAME'][i]))
            self.wndIndi.twStrategyInfo.setItem(nRowCnt, 3, QTableWidgetItem(str(Strategy.dfStrategyInfo['TR_UNIT'][i])))
            self.wndIndi.twStrategyInfo.setItem(nRowCnt, 4, QTableWidgetItem(str(Strategy.dfStrategyInfo['WEIGHT'][i]*100)))

        self.wndIndi.twStrategyInfo.resizeColumnsToContents()


    def setTwPositionInfoUI(self):
        self.wndIndi.twPositionInfo.setRowCount(0)  # 기존 내용 삭제
        for i in Strategy.dfPosition.index:
            nRowCnt = self.wndIndi.twPositionInfo.rowCount()
            self.wndIndi.twPositionInfo.insertRow(nRowCnt)

            self.wndIndi.twPositionInfo.setItem(nRowCnt, 0, QTableWidgetItem(Strategy.dfPosition['STRATEGY_ID'][i]))   # 전략명
            self.wndIndi.twPositionInfo.setItem(nRowCnt, 1, QTableWidgetItem(Strategy.dfPosition['ASSET_NAME'][i]))    # 자산명
            self.wndIndi.twPositionInfo.setItem(nRowCnt, 2, QTableWidgetItem(Strategy.dfPosition['ASSET_TYPE'][i]))    # 자산구분
            self.wndIndi.twPositionInfo.setItem(nRowCnt, 3, QTableWidgetItem(str(Strategy.dfPosition['POSITION'][i])))  # 포지션
        
        self.wndIndi.twPositionInfo.resizeColumnsToContents()

    
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
            if moduleName == 'order.py':
                self.boolSysReady = True
                self.event_loop.exit()
        else:
            MsgStr = "System Message Received in module '" + moduleName + "' = " + str(MsgID)
        # print(MsgStr)
        self.wndIndi.statusbar.showMessage(MsgStr)
        self.wndIndi.statusbar.repaint()
        logging.info('%s', MsgStr)


    def setErrMsgOnStatusBar(self, ErrState, ErrCode, ErrMsg, moduleName):
        if ErrState == 0:
            strMsg = '정상'
        elif ErrState == 1:
            strMsg = '통신 오류'
        elif ErrState == 2:
            strMsg = '업무 오류'

        self.wndIndi.statusbar.showMessage('TR상태: ' + strMsg + ' / 에러코드: ' + ErrCode + ' / 메시지: ' + ErrMsg + ' / 모듈: ' + moduleName.split('\\')[-1])
        self.wndIndi.statusbar.repaint()
        logging.info('TR상태: %s, 에러코드: %s, 메시지: %s, 모듈: %s', strMsg, ErrMsg, moduleName.split('\\')[-1])