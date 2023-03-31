import importlib
import numpy as np
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
        self.strTR_MST = 'cfut_mst'
        self.lstChkBox = []
        self.lstObj_Strategy = []
        self.strAcntCode = ''
        self.dfAcntInfo = pd.DataFrame(None)
        self.strProductCode = ''
        self.dfPositionT_1 = pd.DataFrame(None)

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
            self.price.rqProductMstInfo(self.strTR_MST) # 상품선물 전종목 정보 (-> setNearMonth)
            self.event_loop.exec_()
            
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
        strQuery = 'SELECT base_date from (SELECT DISTINCT base_date FROM  market_data ORDER BY base_date DESC) WHERE ROWNUM >= 1 AND ROWNUM <= 2'
        df = self.instDB.query_to_df(strQuery, 1)
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
        
        # 종목코드가 있는 상태에서 펀드코드 변경시에도 전략 세팅될 수 있도록 - 김우일 차장 (23.03.15)
        if self.wndIndi.cbProductCode.currentText() != '':
            self.lstChkBox = []
            self.strProductCode = self.wndIndi.cbProductCode.currentText()
            Strategy.setStrategyInfo(self.strProductCode)
            if len(Strategy.dfStrategyInfo) != 0:
                for i in Strategy.dfStrategyInfo.index:
                    self.lstChkBox.append(QCheckBox())
                    self.lstChkBox[i].setCheckState(int(Strategy.dfStrategyInfo['use'][i] * 2)) # 2: Checked, 0: Not checked
                    self.lstChkBox[i].toggled.connect(self.chkbox_toggled)
                self.setTwStrategyInfoUI()  # 전략 세팅값 확인(UI)
                self.initPosition()
            self.initBalance()


    def initStrategyInfo(self):
        self.wndIndi.twStrategyInfo.setRowCount(0)  # 기존 내용 삭제
        self.lstChkBox = []
        if self.wndIndi.cbProductCode.currentText() == '':
            self.price.rqProductMstInfo(self.strTR_MST) # 상품선물 전종목 정보 (-> setNearMonth)
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
        self.initBalance()


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
        if not Strategy.dfStrategyInfo.empty:
            lstAssetCode = []
            for i in Strategy.dfStrategyInfo['ASSET_CODE']: # 전략에 사용된 자산 리스트
                tmp = i.split(',')
                for j in tmp:
                    lstAssetCode.append(j.strip())
            assetCode = str(tuple(set(lstAssetCode)))
            assetCode = assetCode.split(',)')[0].split(')')[0] + ')'
            # 당일자 포지션 있는지 조회
            strQuery = f"SELECT position.*, pos_direction*pos_amount AS position FROM position WHERE base_date = '{self.dtToday}' AND asset_code IN {assetCode}"
            self.dfPositionT_1 = self.instDB.query_to_df(strQuery, 100)
            if self.dfPositionT_1.empty:    # 없으면 전일자 조회
                strQuery = f"SELECT position.*, pos_direction*pos_amount AS position FROM position WHERE base_date = '{self.dtT_1}' AND asset_code IN {assetCode}"
                self.dfPositionT_1 = self.instDB.query_to_df(strQuery, 100)
            # # 당일 거래내역 조회
            # lstStrategyID = Strategy.dfStrategyInfo['NAME']
            # strQuery = f"SELECT * FROM transactions WHERE base_datetime >= '{self.dtToday}' AND fund_code = {self.strAcntCode} AND strategy_id IN {tuple(lstStrategyID)} AND asset_code IN {assetCode}"
            # df = self.instDB.query_to_df(strQuery, 100)
            
            # # TODO: 기존 포지션(self.dfPositionT_1)과 당일 거래내역(df)을 merge해서 포지션 보여줘야 함 
            if not self.dfPositionT_1.empty:
                Strategy.dfPosition = self.dfPositionT_1.copy()
                self.setTwPositionInfoUI()


    def initBalance(self):
        self.objBalance.rqBalance(self.strAcntCode, self.dfAcntInfo['Acnt_Pwd'].values[0])  # 현재 잔고 조회


    def pbRunStrategy(self):
        Strategy.nOrderCnt = 0
        # self.objBalance.startBalanceRT(self.strAcntCode)    # 실시간 잔고 요청 (작동 안됨)
        self.timerBalance = QTimer()
        
        self.createStrategy()   # 1. 전략 생성 & 실행(최초 1회)
        self.pbRqPrice()    # 2. 실시간 시세 수신

        self.timerBalance.start(5000)   # 5초마다 잔고조회
        self.timerBalance.timeout.connect(self.initBalance)


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

        # for i in self.lstObj_Strategy:  # 전략별 과거 데이터 세팅
        #     i.createHistData(self)

        start = time.process_time()
        for i in self.lstObj_Strategy:  # 전략 실행 (최초 1회)
            i.execute(0)
        end = time.process_time()
        logging.info('Time elapsed(1st run): %s', timedelta(seconds=end-start))

        self.orderStrategy()    # 접수된 주문 실행
        # self.event_loop.exec_()


    # 2. 실시간 시세 수신
    def pbRqPrice(self):
        self.wndIndi.twProductInfo.setRowCount(0)   # 기존 내용 삭제
        self.priceRT.startTR()    # 시세 수신


    # 3. 전략 실행 (실시간)
    def executeStrategy(self, PriceInfo):
        # Strategy.chkPrice(self, PriceInfo)  # 분봉 완성 check
        Strategy.setPrice(PriceInfo)  # 실시간 가격 정보 세팅
        
        start = time.process_time()
        for i in self.lstObj_Strategy:
            i.execute(PriceInfo) # 3. 전략 실행
        end = time.process_time()
        # logging.info('Time elapsed: %s', timedelta(seconds=end-start))
        
        self.orderStrategy(PriceInfo)    # 4. 접수된 주문 실행
        # self.chkStop(PriceInfo) # 5. 실시간 손익 직접 계산


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
            Strategy.dictOrderInfo[Strategy.nOrderCnt] = Strategy.lstOrderInfo.copy()   # 주문내역 별도 보관(넘버링)
            Strategy.dictOrderInfo_Net[Strategy.nOrderCnt] = Strategy.lstOrderInfo_Net.copy()
            Strategy.lstOrderInfo.clear()   # 주문내역 초기화
            Strategy.lstOrderInfo_Net.clear()
            Strategy.nOrderCnt += 1

            if Strategy.nOrderCnt == 1: # 최초 주문후
                self.objOrder.startSettleRT(self.strAcntCode)  # 실시간 체결 수신
            

    # Realtime PL check! (실시간 잔고 수신 안됨)
    def chkPL(self, DATA):
        if DATA['종목코드'] != '':
            strUnder_ID = Strategy.dfCFutMst['기초자산ID'][Strategy.dfCFutMst['단축코드']==DATA['종목코드']].values[0]
            threadshold = Strategy.dfProductInfo['threadshold_loss'][Strategy.dfProductInfo['UNDERLYING_ID']==strUnder_ID].values[0]
            if float(DATA['평가손익']) < threadshold:
                d = int(DATA['매수매도구분']) * 2 - 3
                if d == 1:
                    Strategy.setOrder('LossCut', DATA['종목코드'], 'S', int(DATA['청산가능수량']), 0)
                elif d == -1:
                    Strategy.setOrder('LossCut', DATA['종목코드'], 'B', int(DATA['청산가능수량']), 0)


    # Realtime PL check! (직접 계산)
    def chkStop(self, PriceInfo):
        if not Strategy.dfPosition.empty:
            strUnder_ID = Strategy.dfCFutMst['기초자산ID'][Strategy.dfCFutMst['단축코드']==PriceInfo['단축코드'].decode()].values[0]
            asset_code = Strategy.dfStrategyInfo['ASSET_CODE'][Strategy.dfStrategyInfo['UNDERLYING_ID']==strUnder_ID].values[0]
            dfTarget = Strategy.dfPosition[Strategy.dfPosition['ASSET_CODE']==asset_code]
            if not dfTarget.empty:
                threadshold = Strategy.dfProductInfo['THREADSHOLD_LOSS'][Strategy.dfProductInfo['UNDERLYING_ID']==strUnder_ID].values[0]
                pl = ((float(PriceInfo['현재가']) - np.array(dfTarget['POS_PRICE'])) * np.array(dfTarget['POSITION'])).sum()
                if pl < threadshold:    # Loss cut
                    q = dfTarget['POSITION'].sum()
                    if q < 0:
                        Strategy.setOrder('LossCut', PriceInfo['단축코드'].decode(), 'B', abs(q), 0)
                    elif q > 0:
                        Strategy.setOrder('LossCut', PriceInfo['단축코드'].decode(), 'S', abs(q), 0)


    def chkStop2(self, lstDATA):
        for i in lstDATA:
            strUnder_ID = Strategy.dfCFutMst['기초자산ID'][Strategy.dfCFutMst['단축코드']==i['단축코드']].values[0]
            threadshold = Strategy.dfProductInfo['threadshold_loss'][Strategy.dfProductInfo['UNDERLYING_ID']==strUnder_ID].values[0]
            if float(i['평가손익']) < threadshold:
                d = int(i['매매구분']) * 2 - 3
                if d == 1:
                    Strategy.setOrder('LossCut', i['단축코드'], 'S', int(i['청산가능수량']), 0)
                elif d == -1:
                    Strategy.setOrder('LossCut', i['단축코드'], 'B', int(i['청산가능수량']), 0)


    # 실시간 체결정보 확인
    # def setSettleInfo(self, DATA):
    #     if Strategy.dictOrderInfo_Rcv[DATA['주문번호']] == None:   # 첫 체결
    #         if int(DATA['미체결수량']) == 0:    # 전량 체결
    #             dictOrderInfo_Net = dict(sorted(Strategy.dictOrderInfo_Net.items(), reverse=True))  # 가장 나중 주문부터
    #             settleData = [DATA['종목코드'], (int(DATA['매도매수구분'])*2-3) * int(DATA['주문수량']), float(DATA['주문단가'])]
    #             for i in dictOrderInfo_Net:
    #                 for j in dictOrderInfo_Net[i]:  # j == Strategy.lstOrderInfo_Net
    #                     orderData = [j['PRODUCT_CODE'], j['QUANTITY'], j['PRICE']]
    #                     if np.array_equal(settleData, orderData):   # 종목코드, 수량(방향), 주문가격 일치하면
    #                         lstOrderInfo = Strategy.dictOrderInfo[i]
    #                         for k in lstOrderInfo:
    #                             k['SETTLE_PRICE'] = float(DATA['체결단가'])
    #                         self.updatePosition(lstOrderInfo)   # 포지션 업데이트
    #                         self.inputOrder2DB(lstOrderInfo)    # DB에 쓰기
    #                         break
    #                 if j < len(dictOrderInfo_Net[i])-1:
    #                     break
    #         else:   # TODO: 일부만 체결된 경우 처리 (미체결은 남은 경우)
    #             Strategy.dictOrderInfo_Rcv[int(DATA['주문번호'])] == DATA.copy()
    #             logging.warning('일부 체결 발생 1. 로직 완성해야 함')
    #     else:
    #         if int(DATA['미체결수량']) == 0:    # TODO: 일부만 체결되었다가 나머지 잔량 체결된 경우 처리
    #             logging.warning('일부 체결 발생 2. 로직 완성해야 함')
    #         else:   # TODO: 일부만 체결된 경우 처리 (미체결은 남은 경우)
    #             logging.warning('일부 체결 발생 3. 로직 완성해야 함')

    #     logging.info('주문 체결')
    #     self.setTwSettleInfoUI(DATA)
        

    def setSettleInfo(self, DATA):
        if int(DATA['미체결수량']) == 0:    # 주문수량 전량 체결
            for i in Strategy.dictOrderInfo_Net:
                for j in Strategy.dictOrderInfo_Net[i]:
                    if j['주문번호'] == int(DATA['주문번호']):   # 주문번호로 주문내역 찾고
                        lstIDs = j['NET_ID'].split(',')
                        for k in lstIDs:    # 네팅되기 전 주문내역 찾아
                            lstOrderInfo = Strategy.dictOrderInfo[i]
                            ordInfo = lstOrderInfo[int(k)]
                            ordInfo['SETTLE_PRICE'] = float(DATA['체결단가']) # 체결정보 업데이트
                            self.updatePosition(ordInfo)    # 포지션 업데이트
                            self.inputOrder2DB(ordInfo) # 거래내역 DB에 쓰기
            self.setTwSettleInfoUI(DATA)
            logging.info('주문 체결')
            self.setTwPositionInfoUI()  # 포지션 현황 출력
            logging.info('포지션 업데이트')
            self.inputPos2DB()  # 포지션 DB에 쓰기
        else:
            # TODO: 일부 체결된 경우 처리 필요
            pass


    # 포지션 업데이트
    # def updatePosition(self, lstOrderInfo):
    #     for i in lstOrderInfo:
    #         if Strategy.dfPosition.empty:   # 처음이면 신규추가
    #             self.insertPosition(i)
    #         else:
    #             order = [i['STRATEGY_NAME'], i['ASSET_CODE'], i['MATURITY'], self.strAcntCode]
    #             for j in Strategy.dfPosition.index: # 포지션 현황 업데이트
    #                 pos = [Strategy.dfPosition['STRATEGY_ID'][j], Strategy.dfPosition['ASSET_CODE'][j], Strategy.dfPosition['MATURITY'][j], Strategy.dfPosition['FUND_CODE'][j]]
    #                 if np.array_equal(order, pos):  # 전략명, 자산코드, 만기, 계좌 일치
    #                     qty = Strategy.dfPosition['POSITION'][j] + i['QUANTITY']
    #                     if qty == 0:  # 수량이 0이 되면 목록에서 제외
    #                         Strategy.dfPosition = Strategy.dfPosition.drop(j, axis=0)
    #                     else:
    #                         Strategy.dfPosition.loc[j, 'POS_DIRECTION'] = int(qty / abs(qty))
    #                         Strategy.dfPosition.loc[j, 'POS_AMOUNT'] = abs(qty)
    #                         if abs(i['QUANTITY']) > abs(Strategy.dfPosition['POSITION'][j]):
    #                             Strategy.dfPosition.loc[j, 'POS_PRICE'] = i['SETTLE_PRICE']
    #                         Strategy.dfPosition.loc[j, 'POSITION'] = qty
    #                     break

    #                 if j == Strategy.dfPosition.last_valid_index(): # 없으면 신규 추가
    #                     self.insertPosition(i)

    #     logging.info('포지션 업데이트')
    #     self.setTwPositionInfoUI()  # 포지션 현황 출력


    def updatePosition(self, ordInfo):
        if Strategy.dfPosition.empty:   # 처음이면 신규추가
            self.insertPosition(ordInfo)
        else:
            if ordInfo['STRATEGY_NAME'] == 'LossCut':   # 손절에 의한 경우 처리
                order = [ordInfo['ASSET_CODE'], ordInfo['MATURITY'], self.strAcntCode]
                for i in Strategy.dfPosition.index: # 포지션 현황 업데이트
                    pos = [Strategy.dfPosition['ASSET_CODE'][i], Strategy.dfPosition['MATURITY'][i], Strategy.dfPosition['FUND_CODE'][i]]
                    if np.array_equal(order, pos):  # 자산코드, 만기, 계좌 일치
                        Strategy.dfPosition.loc[i, 'POS_DIRECTION'] = 0
                        Strategy.dfPosition.loc[i, 'POS_AMOUNT'] = 0
                        Strategy.dfPosition.loc[i, 'POS_PRICE'] = ordInfo['SETTLE_PRICE']
                        Strategy.dfPosition.loc[i, 'POSITION'] = 0
            else:
                order = [ordInfo['STRATEGY_NAME'], ordInfo['ASSET_CODE'], ordInfo['MATURITY'], self.strAcntCode]
                for i in Strategy.dfPosition.index: # 포지션 현황 업데이트
                    pos = [Strategy.dfPosition['STRATEGY_ID'][i], Strategy.dfPosition['ASSET_CODE'][i], Strategy.dfPosition['MATURITY'][i], Strategy.dfPosition['FUND_CODE'][i]]
                    if np.array_equal(order, pos):  # 전략명, 자산코드, 만기, 계좌 일치
                        qty = Strategy.dfPosition['POSITION'][i] + ordInfo['QUANTITY']
                        if qty == 0:    # 수량이 0 되면 목록에서 제외
                            Strategy.dfPosition = Strategy.dfPosition.drop(i, axis=0)
                        else:
                            Strategy.dfPosition.loc[i, 'POS_DIRECTION'] = int(qty / abs(qty))
                            Strategy.dfPosition.loc[i, 'POS_AMOUNT'] = abs(qty)
                            if abs(ordInfo['QUANTITY']) > abs(Strategy.dfPosition['POSITION'][i]):
                                Strategy.dfPosition.loc[i, 'POS_PRICE'] = ordInfo['SETTLE_PRICE']
                            Strategy.dfPosition.loc[i, 'POSITION'] = qty
                        break

                    if i == Strategy.dfPosition.last_valid_index(): # 없으면 신규 추가
                        self.insertPosition(ordInfo)
    

    def insertPosition(self, ordInfo):
        l = len(Strategy.dfPosition)
        q = int(ordInfo['QUANTITY'])
        Strategy.dfPosition.loc[l, 'BASE_DATE'] = self.dtToday
        Strategy.dfPosition.loc[l, 'STRATEGY_CLASS'] = self.strStrategyClass
        Strategy.dfPosition.loc[l, 'STRATEGY_ID'] = ordInfo['STRATEGY_NAME']
        Strategy.dfPosition.loc[l, 'ASSET_CLASS'] = ordInfo['ASSET_CLASS']
        Strategy.dfPosition.loc[l, 'ASSET_NAME'] = ordInfo['ASSET_NAME']
        Strategy.dfPosition.loc[l, 'ASSET_CODE'] = ordInfo['ASSET_CODE']
        Strategy.dfPosition.loc[l, 'ASSET_TYPE'] = ordInfo['ASSET_TYPE']
        Strategy.dfPosition.loc[l, 'MATURITY'] = ordInfo['MATURITY']
        Strategy.dfPosition.loc[l, 'SETTLE_CURNCY'] = self.strSettleCrncy
        Strategy.dfPosition.loc[l, 'POS_DIRECTION'] = q / abs(q)
        Strategy.dfPosition.loc[l, 'POS_AMOUNT'] = abs(q)
        Strategy.dfPosition.loc[l, 'POS_PRICE'] = ordInfo['SETTLE_PRICE']
        Strategy.dfPosition.loc[l, 'FUND_CODE'] = self.strAcntCode
        Strategy.dfPosition.loc[l, 'POSITION'] = q


    # 전략별 거래내역 DB에 기록
    # def inputOrder2DB(self, lstOrderInfo):
    #     for i in lstOrderInfo:
    #         strQuery = f"SELECT COUNT(*) AS cnt FROM transactions WHERE base_datetime LIKE '{self.dtToday}%'"
    #         ret = Strategy.instDB.query_to_df(strQuery, 1)
    #         strTRnum = self.strStrategyClass[:1]    # ex) 'S'
    #         strTRnum += self.dtToday.strftime('%y%m%d') # Syymmdd ex) 'S221103'
    #         strTRnum += '_' + format(int(ret[0]['cnt'])+1, '04')    # Syymmdd_xxxx ex) 'S221103_0012'

    #         dictTrInfo = {}
    #         dictTrInfo['BASE_DATETIME'] = self.dtToday.strftime('%Y-%m-%d') + ' ' + i['OCCUR_TIME'].strftime('%H:%M:%S')
    #         dictTrInfo['STRATEGY_CLASS'] = self.strStrategyClass
    #         dictTrInfo['TR_NUMBER'] = strTRnum
    #         dictTrInfo['STRATEGY_ID'] = i['STRATEGY_NAME']
    #         dictTrInfo['ASSET_CLASS'] = i['ASSET_CLASS']
    #         dictTrInfo['ASSET_NAME'] = i['ASSET_NAME']
    #         dictTrInfo['ASSET_CODE'] = i['ASSET_CODE']
    #         dictTrInfo['ASSET_TYPE'] = i['ASSET_TYPE']
    #         dictTrInfo['MATURITY'] = i['MATURITY']
    #         dictTrInfo['UNDERLYING_ID'] = i['UNDERLYING_ID']
    #         dictTrInfo['SETTLE_CURNCY'] = self.strSettleCrncy
    #         q = i['QUANTITY']
    #         dictTrInfo['TR_DIRECTION'] = int(q / abs(q))
    #         dictTrInfo['TR_AMOUNT'] = abs(q)
    #         dictTrInfo['TR_PRICE'] = i['SETTLE_PRICE']
    #         dictTrInfo['TR_COST'] = 0
    #         dictTrInfo['FUND_CODE'] = self.strAcntCode

    #         strQuery = f'INSERT INTO transactions VALUES {tuple(dictTrInfo.values())}'
    #         Strategy.instDB.execute(strQuery)
    #         Strategy.instDB.commit()


    def inputOrder2DB(self, ordInfo):
        strQuery = f"SELECT COUNT(*) AS cnt FROM transactions WHERE base_datetime >= '{self.dtToday}'"
        ret = Strategy.instDB.query_to_df(strQuery, 1)
        strTRnum = self.strStrategyClass[:1]    # ex) 'S'
        strTRnum += self.dtToday.strftime('%y%m%d') # Syymmdd ex) 'S221103'
        # strTRnum += '_' + format(int(ret['CNT'][0])+1, '04')    # Syymmdd_xxxx ex) 'S221103_0012'
        strTRnum += '_' + format(int(ret.loc[0, 'CNT'])+1, '04')    # Syymmdd_xxxx ex) 'S221103_0012'

        dictTrInfo = {}
        dictTrInfo['BASE_DATETIME'] = self.dtToday.strftime('%Y-%m-%d') + ' ' + ordInfo['OCCUR_TIME'].strftime('%H:%M:%S')
        dictTrInfo['STRATEGY_CLASS'] = self.strStrategyClass
        dictTrInfo['TR_NUMBER'] = strTRnum
        dictTrInfo['STRATEGY_ID'] = ordInfo['STRATEGY_NAME']
        dictTrInfo['ASSET_CLASS'] = ordInfo['ASSET_CLASS']
        dictTrInfo['ASSET_NAME'] = ordInfo['ASSET_NAME']
        dictTrInfo['ASSET_CODE'] = ordInfo['ASSET_CODE']
        dictTrInfo['ASSET_TYPE'] = ordInfo['ASSET_TYPE']
        dictTrInfo['MATURITY'] = ordInfo['MATURITY']
        dictTrInfo['UNDERLYING_ID'] = ordInfo['UNDERLYING_ID']
        dictTrInfo['SETTLE_CURNCY'] = self.strSettleCrncy
        q = ordInfo['QUANTITY']
        dictTrInfo['TR_DIRECTION'] = int(q / abs(q))
        dictTrInfo['TR_AMOUNT'] = abs(q)
        dictTrInfo['TR_PRICE'] = ordInfo['SETTLE_PRICE']
        dictTrInfo['TR_COST'] = 0
        dictTrInfo['FUND_CODE'] = self.strAcntCode

        strQuery = f'INSERT INTO transactions COLUMNS (base_datetime, strategy_class, tr_number, strategy_id, asset_class, asset_name, asset_code, asset_type, maturity, underlying_id, settle_curncy, tr_direction, tr_amount, tr_price, tr_cost, fund_code) VALUES {tuple(dictTrInfo.values())}'
        Strategy.instDB.execute(strQuery)
        Strategy.instDB.commit()


    # 포지션 현황 DB에 기록
    def inputPos2DB(self):
        if not Strategy.dfPosition.empty:
            self.instDB = oracleDB('oraDB1')
            lstAssetCode = []
            for i in Strategy.dfStrategyInfo['ASSET_CODE']: # 전략에 사용된 자산 리스트
                tmp = i.split(',')
                for j in tmp:
                    lstAssetCode.append(j.strip())
            assetCode = str(tuple(set(lstAssetCode)))
            assetCode = assetCode.split(',)')[0].split(')')[0] + ')'
            strQuery = f"SELECT position.*, pos_direction*pos_amount AS position FROM position WHERE base_date = '{self.dtToday}' AND asset_code IN {assetCode}"
            df = self.instDB.query_to_df(strQuery, 100)
            if not df.empty:    # 당일자로 이미 기록된 포지션이 있으면
                strQuery = f"DELETE FROM position WHERE base_date = '{self.dtToday}' AND asset_code IN {assetCode}"   # 지우고
                Strategy.instDB.execute(strQuery)
            # 쓰기
            Strategy.dfPosition['BASE_DATE'] = self.dtToday
            Strategy.instDB.executemany("INSERT INTO position COLUMNS (base_date, strategy_class, strategy_id, asset_class, asset_name, asset_code, asset_type, maturity, settle_curncy, pos_direction, pos_amount, pos_price, fund_code) VALUES (:1, :2, :3, :4, :5, :8, :6, :7, :9, :10, :11, :12, :13)", Strategy.dfPosition.drop('POSITION', axis=1).values.tolist())
            Strategy.instDB.commit()


    # 이하 출력부분
    def setTwBalanceInfoUI(self, DATA, isRT=None):
        # if type(DATA) == int:
        #     self.wndIndi.twBalanceInfo.setRowCount(0)   # 기존 내용 삭제
        #     for i in Strategy.dfPosition.index:
        #         nRowCnt = self.wndIndi.twBalanceInfo.rowCount()
        #         self.wndIndi.twBalanceInfo.insertRow(nRowCnt)
        #         self.wndIndi.twBalanceInfo.setItem(nRowCnt, 1, QTableWidgetItem(Strategy.dfPosition['ASSET_NAME'][i]))
        #         self.wndIndi.twBalanceInfo.setItem(nRowCnt, 2, QTableWidgetItem(str(Strategy.dfPosition['POS_DIRECTION'][i])))
        #         self.wndIndi.twBalanceInfo.setItem(nRowCnt, 4, QTableWidgetItem(str(Strategy.dfPosition['POS_AMOUNT'][i])))
        #         self.wndIndi.twBalanceInfo.setItem(nRowCnt, 5, QTableWidgetItem(str(Strategy.dfPosition['POS_PRICE'][i])))
        # else:
        if isRT:
            nRowCnt = self.wndIndi.twBalanceInfo.rowCount()
            self.wndIndi.twBalanceInfo.insertRow(nRowCnt)
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 0, QTableWidgetItem(DATA['종목코드']))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 1, QTableWidgetItem(DATA['종목명']))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 2, QTableWidgetItem(DATA['매수매도구분']))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 3, QTableWidgetItem(DATA['당일잔고']))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 4, QTableWidgetItem(DATA['청산가능수량']))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 5, QTableWidgetItem(DATA['평균단가']))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 6, QTableWidgetItem(DATA['미체결수량']))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 7, QTableWidgetItem(DATA['평가손익']))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 8, QTableWidgetItem(DATA['수수료']))
            self.wndIndi.twBalanceInfo.setItem(nRowCnt, 9, QTableWidgetItem(DATA['세금']))
        else:
            for i in DATA:
                self.wndIndi.twBalanceInfo.setRowCount(0)   # 기존 내용 삭제
                nRowCnt = self.wndIndi.twBalanceInfo.rowCount()
                self.wndIndi.twBalanceInfo.insertRow(nRowCnt)
                self.wndIndi.twBalanceInfo.setItem(nRowCnt, 0, QTableWidgetItem(i['단축코드']))
                self.wndIndi.twBalanceInfo.setItem(nRowCnt, 1, QTableWidgetItem(i['종목명']))
                self.wndIndi.twBalanceInfo.setItem(nRowCnt, 2, QTableWidgetItem(i['매매구분']))
                # self.wndIndi.twBalanceInfo.setItem(nRowCnt, 3, QTableWidgetItem(i['당일잔고']))
                self.wndIndi.twBalanceInfo.setItem(nRowCnt, 4, QTableWidgetItem(i['청산가능수량']))
                self.wndIndi.twBalanceInfo.setItem(nRowCnt, 5, QTableWidgetItem(i['평균단가']))
                self.wndIndi.twBalanceInfo.setItem(nRowCnt, 6, QTableWidgetItem(i['미체결수량']))
                self.wndIndi.twBalanceInfo.setItem(nRowCnt, 7, QTableWidgetItem(i['평가손익']))
                self.wndIndi.twBalanceInfo.setItem(nRowCnt, 8, QTableWidgetItem(i['수수료']))
                # self.wndIndi.twBalanceInfo.setItem(nRowCnt, 9, QTableWidgetItem(i['세금']))

        self.wndIndi.twBalanceInfo.resizeColumnsToContents()


    # def setTwOrderInfoUI(self, DATA=None):
    def setTwOrderInfoUI(self): # 주문내역 출력
        # if DATA == None:
        if len(Strategy.lstOrderInfo) > 0:
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
            # else:
        if len(Strategy.lstOrderInfo_Net) > 0:
            for i in Strategy.lstOrderInfo_Net: # 실주문 내역
                if i['QUANTITY'] != 0:
                    if i['QUANTITY'] > 0:
                        d = '매수'
                    elif i['QUANTITY'] < 0:
                        d = '매도'
                    nRowCnt = self.wndIndi.twOrderInfo.rowCount()
                    self.wndIndi.twOrderInfo.insertRow(nRowCnt)
                    self.wndIndi.twOrderInfo.setItem(nRowCnt, 0, QTableWidgetItem('주문'))
                    self.wndIndi.twOrderInfo.setItem(nRowCnt, 1, QTableWidgetItem(str(i['OCCUR_TIME'])))
                    # self.wndIndi.twOrderInfo.setItem(nRowCnt, 2, QTableWidgetItem(DATA['주문번호']))
                    # self.wndIndi.twOrderInfo.setItem(nRowCnt, 2, QTableWidgetItem(i['STRATEGY_NAME']))
                    self.wndIndi.twOrderInfo.setItem(nRowCnt, 3, QTableWidgetItem(i['PRODUCT_CODE']))
                    self.wndIndi.twOrderInfo.setItem(nRowCnt, 4, QTableWidgetItem(d))
                    self.wndIndi.twOrderInfo.setItem(nRowCnt, 5, QTableWidgetItem(str(abs(i['QUANTITY']))))
                    try:
                        self.wndIndi.twOrderInfo.setItem(nRowCnt, 6, QTableWidgetItem(str(i['PRICE'])))
                    except:
                        self.wndIndi.twOrderInfo.setItem(nRowCnt, 6, QTableWidgetItem('0'))
                
        self.wndIndi.twOrderInfo.resizeColumnsToContents()


    def setTwSettleInfoUI(self, DATA):  # 실시간 체결 출력
        nRowCnt = self.wndIndi.twSettleInfo.rowCount()
        self.wndIndi.twSettleInfo.insertRow(nRowCnt)
        self.wndIndi.twSettleInfo.setItem(nRowCnt, 0, QTableWidgetItem(DATA['주문번호']))
        self.wndIndi.twSettleInfo.setItem(nRowCnt, 1, QTableWidgetItem(DATA['체결시간']))
        self.wndIndi.twSettleInfo.setItem(nRowCnt, 2, QTableWidgetItem(DATA['종목코드']))
        self.wndIndi.twSettleInfo.setItem(nRowCnt, 3, QTableWidgetItem(DATA['종목명']))
        self.wndIndi.twSettleInfo.setItem(nRowCnt, 4, QTableWidgetItem(DATA['매매구분']))
        self.wndIndi.twSettleInfo.setItem(nRowCnt, 5, QTableWidgetItem(DATA['주문구분']))
        self.wndIndi.twSettleInfo.setItem(nRowCnt, 6, QTableWidgetItem(DATA['주문수량']))
        self.wndIndi.twSettleInfo.setItem(nRowCnt, 7, QTableWidgetItem(DATA['주문단가']))
        self.wndIndi.twSettleInfo.setItem(nRowCnt, 8, QTableWidgetItem(DATA['체결수량']))
        self.wndIndi.twSettleInfo.setItem(nRowCnt, 9, QTableWidgetItem(DATA['미체결수량']))
        self.wndIndi.twSettleInfo.setItem(nRowCnt, 10, QTableWidgetItem(DATA['체결단가']))

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
        # self.wndIndi.twProductInfo.setItem(nRowCnt, 0, QTableWidgetItem(str(PriceInfo['한글종목명']).split("'")[1]))   # 한글종목명
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
            if moduleName.startswith('order'):
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
        logging.info('TR상태: %s, 에러코드: %s, 메시지: %s, 모듈: %s', strMsg, ErrCode, ErrMsg, moduleName.split('\\')[-1])