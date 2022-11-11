from PyQt5.QAxContainer import *

from System.strategy import Strategy



class Order():

    def __init__(self, instInterface):
        self.instInterface = instInterface
        self.strCurrentAcnt = ''

        # 일반 TR OCX
        self.IndiTR = QAxWidget("GIEXPERTCONTROL.GiExpertControlCtrl.1")
        self.IndiReal = QAxWidget("GIEXPERTCONTROL.GiExpertControlCtrl.1")
        self.IndiTR.ReceiveData.connect(self.ReceiveData)
        self.IndiReal.ReceiveRTData.connect(self.ReceiveRTData)
        self.IndiTR.ReceiveSysMsg.connect(self.ReceiveSysMsg) # 일반 TR에 대한 응답을 받는 함수를 연결

        # TR ID를 저장해놓고 처리할 딕셔너리 생성
        self.rqidD = {}


    def order(self, acnt_num:str, pwd:str, code:str, qty, price, direction:str, condition:str='0', order_type:str='L', trading_type:str='3', treat_type:str='1', modify_type:str='0', origin_order_num=None, reserve_order=None):
        " 선물 주문을 요청한다."
        if Strategy.chkAbnormOrder(acnt_num, code, qty, price, direction):  # 주문 이상여부 체크
            self.ReceiveSysMsg('주문 거부. 이상주문 감지. 시스템을 확인하세요!')
            return False
        else:
            ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "SABC100U1")
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, acnt_num)  # 계좌번호
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, pwd)  # 비밀번호
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, code)  # 종목코드
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 3, str(qty))  # 주문수량
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 4, str(round(price, 2)))  # 주문단가 (소수점 2자리(양수:999.99, 음수:-999.99), RFR종목은 소수점 3자리)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 5, condition)  # 주문조건 (0:일반(FAS), 3:IOC(FAK), 4:FOK)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 6, direction)  # 매매구분 (01:매도, 02:매수, (정정/취소시):01)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 7, order_type)  # 호가유형 (L:지정가, M:시장가, C:조건부, B:최유리)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 8, trading_type)  # 차익거래구분 (1:차익, 2:헷지, 3:기타)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 9, treat_type)  # 처리구분 (1:신규, 2:정정, 3:취소)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 10, modify_type)  # 정정취소수량구분 (0:신규, 1:전부, 2:일부)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 11, origin_order_num)  # 원주문번호 (매수/매도시 생략가능)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 12, reserve_order)  # 예약주문여부 (생략가능, 1:예약)
            rqid = self.IndiTR.dynamicCall("RequestData()")  # 데이터 요청
            self.rqidD[rqid] = "SABC100U1"
            return True


    def startSettleRT(self, acnt_num):
        '선물/옵션 주문체결 실시간'
        if self.strCurrentAcnt != acnt_num:
            self.strCurrentAcnt = acnt_num
            ret = self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", "AB", self.strCurrentAcnt)


    def stopSettleRT(self, acnt_num):
        self.IndiReal.dynamicCall("UnRequestRTReg(QString, QString)", "AB", acnt_num)
        self.strCurrentAcnt = ''


    def iqrySettle(self, acnt_num:str, pwd:str, tr_date:str, product_type:str='1', boundary:str='0', iqry:str='1', sort:str='0', irqy_prd:str='0'):
        " 체결/미체결 내역조회"
        ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "SABC258Q1")
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, acnt_num)  # 계좌번호
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, pwd)  # 비밀번호
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, product_type)  # 상품구분(0: 전체, 1: 선물, 2:옵션(지수옵션+주식옵션), 3:주식옵션만)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 4, tr_date)  # 매매일자 ("YYYYMMDD")
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 5, boundary)  # 조회구분 (0: 전체, 1: 체결, 2:미체결)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 6, iqry)  # 합산구분 (0: 합산, 1: 건별)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 7, sort)  # Sort구분 (0: 주문번호순, 1: 주문번호역순)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 8, irqy_prd)  # 종목별합산구분 (0: 일반조회, 1: 종목별합산조회)
        rqid = self.IndiTR.dynamicCall("RequestData()")  # 조회 요청
        self.rqidD[rqid] = "SABC258Q1"


    def ReceiveData(self, rqid):
        TRName = self.rqidD[rqid]

        if TRName == "SABC100U1":
            DATA = {}
            DATA['주문번호'] = self.IndiTR.dynamicCall("GetSingleData(int)", 0)  # 주문번호
            DATA['ORC주문번호'] = self.IndiTR.dynamicCall("GetSingleData(int)", 1)  # ORC주문번호
            if DATA['주문번호'] == '':
                ErrState = self.IndiTR.dynamicCall("GetErrorState()")
                ErrCode = self.IndiTR.dynamicCall("GetErrorCode()")
                ErrMsg = self.IndiTR.dynamicCall("GetErrorMessage()")
                self.instInterface.setErrMsgOnStatusBar(ErrState, ErrCode, ErrMsg, __file__)
            else:
                print("매수 및 매도 주문결과 :", DATA)
                # self.instInterface.objOrder.iqrySettle(self.instInterface.strAcntCode, self.instInterface.dfAcntInfo['Acnt_Pwd'][0], self.instInterface.strToday)   # 체결/미체결 조회
                self.instInterface.setTwOrderInfoUI(DATA)
            
            self.instInterface.event_loop.exit()

        elif TRName == "SABC258Q1":
            DATA = {}
            self.nCnt = self.IndiTR.dynamicCall("GetMultiRowCount()")
            for i in range(0, self.nCnt):
                DATA['주문완료여부'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 0)  # 1: 주문완료, 2: 주문거부
                DATA['종목코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)
                DATA['종목명'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 2)
                DATA['매매구분'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 3)  # 01: 매도, 02: 매수
                DATA['매매구분명'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 4)
                DATA['주문수량'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 5)
                DATA['주문단가'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 6)
                DATA['체결수량'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 7)
                DATA['체결단가'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 8)
                DATA['미체결수량'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 9)
                DATA['현재가'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 10)
                DATA['호가구분'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 11) # 지정가, 시장가, 최유리, 조건부, 지정가전환시, 지정가전환최
                DATA['주문처리상태'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 12)
                DATA['주문번호'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 13)
                DATA['원주문번호'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 14)
                DATA['거래소접수번호'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 15)
                DATA['접수시간'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 16)
                DATA['작업자사번'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 17)
                DATA['체결시간'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 18)
                DATA['차익헤지구분'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 19)
                DATA['주문조건'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 20) # N: 일반, F: FOK, I: IOK
                DATA['자동취소수량'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 21)
                DATA['기초자산'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 22)
                DATA['채널구분'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 23)
                # 6: 신3년국채선물, 7: 통안증권선물, 8: 신5년국채선물, 9: 신10년국채선물, A: 미국달러선물, C: 엔선물, D: 유로선물, E: 금선물, F: 돈육선물, G: FLEX미국달러선물, H: 미니금선물, 
                DATA['선물옵션상세구분'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 24)
                DATA['거래승수'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 25)
                DATA['체결금액'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 26)
                DATA['선물시장구분'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 27) # C: CME, E: 유렉스

            if DATA == {}:
                ErrState = self.IndiTR.dynamicCall("GetErrorState()")
                ErrCode = self.IndiTR.dynamicCall("GetErrorCode()")
                ErrMsg = self.IndiTR.dynamicCall("GetErrorMessage()")
                self.instInterface.setErrMsgOnStatusBar(ErrState, ErrCode, ErrMsg, __file__)
            else:
                print(DATA)
                self.instInterface.objBalance.rqBalance(self.instInterface.strAcntCode, self.instInterface.dfAcntInfo['Acnt_Pwd'][0])    # 계좌 잔고 조회
                self.instInterface.allocSettlePrice(DATA)
                self.instInterface.setTwSettleInfoUI(DATA)
        

    def ReceiveRTData(self, RealType):
        if RealType == 'AB':
            DATA = {}
            DATA['처리구분'] = self.IndiTR.dynamicCall("GetSingleData(int)", 0) # 00: 정상주문, 03: 체결, 04: 정정확인, 05: 취소확인, 07: 지정가전환, 09: 주문거부
            DATA['지점번호'] = self.IndiTR.dynamicCall("GetSingleData(int)", 1)
            DATA['계좌번호'] = self.IndiTR.dynamicCall("GetSingleData(int)", 2)
            DATA['시장구분'] = self.IndiTR.dynamicCall("GetSingleData(int)", 3) # 1=KOSPI, 2=KOSDAQ, 3=파생, 4=프리보드, 5=일반채권, C=CME, ?=야간달러선물, H=EUREX, D=소액채권, E=REPO채권, F=코넥스, G=KTS채권, K=금현물
            DATA['상품구분'] = self.IndiTR.dynamicCall("GetSingleData(int)", 4)
            DATA['주문번호'] = self.IndiTR.dynamicCall("GetSingleData(int)", 5)
            DATA['원주문번호'] = self.IndiTR.dynamicCall("GetSingleData(int)", 6)
            DATA['주문구분'] = self.IndiTR.dynamicCall("GetSingleData(int)", 7) # L:지정가, M:시장가, C:조건부지정가, B:최유리지정가
            DATA['매도매수구분'] = self.IndiTR.dynamicCall("GetSingleData(int)", 8) # 01: 매도, 02: 매수
            DATA['상태'] = self.IndiTR.dynamicCall("GetSingleData(int)", 9) # 01: 접수, 02: 확인, 03: 거부
            DATA['종목코드'] = self.IndiTR.dynamicCall("GetSingleData(int)", 10)
            DATA['종목명'] = self.IndiTR.dynamicCall("GetSingleData(int)", 11)
            DATA['계좌명'] = self.IndiTR.dynamicCall("GetSingleData(int)", 12)
            DATA['주문수량'] = self.IndiTR.dynamicCall("GetSingleData(int)", 13)
            DATA['주문단가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 14)
            DATA['체결수량 합계'] = self.IndiTR.dynamicCall("GetSingleData(int)", 15)
            DATA['정정수량 합계'] = self.IndiTR.dynamicCall("GetSingleData(int)", 16)
            DATA['취소수량 합계'] = self.IndiTR.dynamicCall("GetSingleData(int)", 17)
            DATA['확인수량'] = self.IndiTR.dynamicCall("GetSingleData(int)", 18)
            DATA['미체결수량'] = self.IndiTR.dynamicCall("GetSingleData(int)", 19)
            DATA['접수시간'] = self.IndiTR.dynamicCall("GetSingleData(int)", 20)
            DATA['체결번호'] = self.IndiTR.dynamicCall("GetSingleData(int)", 21)
            DATA['체결수량'] = self.IndiTR.dynamicCall("GetSingleData(int)", 22)
            DATA['체결단가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 23)
            DATA['체결시간'] = self.IndiTR.dynamicCall("GetSingleData(int)", 24)
            DATA['채널'] = self.IndiTR.dynamicCall("GetSingleData(int)", 25)
            DATA['입력자'] = self.IndiTR.dynamicCall("GetSingleData(int)", 26)
            DATA['관리자'] = self.IndiTR.dynamicCall("GetSingleData(int)", 27)
            DATA['일련번호'] = self.IndiTR.dynamicCall("GetSingleData(int)", 28)
            DATA['현재가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 29)
            DATA['원주문구분'] = self.IndiTR.dynamicCall("GetSingleData(int)", 30)  # 0: 정상주문 결과, 1: 원주문에 대한 결과(정정/취소일 때)
            DATA['주문조건'] = self.IndiTR.dynamicCall("GetSingleData(int)", 31)    # ?: 일반, F:FOK, I:IOC, M:IFM, D:DIFM
            DATA['기초자산명'] = self.IndiTR.dynamicCall("GetSingleData(int)", 32)
            DATA['최근월물 주문가격'] = self.IndiTR.dynamicCall("GetSingleData(int)", 33)
            DATA['차근월물 주문가격'] = self.IndiTR.dynamicCall("GetSingleData(int)", 34)
            DATA['실시간가격제한주문정보'] = self.IndiTR.dynamicCall("GetSingleData(int)", 35)  # 매뉴얼 참고

            if DATA['처리구분'] == '03':
                self.instInterface.setSettleInfo(DATA)


    def ReceiveSysMsg(self, MsgID):
        self.instInterface.setSysMsgOnStatusBar(MsgID, __file__)