from PyQt5.QAxContainer import *
import time



class Order():

    def __init__(self, instInterface):
        self.instInterface = instInterface

        # 일반 TR OCX
        self.IndiTR = QAxWidget("GIEXPERTCONTROL.GiExpertControlCtrl.1")
        self.IndiTR.ReceiveData.connect(self.ReceiveData)
        self.IndiTR.ReceiveSysMsg.connect(self.ReceiveSysMsg) # 일반 TR에 대한 응답을 받는 함수를 연결

        # TR ID를 저장해놓고 처리할 딕셔너리 생성
        self.rqidD = {}


    def order(self, acnt_num:str, pwd:str, code:str, qty, price, direction:str, condition:str='0', order_type:str='L', trading_type:str='3', treat_type:str='1', modify_type:str='0', origin_order_num=None, reserve_order=None):
        " 선물 주문을 요청한다."
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
                self.instInterface.setTwOrderInfoUI(DATA)

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
                self.instInterface.allocSettlePrice(DATA)
                self.instInterface.setTwSettleInfoUI(DATA)
        

    def ReceiveSysMsg(self, MsgID):
        self.instInterface.setSysMsgOnStatusBar(MsgID, __file__)