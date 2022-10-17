from PyQt5.QAxContainer import *
from interfaceRT import InterfaceRT


class Order():
    def __init__(self):
        # 일반 TR OCX
        self.IndiTR = QAxWidget("GIEXPERTCONTROL.GiExpertControlCtrl.1")
        self.IndiTR.ReceiveData.connect(self.ReceiveData)
        self.IndiTR.ReceiveSysMsg.connect(self.ReceiveSysMsg) # 일반 TR에 대한 응답을 받는 함수를 연결

        # TR ID를 저장해놓고 처리할 딕셔너리 생성
        self.rqidD = {}

        # 수신 데이터 전달
        self.instInterfaceRT = InterfaceRT(self.wndIndi)

    def order_full(self, acnt_num, pwd, code, qty, price, condition, direction, order_type, trading_type, treat_type, modify_type, origin_order_num, reserve_order):
        " 선물 주문을 요청한다.(Long form) "
        ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "SABC100U1")
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, str(acnt_num))  # 계좌번호
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, str(pwd))  # 비밀번호
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, str(code))  # 종목코드
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 3, str(qty))  # 주문수량
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 4, str(round(price, 2)))  # 주문단가 (소수점 2자리(양수:999.99, 음수:-999.99), RFR종목은 소수점 3자리)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 5, str(condition))  # 주문조건 (0:일반(FAS), 3:IOC(FAK), 4:FOK)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 6, str(direction))  # 매매구분 (01:매도, 02:매수, (정정/취소시):01)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 7, order_type)  # 호가유형 (L:지정가, M:시장가, C:조건부, B:최유리)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 8, str(trading_type))  # 차익거래구분 (1:차익, 2:헷지, 3:기타)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 9, str(treat_type))  # 처리구분 (1:신규, 2:정정, 3:취소)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 10, modify_type)  # 정정취소수량구분 (0:신규, 1:전부, 2:일부)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 11, origin_order_num)  # 원주문번호 (매수/매도시 생략가능)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 12, reserve_order)  # 예약주문여부 (생략가능, 1:예약)
        rqid = self.IndiTR.dynamicCall("RequestData()")  # 데이터 요청

        # 요청한 ID를 저장
        self.rqidD[rqid] = "SABC100U1"
        #self.codeID[rqid] = code
        print("매매TR요청 : ", rqid)
    
    def order(self, acnt_num, pwd, code, qty, price, direction):
        " 선물 주문을 요청한다.(Short form) "
        ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "SABC100U1")
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, acnt_num)  # 계좌번호
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, pwd)  # 비밀번호
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, code)  # 종목코드
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 3, qty)  # 주문수량
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 4, price)  # 주문단가 (소수점 2자리(양수:999.99, 음수:-999.99), RFR종목은 소수점 3자리)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 5, '0')  # 주문조건 (0:일반(FAS), 3:IOC(FAK), 4:FOK)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 6, direction)  # 매매구분 (01:매도, 02:매수, (정정/취소시):01)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 7, 'L')  # 호가유형 (L:지정가, M:시장가, C:조건부, B:최유리)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 8, '3')  # 차익거래구분 (1:차익, 2:헷지, 3:기타)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 9, '1')  # 처리구분 (1:신규, 2:정정, 3:취소)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 10, '0')  # 정정취소수량구분 (0:신규, 1:전부, 2:일부)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 11, None)  # 원주문번호 (매수/매도시 생략가능)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 12, None)  # 예약주문여부 (생략가능, 1:예약)
        rqid = self.IndiTR.dynamicCall("RequestData()")  # 데이터 요청

        # 요청한 ID를 저장
        self.rqidD[rqid] = "SABC100U1"
        #self.codeID[rqid] = code
        print("매매TR요청 : ", rqid)


    def ReceiveData(self, rqid):
        TRName = self.rqidD[rqid]

        if TRName == "SABC100U1":
            DATA = {}
            DATA['주문번호'] = self.IndiTR.dynamicCall("GetSingleData(int)", 0)  # 주문번호
            DATA['ORC주문번호'] = self.IndiTR.dynamicCall("GetSingleData(int)", 1)  # ORC주문번호
            print("매수 및 매도 주문결과 :", DATA)

    def ReceiveSysMsg(self, MsgID):
        self.instInterfaceRT.setSysMsgOnStatusBar(MsgID, __file__)