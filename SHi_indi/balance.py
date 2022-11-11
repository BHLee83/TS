from PyQt5.QAxContainer import *



class Balance():
    def __init__(self, instInterface) -> None:
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


    def rqBalance(self, acnt_num:str, pwd:str, type:str='0', product:str='0', avgCode:str='1'):
        "선물 잔고 조회"
        ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "SABC967Q1")
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, acnt_num)  # 계좌번호
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, pwd)  # 비밀번호
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, type)  # 구분(0: 전체, 1:강세불리, 2:약세불리)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 3, product)  # 상품군 (0: 전체(상품선물), 1:지수 TOBE 1:선물, 2:옵션)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, avgCode)  # 평균가구분코드
        rqid = self.IndiTR.dynamicCall("RequestData()")  # 데이터 요청
        self.rqidD[rqid] = "SABC967Q1"

    
    def startBalanceRT(self, acnt_num):
        '선물/옵션 주문체결 실시간'
        if self.strCurrentAcnt != acnt_num:
            self.strCurrentAcnt = acnt_num
            ret = self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", "AE", self.strCurrentAcnt)


    def stopBalanceRT(self, acnt_num):
        self.IndiReal.dynamicCall("UnRequestRTReg(QString, QString)", "AE", acnt_num)
        self.strCurrentAcnt = ''

    
    def ReceiveData(self, rqid):
        TRName = self.rqidD[rqid]

        if TRName == "SABC967Q1":
            DATA = {}
            self.nCnt = self.IndiTR.dynamicCall("GetMultiRowCount()")
            for i in range(0, self.nCnt):
                DATA['단축코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 0)
                DATA['종목명'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)
                DATA['매매구분'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 2)  # 01:매도, 02:매수, 03:금전신탁
                DATA['잔고수량'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 3)
                DATA['청산가능수량'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 4)
                DATA['미체결수량'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 5)
                DATA['평균단가'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 6)  # 소수점2자리
                DATA['현재가'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 7)  # 소수점2자리
                DATA['전일대비'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 8)  # 소수점2자리
                DATA['전일대비율'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 9)  # 소수점2자리
                DATA['매입금액'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 10)
                DATA['평가금액'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 11)
                DATA['평가손익'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 12)
                DATA['손익율'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 13)   # 소수점2자리
                DATA['전일대비손익'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 14)
                DATA['손익가감액'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 15)
                DATA['델타'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 16)   # 소수점6자리
                DATA['감마'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 17)   # 소수점6자리
                DATA['매매손익'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 18)
                DATA['수수료'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 19)
                DATA['계약당승수'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 20)
                DATA['전일종가'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 21)
                DATA['선물옵션구분'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 22)
                DATA['베가'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 23) # 소수점6자리
                DATA['세타'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 24) # 소수점6자리
                DATA['기초자산'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 25)
                DATA['원체결단가'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 26)


            if self.nCnt != 0:
                print("매수 및 매도 주문결과 :", DATA)
                self.instInterface.setTwBalanceInfoUI(DATA)
    

    def ReceiveRTData(self, RealType):
        if RealType == 'AE':
            DATA = {}
            DATA['계좌번호'] = self.IndiTR.dynamicCall("GetSingleData(int)", 0)
            DATA['상품구분'] = self.IndiTR.dynamicCall("GetSingleData(int)", 1)
            DATA['종목코드'] = self.IndiTR.dynamicCall("GetSingleData(int)", 2)
            DATA['종목명'] = self.IndiTR.dynamicCall("GetSingleData(int)", 3)
            DATA['계좌명'] = self.IndiTR.dynamicCall("GetSingleData(int)", 4)
            DATA['매수매도구분'] = self.IndiTR.dynamicCall("GetSingleData(int)", 5) # 01: 매도, 02: 매수
            DATA['당일잔고'] = self.IndiTR.dynamicCall("GetSingleData(int)", 6)
            DATA['평균단가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 7)
            DATA['미체결수량'] = self.IndiTR.dynamicCall("GetSingleData(int)", 8)
            DATA['청산가능수량'] = self.IndiTR.dynamicCall("GetSingleData(int)", 9)
            DATA['총매입금액'] = self.IndiTR.dynamicCall("GetSingleData(int)", 10)
            DATA['평가금액'] = self.IndiTR.dynamicCall("GetSingleData(int)", 11)
            DATA['평가손익'] = self.IndiTR.dynamicCall("GetSingleData(int)", 12)
            DATA['손익률'] = self.IndiTR.dynamicCall("GetSingleData(int)", 13)
            DATA['수수료'] = self.IndiTR.dynamicCall("GetSingleData(int)", 14)
            DATA['세금'] = self.IndiTR.dynamicCall("GetSingleData(int)", 15)
            DATA['현재가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 17)
            DATA['거래승수'] = self.IndiTR.dynamicCall("GetSingleData(int)", 23)
            DATA['종목구분'] = self.IndiTR.dynamicCall("GetSingleData(int)", 24)    # 1: 선물, 2: 지수옵션, 3: 주식옵션
            DATA['종목매매손익'] = self.IndiTR.dynamicCall("GetSingleData(int)", 25)
            DATA['선물총매매손익'] = self.IndiTR.dynamicCall("GetSingleData(int)", 26)
            DATA['옵션총매매손익'] = self.IndiTR.dynamicCall("GetSingleData(int)", 27)
            DATA['기초자산명'] = self.IndiTR.dynamicCall("GetSingleData(int)", 28)
            DATA['이동평균단가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 29)
            DATA['원체결단가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 30)
            DATA['이동평균 매매손익'] = self.IndiTR.dynamicCall("GetSingleData(int)", 31)

            self.instInterface.setTwBalanceInfoUI(DATA)


    # 시스템 메시지를 받은 경우 출력
    def ReceiveSysMsg(self, MsgID):
        self.instInterface.setSysMsgOnStatusBar(MsgID, __file__)