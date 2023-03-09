from PyQt5.QAxContainer import *
import numpy as np
# from System.strategyRT import StrategyRT


class PriceRT():
    def __init__(self, instInterface):
        self.instInterface = instInterface
        
        # 인디의 TR을 처리할 변수를 생성합니다.
        self.IndiTR = QAxWidget("GIEXPERTCONTROL.GiExpertControlCtrl.1")
        self.IndiReal = QAxWidget("GIEXPERTCONTROL.GiExpertControlCtrl.1")

        # Indi API event
        self.IndiTR.ReceiveData.connect(self.ReceiveData)
        self.IndiTR.ReceiveSysMsg.connect(self.ReceiveSysMsg)
        self.IndiReal.ReceiveRTData.connect(self.ReceiveRTData)

        self.rqidD = {} # TR 관리를 위해 사전 변수를 하나 생성

        # 데이터
        self.currentCode = None

        PriceInfodt = np.dtype([('한글종목명', 'S40'), ('상한가', 'f'), ('하한가', 'f'), ('전일종가', 'f'),
                        ('단축코드', 'S10'), ('체결시간', 'S6'), ('시가', 'f'), ('고가', 'f'), ('저가', 'f'), ('현재가', 'f'), 
                        ('단위체결량', 'u4'), ('누적거래량', 'u4'), ('매도1호가', 'f'), ('매도1호가수량', 'u4'), ('매수1호가', 'f'), ('매수1호가수량', 'u4')])
        self.PriceInfo = np.empty([1], dtype=PriceInfodt)

    def stopTR(self):
        # self.IndiReal.dynamicCall("UnRequestRTRegAll()")
        if self.currentCode != None:
            self.IndiReal.dynamicCall("UnRequestRTReg(QString, QString)", "MB", self.currentCode)
            self.IndiReal.dynamicCall("UnRequestRTReg(QString, QString)", "MC", self.currentCode)
            self.IndiReal.dynamicCall("UnRequestRTReg(QString, QString)", "MH", self.currentCode)

    def startTR(self):
        # 기존종목 실시간 해제
        self.stopTR()

        # 현재 종목코드 정보
        self.currentCode = self.instInterface.wndIndi.cbProductCode.currentText().upper()

        # 종목 기본정보 조회
        ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "MB")
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, self.currentCode)
        rqid = self.IndiTR.dynamicCall("RequestData()")
        self.rqidD[rqid] = "MB"

        # 종목 현재가 조회
        ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "MC")
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, self.currentCode)
        rqid = self.IndiTR.dynamicCall("RequestData()")
        self.rqidD[rqid] = "MC"

        # 종목 호가 조회
        ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "MH")
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, self.currentCode)
        rqid = self.IndiTR.dynamicCall("RequestData()")
        self.rqidD[rqid] = "MH"
        
        # 실시간 등록
        # ret = self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", "MB", self.currentCode)   # 종목 기본정보
        # ret = self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", "MC", self.currentCode)   # 현재가
        # ret = self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", "MH", self.currentCode)   # 호가

    # 요청한 TR로 부터 데이터 수신
    def ReceiveData(self, rqid):
        # TR을 날릴때의 ID를 통해 TR이름 가져옴
        TRName = self.rqidD[rqid]
        
        # 종목 기본정보 수신
        if TRName == "MB":
            self.PriceInfo[0]['한글종목명'] = (self.IndiTR.dynamicCall("GetSingleData(int)", 5)).encode('utf8')
            self.PriceInfo[0]['상한가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 13)
            self.PriceInfo[0]['하한가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 14)
            self.PriceInfo[0]['전일종가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 38)
            # 실시간 등록
            ret = self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", "MB", self.currentCode)
        
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

            # Data transfer
            self.instInterface.executeStrategy(self.PriceInfo[0])
            self.instInterface.setTwProductInfoUI(self.PriceInfo[0])

            # 실시간 등록
            ret = self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", "MC", self.currentCode)

        # 종목 호가 수신
        elif TRName == "MH":
            try:
                self.PriceInfo[0]['매도1호가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 3)
                self.PriceInfo[0]['매수1호가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 4)
                self.PriceInfo[0]['매도1호가수량'] = self.IndiTR.dynamicCall("GetSingleData(int)", 5)
                self.PriceInfo[0]['매수1호가수량'] = self.IndiTR.dynamicCall("GetSingleData(int)", 6)
            except:
                self.instInterface.setSysMsgOnStatusBar("현재 호가 데이터가 수신되지 않습니다", __file__)
            # 실시간 등록
            ret = self.IndiReal.dynamicCall("RequestRTReg(QString, QString)", "MH", self.currentCode)

        self.rqidD.__delitem__(rqid)


    def ReceiveRTData(self, RealType):
        # 종목 기본정보 실시간 수신
        if RealType == "MB":
            self.PriceInfo[0]['한글종목명'] = (self.IndiReal.dynamicCall("GetSingleData(int)", 5)).encode('utf8')
            self.PriceInfo[0]['상한가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 13)
            self.PriceInfo[0]['하한가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 14)
            self.PriceInfo[0]['전일종가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 38)

        # 종목 현재가 실시간 수신
        elif RealType == "MC":
            self.PriceInfo[0]['단축코드'] = self.IndiReal.dynamicCall("GetSingleData(int)", 1)
            self.PriceInfo[0]['체결시간'] = self.IndiReal.dynamicCall("GetSingleData(int)", 2)
            self.PriceInfo[0]['현재가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 4)
            self.PriceInfo[0]['누적거래량'] = self.IndiReal.dynamicCall("GetSingleData(int)", 8)
            self.PriceInfo[0]['단위체결량'] = self.IndiReal.dynamicCall("GetSingleData(int)", 10)
            self.PriceInfo[0]['시가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 12)
            self.PriceInfo[0]['고가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 13)
            self.PriceInfo[0]['저가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 14)

            # Data transfer
            self.instInterface.executeStrategy(self.PriceInfo[0])
            self.instInterface.setTwProductInfoUI(self.PriceInfo[0])
            
        # 종목 호가 실시간 수신
        elif RealType == "MH":
            self.PriceInfo[0]['매도1호가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 3)
            self.PriceInfo[0]['매수1호가'] = self.IndiReal.dynamicCall("GetSingleData(int)", 4)
            self.PriceInfo[0]['매도1호가수량'] = self.IndiReal.dynamicCall("GetSingleData(int)", 5)
            self.PriceInfo[0]['매수1호가수량'] = self.IndiReal.dynamicCall("GetSingleData(int)", 6)

        # print(self.PriceInfo[0])


    # 시스템 메시지를 받은 경우 출력
    def ReceiveSysMsg(self, MsgID):
        self.instInterface.setSysMsgOnStatusBar(MsgID, __file__)