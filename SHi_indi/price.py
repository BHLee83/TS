from PyQt5.QAxContainer import *
import numpy as np
import pandas as pd
from System.strategy import Strategy



class Price():

    def __init__(self, instInterface):
        self.instInterface = instInterface
        
        # 인디의 TR을 처리할 변수를 생성합니다.
        self.IndiTR = QAxWidget("GIEXPERTCONTROL.GiExpertControlCtrl.1")

        # Indi API event
        self.IndiTR.ReceiveData.connect(self.ReceiveData)
        self.IndiTR.ReceiveSysMsg.connect(self.ReceiveSysMsg)

        self.rqidD = {} # TR 관리를 위해 사전 변수를 하나 생성

        # 데이터
        # self.objCurrentStrategy = None
        self.currentProductCode = None
        self.strRqPeriod = None

        self.dfCFutMst = pd.DataFrame(None, columns={'표준코드', '단축코드', '파생상품ID', '한글종목명', '기초자산ID', '스프레드근월물표준코드', '스프레드원월물표준코드', '최종거래일', '기초자산종목코드', '거래단위', '거래승수'})

        PriceInfodt = np.dtype([('한글종목명', 'S40'), ('상한가', 'f'), ('하한가', 'f'), ('전일종가', 'f'),
                        ('단축코드', 'S10'), ('체결시간', 'S6'), ('시가', 'f'), ('고가', 'f'), ('저가', 'f'), ('현재가', 'f'), 
                        ('단위체결량', 'u4'), ('누적거래량', 'u4'), ('매도1호가', 'f'), ('매도1호가수량', 'u4'), ('매수1호가', 'f'), ('매수1호가수량', 'u4')])
        self.PriceInfo = np.empty([1], dtype=PriceInfodt)

        self.Historicaldt = np.dtype([('일자', 'S8'), ('시간', 'S6'), ('시가', 'f'), ('고가', 'f'), ('저가', 'f'), ('종가', 'f'), ('단위거래량', 'u4')])
        # self.Historical = np.empty([300], dtype=Historicaldt)


    def rqProductMstInfo(self, tr:str):
        # cfut_mst : 상품선물종목코드조회(전종목)
        ret = self.IndiTR.dynamicCall("SetQueryName(QString)", tr)
        rqid = self.IndiTR.dynamicCall("RequestData()") # 데이터 요청
        self.rqidD[rqid] =  tr

    
    # 과거(차트) 데이터 조회
    def rqHistData(self, strProductCode, strTimeFrame, strTimeIntrv, strStartDate, strEndDate, strRqCnt):
        self.currentProductCode = strProductCode
        self.strRqPeriod = Strategy.convertTimeFrame(strTimeFrame, strTimeIntrv)
        if strProductCode.startswith('KRDRVFU'):
            trName = 'TR_CFNCHART'
        else:
            trName = 'TR_CFCHART'

        ret = self.IndiTR.dynamicCall("SetQueryName(QString)", trName)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, strProductCode)    # 단축코드
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, strTimeFrame)    # 그래프 종류 (1:분데이터, D:일데이터)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, strTimeIntrv)    # 시간간격 (분데이터일 경우: 1 – 30, 일데이터일 경우: 1)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 3, strStartDate) # 시작일 (YYYYMMDD, 분 데이터 요청시 : “00000000”)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 4, strEndDate) # 종료일 (YYYYMMDD, 분 데이터 요청시 : “99999999”)
        ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 5, strRqCnt)  # 조회갯수 (1-9999)
        rqid = self.IndiTR.dynamicCall("RequestData()")
        self.rqidD[rqid] =  trName


    # 요청한 TR로 부터 데이터 수신
    def ReceiveData(self, rqid):
        # TR을 날릴때의 ID를 통해 TR이름 가져옴
        TRName = self.rqidD[rqid]
        
        # 상품선물종목코드 수신(전종목)
        if TRName == self.instInterface.strTR_MST:
            # multi row 개수를 리턴
            nCnt = self.IndiTR.dynamicCall("GetMultiRowCount()")
            for i in range(0, nCnt):
                # 항목별 데이터
                dictCFutMst = {}
                dictCFutMst['표준코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 0)
                dictCFutMst['단축코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)
                dictCFutMst['파생상품ID'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 2)
                dictCFutMst['한글종목명'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 3)
                dictCFutMst['기초자산ID'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 4)
                dictCFutMst['스프레드근월물표준코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 5)
                dictCFutMst['스프레드원월물표준코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 6)
                dictCFutMst['최종거래일'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 7)
                dictCFutMst['기초자산종목코드'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 8)
                dictCFutMst['거래단위'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 9)
                dictCFutMst['거래승수'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 10)
                # print(dictCFutMst)

                self.dfCFutMst = self.dfCFutMst.append(dictCFutMst, ignore_index=True)

            Strategy.dfCFutMst = self.dfCFutMst
            self.instInterface.setNearMonth()
            self.instInterface.event_loop.exit()
        

        # 차트 데이터 수신
        elif (TRName == "TR_CFCHART") or (TRName == "TR_CFNCHART"):
            nCnt = self.IndiTR.dynamicCall("GetMultiRowCount()")
            # np.reshape(self.Historical, nCnt)
            self.Historical = np.empty([nCnt], dtype=self.Historicaldt)
            for i in range(0, nCnt):
                self.Historical[i]['일자'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 0)
                self.Historical[i]['시간'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)
                self.Historical[i]['시가'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 2)
                self.Historical[i]['고가'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 3)
                self.Historical[i]['저가'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 4)
                self.Historical[i]['종가'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 5)
                self.Historical[i]['단위거래량'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 9)
                # print(self.Historical[i])

            if nCnt > 0:
                Strategy.setHistData(self.currentProductCode, self.strRqPeriod, self.Historical)
            
            self.instInterface.event_loop.exit()

        
        # 종목 기본정보 수신
        elif TRName == "MB":
            self.PriceInfo[0]['한글종목명'] = (self.IndiTR.dynamicCall("GetSingleData(int)", 7)).encode('utf8')
            self.PriceInfo[0]['상한가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 13)
            self.PriceInfo[0]['하한가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 14)
            self.PriceInfo[0]['전일종가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 38)
            print(self.PriceInfo[0])


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
            # self.instStrategy.setPriceInfo(self.PriceInfo[0])


        # 종목 호가 수신
        elif TRName == "MH":
            self.PriceInfo[0]['매도1호가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 3)
            self.PriceInfo[0]['매수1호가'] = self.IndiTR.dynamicCall("GetSingleData(int)", 4)
            self.PriceInfo[0]['매도1호가수량'] = self.IndiTR.dynamicCall("GetSingleData(int)", 5)
            self.PriceInfo[0]['매수1호가수량'] = self.IndiTR.dynamicCall("GetSingleData(int)", 6)

        self.rqidD.__delitem__(rqid)


    # 시스템 메시지를 받은 경우 출력
    def ReceiveSysMsg(self, MsgID):
        self.instInterface.setSysMsgOnStatusBar(MsgID, __file__)