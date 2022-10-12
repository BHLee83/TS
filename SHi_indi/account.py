from config import Config
from PyQt5.QAxContainer import *
import pandas as pd

class Account():
    def __init__(self):
        # Login Info.
        self.id = Config.SHi_Indi_CONFIG['id']
        self.pwd = Config.SHi_Indi_CONFIG['password']
        self.authpwd = Config.SHi_Indi_CONFIG['authpassword']
        self.indipath = Config.SHi_Indi_CONFIG['indipath']

        # Acc. Info
        self.dfAcntInfo = pd.DataFrame(None, columns={'Acnt_Code', 'Acnt_Name'})

        # Indi API event
        self.IndiTR = QAxWidget("GIEXPERTCONTROL.GiExpertControlCtrl.1")
        self.IndiTR.ReceiveData.connect(self.ReceiveData)
        self.IndiTR.ReceiveSysMsg.connect(self.ReceiveSysMsg)
        self.rqidD = {} # TR 관리를 위해 사전 변수를 하나 생성합니다.

    def userLogin(self):
        # 신한i Indi 자동로그인
        # while True:
        #     login = self.IndiTR.StartIndi(self.id, self.pwd, self.authpwd, self.indipath)
        #     if login == True :
        #         print('Logged in successfully!')
        self.setAccount()
        return True

    def setAccount(self):
        # AccountList : 계좌목록 조회를 요청할 TR입니다.
        # 해당 TR의 필드입력형식에 맞춰서 TR을 날리면 됩니다.
        # 데이터 요청 형식은 다음과 같습니다.
        self.ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "AccountList")
        self.rqid = self.IndiTR.dynamicCall("RequestData()") # 데이터 요청
        self.rqidD[self.rqid] =  "AccountList"

    def getAccount(self):
        return self.dfAcntInfo

    # 요청한 TR로 부터 데이터를 받는 함수입니다.
    def ReceiveData(self, rqid):
        # TR을 날릴때 ID를 통해 TR이름을 가져옵니다.
        if self.rqidD[rqid] == "AccountList" :
            # GetMultiRowCount()는 TR 결과값의 multi row 개수를 리턴합니다.
            self.nCnt = self.IndiTR.dynamicCall("GetMultiRowCount()")
        
            # 받을 열만큼 데이터를 받도록 합니다.
            self.dictAcntInfo = {}
            for i in range(0, self.nCnt):
                # 데이터 받기
                self.dictAcntInfo['Acnt_Code'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 0)  # 계좌번호
                self.dictAcntInfo['Acnt_Name'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)  # 계좌명
                self.dfAcntInfo = self.dfAcntInfo.append(self.dictAcntInfo, ignore_index=True)
            print(self.dfAcntInfo)

        self.rqidD.__delitem__(rqid)

    # 시스템 메시지를 받은 경우 출력합니다.
    def ReceiveSysMsg(self, MsgID):
        # 재연결 실패, 재접속 실패, 공지 등의 수동으로 재접속이 필요할 경우에는 이벤트는 발생하지 않고 신한i Expert Main에서 상태를 표현해준다.
        if MsgID == 3:
            MsgStr = "체결통보 데이터 재조회 필요"
        elif MsgID == 7:
            MsgStr = "통신 실패 후 재접속 성공"
        elif MsgID == 10:
            MsgStr = "시스템이 종료됨"
        elif MsgID == 11:
            MsgStr = "시스템이 시작됨"
        else:
            MsgStr = "System Message Received in module 'account' = " + str(MsgID)
        print(MsgStr)