from config import Config
from PyQt5.QAxContainer import *
from interfaceRT import InterfaceRT
import pandas as pd

class Account():
    def __init__(self, wndIndi):
        self.wndIndi = wndIndi

        # Login Info.
        self.id = Config.SHi_Indi_Connection['id']
        self.pwd = Config.SHi_Indi_Connection['password']
        self.authpwd = Config.SHi_Indi_Connection['authpassword']
        self.indipath = Config.SHi_Indi_Connection['indipath']

        # Acc. Info
        self.dfAcntInfo = pd.DataFrame(None, columns={'Acnt_Code', 'Acnt_Name'})

        # Indi API event
        self.IndiTR = QAxWidget("GIEXPERTCONTROL.GiExpertControlCtrl.1")
        self.IndiTR.ReceiveData.connect(self.ReceiveData)
        self.IndiTR.ReceiveSysMsg.connect(self.ReceiveSysMsg)
        self.rqidD = {} # TR 관리를 위해 사전 변수를 하나 생성합니다.

        # 수신 데이터 전달
        self.instInterfaceRT = InterfaceRT(self.wndIndi)

    def userLogin(self):
        # 신한i Indi 자동로그인
        # while True:
        #     login = self.IndiTR.StartIndi(self.id, self.pwd, self.authpwd, self.indipath)
        #     if login == True :
        #         print('Logged in successfully!')
        self.setAccount()
        return True

    def setAccount(self):
        # AccountList : 계좌목록 조회를 요청할 TR
        self.ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "AccountList")
        self.rqid = self.IndiTR.dynamicCall("RequestData()") # 데이터 요청
        self.rqidD[self.rqid] =  "AccountList"

    def getAccount(self):
        return self.dfAcntInfo

    def ReceiveData(self, rqid):
        if self.rqidD[rqid] == "AccountList" :
            self.nCnt = self.IndiTR.dynamicCall("GetMultiRowCount()")
        
            self.dictAcntInfo = {}
            for i in range(0, self.nCnt):
                self.dictAcntInfo['Acnt_Code'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 0)  # 계좌번호
                self.dictAcntInfo['Acnt_Name'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)  # 계좌명
                
                self.wndIndi.cbAcntCode.addItem(self.dictAcntInfo['Acnt_Code'])
                self.dfAcntInfo = self.dfAcntInfo.append(self.dictAcntInfo, ignore_index=True)
            # print(self.dfAcntInfo)

        self.rqidD.__delitem__(rqid)

    # 시스템 메시지를 받은 경우 출력
    def ReceiveSysMsg(self, MsgID):
        self.wndIndi.setSysMsgOnStatusBar(MsgID, __file__)