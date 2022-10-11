class Account():
    def __init__(self, IndiTR):
        # Indi API event
        self.IndiTR = IndiTR
        self.IndiTR.ReceiveData.connect(self.ReceiveData)
        self.IndiTR.ReceiveSysMsg.connect(self.ReceiveSysMsg)
        self.rqidD = {} # TR 관리를 위해 사전 변수를 하나 생성합니다.

    def userLogin(self):
        # 신한i Indi 자동로그인
        # while True:
        #     login = self.IndiTR.StartIndi('forheart', 'password', 'authpassword', 'C:\SHINHAN-i\indi\giexpertstarter.exe')
        #     print(login)
        #     if login == True :
                return True

    def getAccount(self):
        # AccountList : 계좌목록 조회를 요청할 TR입니다.
        # 해당 TR의 필드입력형식에 맞춰서 TR을 날리면 됩니다.
        # 데이터 요청 형식은 다음과 같습니다.
        ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "AccountList")
        rqid = self.IndiTR.dynamicCall("RequestData()") # 데이터 요청
        self.rqidD[rqid] =  "AccountList"

    # 요청한 TR로 부터 데이터를 받는 함수입니다.
    def ReceiveData(self, rqid):
        # TR을 날릴때 ID를 통해 TR이름을 가져옵니다.
        TRName = self.rqidD[rqid]

        if TRName == "AccountList" :
            # GetMultiRowCount()는 TR 결과값의 multi row 개수를 리턴합니다.
            nCnt = self.IndiTR.dynamicCall("GetMultiRowCount()")
            print(nCnt)

            # 받을 열만큼 데이터를 받도록 합니다.
            for i in range(0, nCnt):
                # 데이터 양식
                DATA = {}

                # 데이터 받기
                DATA['CODE'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 0)  # 단축코드
                DATA['NAME'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)  # 종목명
                print(DATA['CODE'])
                print(DATA['NAME'])

        self.rqidD.__delitem__(rqid)

    # 시스템 메시지를 받은 경우 출력합니다.
    def ReceiveSysMsg(self, MsgID):
        print("System Message Received = ", MsgID)