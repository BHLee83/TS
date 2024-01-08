import pandas as pd

from PyQt5.QAxContainer import *
from System.strategy import Strategy


class Article():
    def __init__(self, instInterface):
        self.instInterface = instInterface

        # Article Info
        self.lstColumns = ['Date', 'Time', 'Title', 'Category', 'No.']
        self.dfArticleInfo = pd.DataFrame(None, columns=self.lstColumns)

        # Indi API event
        self.IndiTR = QAxWidget("GIEXPERTCONTROL.GiExpertControlCtrl.1")
        self.IndiTR.ReceiveData.connect(self.ReceiveData)
        self.IndiTR.ReceiveSysMsg.connect(self.ReceiveSysMsg)
        self.rqidD = {} # TR 관리를 위해 사전 변수를 하나 생성합니다.


    def reqTR(self, pCode, pCategory, pDate):
        "TR Input"
        self.ret = self.IndiTR.dynamicCall("SetQueryName(QString)", "TR_3100_D")
        self.ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, pCode)    # 뉴스_종목코드 (" ": 전체 or 6자리 종목코드)
        self.ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, pCategory)    # 구분 ("1": 전체, "2": 뉴스, "3": 공시)
        self.ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, pDate)     # 조회일자 ("YYYYMMDD")
        self.rqid = self.IndiTR.dynamicCall("RequestData()") # 데이터 요청
        self.rqidD[self.rqid] =  "ArticleList"
        

    def getList(self):
        return self.dfArticleInfo


    def ReceiveData(self, rqid):
        if self.rqidD[rqid] == "ArticleList" :
            self.nCnt = self.IndiTR.dynamicCall("GetMultiRowCount()")
        
            self.dfArticleNewer = pd.DataFrame(None, columns=self.lstColumns)
            self.dictArticleInfo = {}
            for i in range(0, self.nCnt):
                self.dictArticleInfo['Date'] = self.IndiTR.dynamicCall("GetMultiData(QString, QString)", i, 0)  # 일자
                self.dictArticleInfo['Time'] = self.IndiTR.dynamicCall("GetMultiData(QString, QString)", i, 1)  # 입력시간
                self.dictArticleInfo['Title'] = self.IndiTR.dynamicCall("GetMultiData(QString, QString)", i, 2) # 제목
                self.dictArticleInfo['Category'] = self.IndiTR.dynamicCall("GetMultiData(QString, QString)", i, 3)  # 구분 (A:인포, M:MT, E:ED, Y:연합, H:한경, I:내부, F:시황, P:공시, Q:공시, S:공시, G:공시, N:공시, T:공시, U:해외)
                self.dictArticleInfo['No.'] = self.IndiTR.dynamicCall("GetMultiData(QString, QString)", i, 4)   # 기사번호
                
                self.dfArticleNewer = self.dfArticleNewer.append(self.dictArticleInfo, ignore_index=True)
            # 기사 보존 필요 있을 때 아래 코드 사용 (검증 필요)
            # self.dfArticleInfo = pd.merge(self.dfArticleInfo, self.dfArticleNewer, how='outer', on=self.lstColumns).sort_values(by='Time', ascending=False)
            # Strategy.dfArticle = self.dfArticleInfo.copy()
            Strategy.dfArticle = self.dfArticleNewer.copy()

        self.rqidD.__delitem__(rqid)


    # 시스템 메시지를 받은 경우 출력
    def ReceiveSysMsg(self, MsgID):
        self.instInterface.setSysMsgOnStatusBar(MsgID, __file__)