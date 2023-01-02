import numpy as np
import pandas as pd
import datetime as dt

from PyQt5.QtGui import QPalette, QColor
from PyQt5.QtCore import QEventLoop, Qt
from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtWidgets import QApplication, QMainWindow, QPushButton, QLineEdit, QMessageBox

# import sys
# from os import path
# wd = path.dirname( path.dirname( path.abspath(__file__) ) )
# sys.path.append(wd)
from DB.dbconn import oracleDB
from SHi_indi.config import Config



class IndiWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Market data collector")

        self.boolSysReady = False
        self.event_loop = QEventLoop()
        self.instDB = oracleDB('oraDB1')
        self.dtToday = dt.datetime.now().date()
        self.strToday = self.dtToday.strftime('%Y%m%d')
        self.strDtTarget = self.strToday

        self.rqidD = {}
        self.strCFutMst = 'cfut_mst'
        self.strTRCFChart = 'TR_CFCHART'
        self.strTRCFNChart = 'TR_CFNCHART'
        self.dfCFutMst = pd.DataFrame(None, columns={'표준코드', '단축코드', '파생상품ID', '한글종목명', '기초자산ID', '스프레드근월물표준코드', '스프레드원월물표준코드', '최종거래일', '기초자산종목코드', '거래단위', '거래승수'})
        self.Historicaldt = np.dtype([('Date', 'S8'), ('Time', 'S6'), ('Open', 'f'), ('High', 'f'), ('Low', 'f'), ('Close', 'f'), ('Vol', 'u4')])
        self.lstTargets = ['165', '167', '175']
        self.lstSymbols_Near_Month = []
        self.lstSymbols_2nd_Month = []
        self.lstSymbols_Continuous = []
        self.lstAssetClass = ['IR', 'IR', 'FX']
        self.lstAssetName = ['3년국채', '10년국채', '미국달러']
        self.lstAssetType = ['Futures', 'Futures', 'Futures']
        self.strCurrentSymbol = ''
        self.strRqDataType = ''
        self.strRqTimeIntvl = ''
        self.lstRqMinuteIntvl = ['1', '3', '5', '10', '15', '30']

        self.IndiTR = QAxWidget("GIEXPERTCONTROL.GiExpertControlCtrl.1")
        self.IndiTR.ReceiveData.connect(self.ReceiveData)
        self.IndiTR.ReceiveSysMsg.connect(self.ReceiveSysMsg)

        # Login Info.
        self.id = Config.SHi_Indi_Connection['id']
        self.pwd = Config.SHi_Indi_Connection['password']
        self.authpwd = Config.SHi_Indi_Connection['authpassword']
        self.indipath = Config.SHi_Indi_Connection['indipath']
        while True: # 신한i Indi 자동로그인
            login = self.IndiTR.StartIndi(self.id, self.pwd, self.authpwd, self.indipath)
            if login:
                if self.boolSysReady == False:
                    self.event_loop.exec_()
                break

        self.getMst()   # Get master info.
        self.selectSymbols()   # Set symbols list
        
        self.leDate = QLineEdit(self)   # Target date
        self.leDate.setGeometry(20, 20, 60, 20)
        self.leDate.setText(self.strDtTarget)
        self.leDate.editingFinished.connect(self.leDateEdited)
        self.leDate.returnPressed.connect(self.btn_Insert)

        self.leTargets = QLineEdit(self)    # Target symbols
        self.leTargets.setGeometry(20, 50, 160, 20)
        self.leTargets.setText(', '.join(self.lstAssetName))
        self.leTargets.setReadOnly(True)
        palette = QPalette()
        palette.setColor(QPalette.Base, QColor(Qt.GlobalColor.gray))
        self.leTargets.setPalette(palette)

        btnInsert = QPushButton("Insert", self)
        btnInsert.setGeometry(85, 20, 50, 20)
        btnInsert.clicked.connect(self.btn_Insert)


    def leDateEdited(self):
        self.strDtTarget = self.leDate.text()   # It must be 'YYYYMMDD' format


    def btn_Insert(self):
        '버튼 클릭시 데이터 수집/입력 실행'
        self.strDtTarget = self.leDate.text()

        self.rqDaily()
        self.rqMinute()
        # self.rqTick()

        mbDone = QMessageBox(self)
        mbDone.setText('Done!')
        mbDone.exec_()


    def getMst(self):
        '마스터 정보 얻기'
        ret = self.IndiTR.dynamicCall("SetQueryName(QString)", self.strCFutMst)
        rqid = self.IndiTR.dynamicCall("RequestData()")
        self.rqidD[rqid] =  self.strCFutMst
        self.event_loop.exec_()


    def selectSymbols(self):
        '대상 심볼 리스트 세팅'
        for i in self.lstTargets:
            for j in self.dfCFutMst.index:
                if self.dfCFutMst['단축코드'][j].startswith(i):
                    self.lstSymbols_Near_Month.append(self.dfCFutMst['단축코드'][j])    # 근월물
                    self.lstSymbols_2nd_Month.append(self.dfCFutMst['단축코드'][j+1])   # 차근월물
                    self.lstSymbols_Continuous.append('KRDRVFU'+self.dfCFutMst['기초자산ID'][j])    # 연결선물
                    break

    
    def rqDaily(self):
        '일봉 데이터 요청'
        self.strRqDataType = 'D'
        self.strRqTimeIntvl = '1'

        for i, v in enumerate(self.lstSymbols_Near_Month):  # 근월물
            self.strCurrentSymbol = v
            ret = self.IndiTR.dynamicCall("SetQueryName(QString)", self.strTRCFChart)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, self.strCurrentSymbol)  # 단축코드
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, self.strRqDataType)    # 1: 분데이터, D:일데이터
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, self.strRqTimeIntvl)    # 시간간격 (분데이터일 경우 1-30, 일데이터일 경우 1)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 3, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "00000000")
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 4, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "99999999")
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 5, '1')  # 조회갯수 (1 - 9999)
            rqid = self.IndiTR.dynamicCall("RequestData()")
            self.rqidD[rqid] =  self.strTRCFChart
            self.event_loop.exec_()

        for i in self.lstSymbols_Continuous:    # 연결선물
            self.strCurrentSymbol = i
            ret = self.IndiTR.dynamicCall("SetQueryName(QString)", self.strTRCFNChart)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, self.strCurrentSymbol)  # 단축코드
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, self.strRqDataType)    # 1: 분데이터, D:일데이터
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, self.strRqTimeIntvl)    # 시간간격 (분데이터일 경우 1-30, 일데이터일 경우 1)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 3, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "00000000")
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 4, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "99999999")
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 5, '1')  # 조회갯수 (1 - 9999)
            rqid = self.IndiTR.dynamicCall("RequestData()")
            self.rqidD[rqid] =  self.strTRCFNChart
            self.event_loop.exec_()
    
    
    def rqMinute(self):
        '분봉 데이터 요청'
        self.strRqDataType = '1'

        for i, v in enumerate(self.lstSymbols_Near_Month):  # 근월물
            self.strCurrentSymbol = v
            for j in self.lstRqMinuteIntvl:
                self.strRqTimeIntvl = j
                ret = self.IndiTR.dynamicCall("SetQueryName(QString)", self.strTRCFChart)
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, self.strCurrentSymbol)  # 단축코드
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, self.strRqDataType)    # 1: 분데이터, D:일데이터
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, self.strRqTimeIntvl)    # 시간간격 (분데이터일 경우 1-30, 일데이터일 경우 1)
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 3, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "00000000")
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 4, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "99999999")
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 5, '410')  # 조회갯수 (1 - 9999)
                rqid = self.IndiTR.dynamicCall("RequestData()")
                self.rqidD[rqid] =  self.strTRCFChart
                self.event_loop.exec_()

        for i in self.lstSymbols_Continuous:    # 연결선물
            self.strCurrentSymbol = i
            for j in self.lstRqMinuteIntvl:
                self.strRqTimeIntvl = j
                ret = self.IndiTR.dynamicCall("SetQueryName(QString)", self.strTRCFNChart)
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, self.strCurrentSymbol)  # 단축코드
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, self.strRqDataType)    # 1: 분데이터, D:일데이터
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, self.strRqTimeIntvl)    # 시간간격 (분데이터일 경우 1-30, 일데이터일 경우 1)
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 3, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "00000000")
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 4, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "99999999")
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 5, '410')  # 조회갯수 (1 - 9999)
                rqid = self.IndiTR.dynamicCall("RequestData()")
                self.rqidD[rqid] =  self.strTRCFNChart
                self.event_loop.exec_()


    def rqTick(self):
        '틱 데이터 요청'
        self.strRqDataType = 'T'

        for i, v in enumerate(self.lstSymbols_Near_Month):  # 근월물
            self.strCurrentSymbol = v
            ret = self.IndiTR.dynamicCall("SetQueryName(QString)", self.strTRCFChart)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, self.strCurrentSymbol)  # 단축코드
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, self.strRqDataType)    # 1: 분데이터, D:일데이터
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, self.strRqTimeIntvl)    # 시간간격 (분데이터일 경우 1-30, 일데이터일 경우 1)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 3, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "00000000")
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 4, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "99999999")
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 5, '9999')  # 조회갯수 (1 - 9999)
            rqid = self.IndiTR.dynamicCall("RequestData()")
            self.rqidD[rqid] =  self.strTRCFChart
            self.event_loop.exec_()

            # 요청 대상일이 근월물의 최종거래일인 경우 차근월물 데이터도 입수
            if self.strDtTarget == self.dfCFutMst[self.dfCFutMst['단축코드']==self.strCurrentSymbol]['최종거래일'].values[0]:
                self.strCurrentSymbol = self.lstSymbols_2nd_Month[i]
                ret = self.IndiTR.dynamicCall("SetQueryName(QString)", self.strTRCFChart)
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, self.strCurrentSymbol)  # 단축코드
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, self.strRqDataType)    # 1: 분데이터, D:일데이터
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, self.strRqTimeIntvl)    # 시간간격 (분데이터일 경우 1-30, 일데이터일 경우 1)
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 3, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "00000000")
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 4, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "99999999")
                ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 5, '9999')  # 조회갯수 (1 - 9999)
                rqid = self.IndiTR.dynamicCall("RequestData()")
                self.rqidD[rqid] =  self.strTRCFChart
                self.event_loop.exec_()
            
        for i in self.lstSymbols_Continuous:    # 연결선물
            self.strCurrentSymbol = i
            ret = self.IndiTR.dynamicCall("SetQueryName(QString)", self.strTRCFNChart)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 0, self.strCurrentSymbol)  # 단축코드
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 1, self.strRqDataType)    # 1: 분데이터, D:일데이터
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 2, self.strRqTimeIntvl)    # 시간간격 (분데이터일 경우 1-30, 일데이터일 경우 1)
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 3, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "00000000")
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 4, self.strDtTarget) # YYYYMMDD (분 데이터 요청시: "99999999")
            ret = self.IndiTR.dynamicCall("SetSingleData(int, QString)", 5, '9999')  # 조회갯수 (1 - 9999)
            rqid = self.IndiTR.dynamicCall("RequestData()")
            self.rqidD[rqid] =  self.strTRCFNChart
            self.event_loop.exec_()


    def procDaily(self, rqid):
        '일봉 데이터 처리'
        TRName = self.rqidD[rqid]
        df = self.convertNPtoDF()
        # df = df[df['Date']==self.strDtTarget]
        if TRName == self.strTRCFChart:
            ix = self.lstSymbols_Near_Month.index(self.strCurrentSymbol)
            settle_date = self.dfCFutMst[self.dfCFutMst['단축코드']==self.strCurrentSymbol]['최종거래일'].values[0]
            maturity = settle_date[2:6]
            settle_date = dt.datetime.strptime(settle_date, '%Y%m%d').strftime('%Y-%m-%d')
        elif TRName == self.strTRCFNChart:
            ix = self.lstSymbols_Continuous.index(self.strCurrentSymbol)
            settle_date = ''
            maturity = '0000'

        # Set fields
        dictResult = {}
        dictResult['BASE_DATE'] = dt.datetime.strptime(df['Date'].values[0], '%Y%m%d').strftime('%Y-%m-%d')
        dictResult['ASSET_CLASS'] = self.lstAssetClass[ix]
        dictResult['ASSET_NAME'] = self.lstAssetName[ix]
        dictResult['ASSET_TYPE'] = self.lstAssetType[ix]
        dictResult['MATURITY'] = maturity
        dictResult['SETTLE_DATE'] = settle_date
        dictResult['OPEN_PRICE'] = df['Open'].values[0]
        dictResult['HIGH_PRICE'] = df['High'].values[0]
        dictResult['LOW_PRICE'] = df['Low'].values[0]
        dictResult['CLOSE_PRICE'] = df['Close'].values[0]
        dictResult['VOLUME'] = df['Vol'].values[0]

        # Insert to DB
        query = f'INSERT INTO market_data VALUES {tuple(dictResult.values())}'
        self.instDB.execute(query)
        self.instDB.commit()


    def procMinute(self, rqid):
        '분봉 데이터 처리'
        TRName = self.rqidD[rqid]
        df = self.convertNPtoDF()
        # df = df[df['Date']==self.strDtTarget]
        if TRName == self.strTRCFChart:
            ix = self.lstSymbols_Near_Month.index(self.strCurrentSymbol)
            settle_date = self.dfCFutMst[self.dfCFutMst['단축코드']==self.strCurrentSymbol]['최종거래일'].values[0]
            maturity = settle_date[2:6]
            settle_date = dt.datetime.strptime(settle_date, '%Y%m%d').strftime('%Y-%m-%d')
        elif TRName == self.strTRCFNChart:
            ix = self.lstSymbols_Continuous.index(self.strCurrentSymbol)
            settle_date = ''
            maturity = '0000'

        # Set table
        dfResult = pd.DataFrame(None, columns=['BASE_DATETIME', 'TIMEFRAME', 'ASSET_CLASS', 'ASSET_NAME', 'ASSET_TYPE', 'MATURITY', 'SETTLE_DATE', 'OPEN_PRICE', 'HIGH_PRICE', 'LOW_PRICE', 'CLOSE_PRICE', 'VOLUME'])
        # dfResult['BASE_DATE'] = df['Date'].astype('datetime64[ns]', copy=False)
        dfResult['BASE_DATETIME'] = df['Date'] + ' ' + df['Time'] + '00'
        dfResult['BASE_DATETIME'] = dfResult['BASE_DATETIME'].astype('datetime64[ns]', copy=False)
        dfResult['TIMEFRAME'] = self.strRqTimeIntvl
        dfResult['ASSET_CLASS'] = self.lstAssetClass[ix]
        dfResult['ASSET_NAME'] = self.lstAssetName[ix]
        dfResult['ASSET_TYPE'] = self.lstAssetType[ix]
        dfResult['MATURITY'] = maturity
        dfResult['SETTLE_DATE'] = settle_date
        dfResult['OPEN_PRICE'] = df['Open']
        dfResult['HIGH_PRICE'] = df['High']
        dfResult['LOW_PRICE'] = df['Low']
        dfResult['CLOSE_PRICE'] = df['Close']
        dfResult['VOLUME'] = df['Vol']
        
        if int(self.strRqTimeIntvl) < 10:
            dfResult = dfResult.set_index('BASE_DATETIME')
            idx = dfResult.between_time('15:'+str(int(self.strRqTimeIntvl)+35)+':00', '15:45:00', include_end=False).index
            dfResult = dfResult.drop(idx)
            dfResult = dfResult.reset_index()

        # Insert to DB
        # dfResult['BASE_DATETIME'] = dfResult['BASE_DATETIME'].map(str)
        query = 'INSERT INTO market_data_minute VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12)'
        self.instDB.executemany(query, dfResult.values.tolist())
        self.instDB.commit()


    def procTick(self, rqid):
        '틱 데이터 처리'
        TRName = self.rqidD[rqid]
        df = self.convertNPtoDF()
        if TRName == self.strTRCFChart:
            ix = self.lstSymbols_Near_Month.index(self.strCurrentSymbol)
            settle_date = self.dfCFutMst[self.dfCFutMst['단축코드']==self.strCurrentSymbol]['최종거래일'].values[0]
            maturity = settle_date[2:6]
            settle_date = dt.datetime.strptime(settle_date, '%Y%m%d').strftime('%Y-%m-%d')
        elif TRName == self.strTRCFNChart:
            ix = self.lstSymbols_Continuous.index(self.strCurrentSymbol)
            settle_date = ''
            maturity = '0000'

        # Set table
        dfResult = pd.DataFrame(None, columns=['BASE_DATETIME', 'NIX', 'ASSET_NAME', 'ASSET_TYPE', 'MATURITY', 'PRICE', 'VOLUME', 'SETTLE_DATE'])
        dfResult['BASE_DATETIME'] = df['Date'] + ' ' + df['Time']
        dfResult['BASE_DATETIME'] = dfResult['BASE_DATETIME'].astype('datetime64[ns]', copy=False)
        dfResult['NIX'] = dfResult.sort_index(ascending=False).index
        dfResult['ASSET_NAME'] = self.lstAssetName[ix]
        dfResult['ASSET_TYPE'] = self.lstAssetType[ix]
        dfResult['MATURITY'] = maturity
        dfResult['SETTLE_DATE'] = settle_date
        dfResult['PRICE'] = df['Close']
        dfResult['VOLUME'] = df['Vol']
        
        idx = dfResult[dfResult['VOLUME']==0].index
        dfResult = dfResult.drop(idx)

        # Insert to DB
        # dfResult['BASE_DATETIME'] = dfResult['BASE_DATETIME'].map(str)
        query = 'INSERT INTO market_data_tick VALUES (:1, :2, :3, :4, :5, :6, :7, :8)'
        self.instDB.executemany(query, dfResult.values.tolist())
        self.instDB.commit()


    def convertNPtoDF(self):
        'ndarray 데이터를 DataFrame 형식으로 변환'
        df = pd.DataFrame(self.Historical, columns=self.Historical.dtype.names)
        for i in df:    # 신한i Indi특 이진 데이터 Trim
            if str(df[i][0])[:1] == 'b':
                df[i] = list(map(lambda x: str(x).split("'")[1], df[i].values))
        
        return df


    def ReceiveData(self, rqid):
        '데이터 수신시 호출'
        TRName = self.rqidD[rqid]

        # 상품선물종목코드 수신(전종목)
        if TRName == self.strCFutMst:
            nCnt = self.IndiTR.dynamicCall("GetMultiRowCount()")    # multi row 개수를 리턴
            if nCnt > 0:
                for i in range(0, nCnt):    # 항목별 데이터
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

                    self.dfCFutMst = self.dfCFutMst.append(dictCFutMst, ignore_index=True)

            self.event_loop.exit()

        # 차트 데이터 수신
        elif (TRName == self.strTRCFChart) or (TRName == self.strTRCFNChart):
            nCnt = self.IndiTR.dynamicCall("GetMultiRowCount()")
            if nCnt > 0:
                self.Historical = np.empty([nCnt], dtype=self.Historicaldt)
                for i in range(0, nCnt):
                    self.Historical[i]['Date'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 0)
                    self.Historical[i]['Time'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 1)
                    self.Historical[i]['Open'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 2)
                    self.Historical[i]['High'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 3)
                    self.Historical[i]['Low'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 4)
                    self.Historical[i]['Close'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 5)
                    self.Historical[i]['Vol'] = self.IndiTR.dynamicCall("GetMultiData(int, int)", i, 9)

                if self.strRqDataType == 'D':   # 일봉
                    self.procDaily(rqid)
                elif self.strRqDataType == '1': # 분봉
                    self.procMinute(rqid)
                elif self.strRqDataType == 'T': # 틱
                    if nCnt < 9999:
                        self.procTick(rqid)

            self.event_loop.exit()

        self.rqidD.__delitem__(rqid)


    def ReceiveSysMsg(self, MsgID):
        '시스템 메시지 수신시 호출'
        if MsgID == 11:
            self.boolSysReady = True
            self.event_loop.exit()
        else:
            print("System Message Received = ", MsgID)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    IndiWindow = IndiWindow()
    IndiWindow.show()
    app.exec_()