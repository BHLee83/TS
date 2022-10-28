from threading import Lock
import pandas as pd
import numpy as np
from datetime import datetime as dt

from DB.dbconn import oracleDB

from SHi_indi.order import Order



class SingletonMeta(type):

    _instances = {}

    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]



class Strategy(metaclass=SingletonMeta):

    instDB = oracleDB('oraDB1')    # DB 연결

    dfCFutMst = pd.DataFrame(None)
    dfStrategyInfo = pd.DataFrame(None)
    dfProductInfo = pd.DataFrame(None)
    lstPriceInfo = []

    lstMktData = []
    strStartDate = "00000000"
    strEndDate = "99999999"
    strRqCnt = "100"

    lstOrderInfo = []
    lstOrderInfo_Net = []
    
    def __init__() -> None:
        pass


    # Product info. from DB
    def setProductInfo(productCode):
        strQuery = "SELECT * FROM product_info"
        Strategy.dfProductInfo = Strategy.instDB.query_to_df(strQuery, 100)


    def setProductCode(lstUnderId):
        lstProductCode = []
        for i in lstUnderId:
            for j in Strategy.dfCFutMst.index:
                if i == Strategy.dfCFutMst['기초자산ID'][j]:
                    lstProductCode.append(Strategy.dfCFutMst['단축코드'][j])
                    break

        return lstProductCode


    def setTimeFrame(lstTimeFrame): # for SHi-indi spec.
        lstTimeWnd = []
        lstTimeIntrvl = []
        for i in lstTimeFrame:
            if i == 'M' or i == 'W' or i == 'D':
                lstTimeWnd.append(i)
                lstTimeIntrvl.append('1')
            else:
                lstTimeWnd.append('1')
                lstTimeIntrvl.append(i.split('m')[0])

        return [lstTimeWnd, lstTimeIntrvl]


    # Strategy settings info. from DB
    def setStrategyInfo(productCode):
        underyling_id = Strategy.dfCFutMst['기초자산ID'][Strategy.dfCFutMst['단축코드']==productCode].drop_duplicates().values[0]
        strQuery = f"SELECT * FROM strategy_info WHERE underlying_id LIKE '%{underyling_id}%'"
        Strategy.dfStrategyInfo = Strategy.instDB.query_to_df(strQuery, 100)
        if len(Strategy.dfStrategyInfo) != 0:
            Strategy.dfStrategyInfo['TR_UNIT'] = Strategy.dfStrategyInfo['TR_UNIT'].astype(str)


    # def setOrder(objStrategy, qty, price, order_type):
    def setOrder(objStrategy, productCode:str, direction:str, qty:int, price:float):
        direction = direction.upper()
        if direction == 'B' or direction == 'BUY' or direction == 'LONG' or direction == 'EXITSHORT':
            d = 1
        elif direction == 'S' or direction == 'SELL' or direction == 'SHORT' or direction == 'EXITLONG':
            d = -1

        if price == 0:
            order_type = 'M'    # 시장가
        else:
            order_type = 'L'    # 지정가

        dictOrderInfo = {}
        dictOrderInfo['OCCUR_TIME'] = dt.now().time()
        dictOrderInfo['STRATEGY_OBJECT'] = objStrategy
        dictOrderInfo['STRATEGY_NAME'] = objStrategy.dfInfo['NAME']
        dictOrderInfo['PRODUCT_CODE'] = productCode
        dictOrderInfo['QUANTITY'] = qty * d
        # 일단은 order_type 고려하지 않음 (때문에 price도 고려대상 X)
        dictOrderInfo['ORDER_TYPE'] = order_type
        dictOrderInfo['ORDER_PRICE'] = price

        Strategy.lstOrderInfo.append(dictOrderInfo)


    def executeOrder(instInterface, acntCode:str, acntPwd:str, PriceInfo):
        if Strategy.lstOrderInfo != []:    # 주문할게 있으면
            Strategy.nettingOrder() # 네팅해서 주문
            objOrder = Order(instInterface)
            for i in Strategy.lstOrderInfo_Net:
                if i['QUANTITY'] == 0:  # 네팅 수량이 0이면 주문 패스
                    continue
                if PriceInfo == None:
                    if i['QUANTITY'] > 0:
                        direction = '02'
                    elif i['QUANTITY'] < 0:
                        direction = '01'
                    objOrder.order(acntCode, acntPwd, i['PRODUCT_CODE'], abs(i['QUANTITY']), 0, direction, 'M')
                else:
                    if i['QUANTITY'] > 0: # 일단 현재가로 주문 (실거래시 상대호가 등으로 바꿀것)
                        price = PriceInfo['현재가']
                        direction = '02'
                    elif i['QUANTITY'] < 0:
                        price = PriceInfo['현재가']
                        direction = '01'
                    objOrder.order(acntCode, acntPwd, i['PRODUCT_CODE'], abs(i['QUANTITY']), price, direction)

            objOrder.iqrySettle(acntCode, acntPwd, format(dt.now().date(), '%Y%m%d'))
            instInterface.setTwOrderInfoUI()

        
    def nettingOrder(): # 주문 통합하기
        Strategy.lstOrderInfo_Net = []
        for i in Strategy.lstOrderInfo:
            if Strategy.lstOrderInfo_Net == []:
                Strategy.addToNettingOrder(i['PRODUCT_CODE'], i['QUANTITY'])
            else:
                for j in Strategy.lstOrderInfo_Net:
                    if j['PRODUCT_CODE'] == i['PRODUCT_CODE']:
                        j['QUANTITY'] += i['QUANTITY']
                        break
                if j == Strategy.lstOrderInfo_Net[-1]:
                    Strategy.addToNettingOrder(i['PRODUCT_CODE'], i['QUANTITY'])


    def addToNettingOrder(productCode, qty):
        dictOrderInfo = {}
        dictOrderInfo['OCCUR_TIME'] = dt.now().time()
        dictOrderInfo['PRODUCT_CODE'] = productCode
        dictOrderInfo['QUANTITY'] = qty
        Strategy.lstOrderInfo_Net.append(dictOrderInfo)


    def setSettleInfo(settleInfo):
        # Strategy.lstOrderInfo.clear()
        # Strategy.lstOrderInfo_Net.clear()
        pass


    def setHistData(productCode, period, histData):
        if len(histData) == 1:
            for i in Strategy.lstMktData:
                if (i['PRODUCT_CODE'] == productCode) and (i['PERIOD'] == period):
                    i['VALUES'] = np.concatenate((histData.copy(), i['VALUES']))

        elif Strategy.getHistData(productCode, period) == False:
            dictData = {}
            dictData['PRODUCT_CODE'] = productCode
            dictData['PERIOD'] = period
            dictData['VALUES'] = histData.copy()
            Strategy.lstMktData.append(dictData)


    def getHistData(productCode, period):
        for i in Strategy.lstMktData:
            if (i['PRODUCT_CODE'] == productCode) and (i['PERIOD'] == period):
                return i['VALUES']
        
        return False

    
    def chkPrice(price, PriceInfo):
        for i in Strategy.lstPriceInfo:
            if i['단축코드'] == PriceInfo['단축코드']:
                if i['체결시간'][2:4] != PriceInfo['체결시간'][2:4]:    # hour or min. 이 바뀌면
                    Strategy.addToHistData(price, PriceInfo)
                i = PriceInfo
                return

        Strategy.lstPriceInfo.append(PriceInfo.copy())


    def addToHistData(price, PriceInfo):
        for i in Strategy.lstMktData:
            if i['PRODUCT_CODE'] == PriceInfo['단축코드'].decode():
                settleMin = int(PriceInfo['체결시간'][2:4])
                if settleMin == 0:
                    settleMin = 60

                try:
                    p = int(i['PERIOD'])
                except:
                    continue

                if settleMin % p == 0:
                    price.rqHistData(i['PRODUCT_CODE'], '1', i['PERIOD'], Strategy.strStartDate, Strategy.strEndDate, '1')


    def convertTimeFrame(timeFrame, timeIntrv): # TimeFrame: 'M', 'W', 'D', '1' / TimeInterval(min): '5', '10', ...
        if timeFrame == '1':
            return timeIntrv
        else:
            return timeFrame

    
    def convertNPtoDF(ndarray):
        df = pd.DataFrame(ndarray, columns=ndarray.dtype.names)
        for i in df:    # 신한i Indi특 스트링 데이터 Trim
            if str(df[i][0])[:1] == 'b':
                df[i] = str(df[i]).split("'")[1]
        
        return df