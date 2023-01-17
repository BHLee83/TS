from threading import Lock
import pandas as pd
import numpy as np
import datetime as dt
import logging

from PyQt5.QtCore import QTimer

from DB.dbconn import oracleDB



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

    # logger = logging.getLogger(__name__)  # 로그 생성

    strToday = ''
    strT_1 = ''
    instDB = oracleDB('oraDB1')    # DB 연결

    dfCFutMst = pd.DataFrame(None)
    dfStrategyInfo = pd.DataFrame(None)
    dfProductInfo = pd.DataFrame(None)
    dfPosition = pd.DataFrame(None)
    lstPriceInfo = []

    lstMktData = []
    strStartDate = "00000000"
    strEndDate = "99999999"
    strRqCnt = "420"
    
    lstOrderInfo = []
    lstOrderInfo_Net = []
    dictOrderInfo = {}
    dictOrderInfo_Net = {}
    dictOrderInfo_Rcv = {}
    nOrderCnt = 0

    dfOrderInfo_All = pd.DataFrame(None)

    timer = None


    def __init__() -> None:
        Strategy.setProductInfo()


    # Product info. from DB
    def setProductInfo():
        strQuery = "SELECT * FROM product_info"
        Strategy.dfProductInfo = Strategy.instDB.query_to_df(strQuery, 100)


    def setProductCode(lstUnderId):
        lstProductCode = []
        for i in lstUnderId:
            for j in Strategy.dfCFutMst.index:
                if i == Strategy.dfCFutMst['기초자산ID'][j]:
                    if Strategy.dfCFutMst['최종거래일'][j] != Strategy.strToday:
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
        strQuery = f"SELECT * FROM strategy_info WHERE underlying_id LIKE '%{underyling_id}%' ORDER BY name"
        Strategy.dfStrategyInfo = Strategy.instDB.query_to_df(strQuery, 100)
        if len(Strategy.dfStrategyInfo) != 0:
            Strategy.dfStrategyInfo['TR_UNIT'] = Strategy.dfStrategyInfo['TR_UNIT'].astype(str)


    # Abnormal Order Check!
    def chkAbnormOrder(acnt_num, code, qty, price, direction):
        nChkCnt = 10
        strUnder_ID = Strategy.dfCFutMst['기초자산ID'][Strategy.dfCFutMst['단축코드']==code].values[0]
        strAsset_Code = Strategy.dfProductInfo['ASSET_CODE'][Strategy.dfProductInfo['UNDERLYING_ID']==strUnder_ID].values[0]
        dictOrder = {}
        dictOrder['ACCOUNT'] = acnt_num
        dictOrder['PRODUCT'] = strAsset_Code
        dictOrder['QUANTITY'] = qty
        dictOrder['PRICE'] = price
        dictOrder['DIRECTION'] = direction
        dictOrder['OCCUR_TIME'] = dt.datetime.now()
        dictOrder['TIME_DIFF'] = dt.timedelta(0)
        Strategy.dfOrderInfo_All = Strategy.dfOrderInfo_All.append(dictOrder, ignore_index=True)
        if len(Strategy.dfOrderInfo_All) > 1:
            if Strategy.dfOrderInfo_All['PRODUCT'].iloc[-1] == Strategy.dfOrderInfo_All['PRODUCT'].iloc[-2]:
                Strategy.dfOrderInfo_All['TIME_DIFF'].iloc[-1] = Strategy.dfOrderInfo_All['OCCUR_TIME'].iloc[-1] - Strategy.dfOrderInfo_All['OCCUR_TIME'].iloc[-2]

        # 1. 상품별 1회 최대 주문 수량 초과
        if dictOrder['QUANTITY'] > \
            Strategy.dfProductInfo['MAX_QTY_PER_ORDER'][Strategy.dfProductInfo['ASSET_CODE']==strAsset_Code].values[0]:
            return True

        # 2. 상품별 총한도 초과
        elif len(Strategy.dfPosition) > 0:
            if Strategy.dfPosition['POSITION'][Strategy.dfPosition['ASSET_CODE']==strAsset_Code].sum() + dictOrder['QUANTITY'] > \
                Strategy.dfProductInfo['MAX_QTY'][Strategy.dfProductInfo['ASSET_CODE']==strAsset_Code].values[0]:
                return True

        # 3. 단시간 내 연속 주문
        elif len(Strategy.dfOrderInfo_All) > nChkCnt:   # nChkCnt 번을 초과하여
            if all(Strategy.dfOrderInfo_All.tail(nChkCnt)['PRODUCT'] == strAsset_Code):   # 같은 종목을 연달아 주문하고
                if all(Strategy.dfOrderInfo_All.tail(nChkCnt)['TIME_DIFF'] < dt.timedelta(seconds=1)):    # 매번 1초 이내에 발생했으면
                    return True
                    
        else:
            return False


    def setOrder(strategyName:str, productCode:str, direction:str, qty:int, price:float):
        if qty != 0:
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
            dictOrderInfo['OCCUR_TIME'] = dt.datetime.now().time()
            dictOrderInfo['STRATEGY_NAME'] = strategyName
            dictOrderInfo['PRODUCT_CODE'] = productCode
            dictOrderInfo['UNDERLYING_ID'] = Strategy.dfCFutMst['기초자산ID'][Strategy.dfCFutMst['단축코드']==productCode].values[0]
            dictOrderInfo['ASSET_CODE'] = Strategy.dfProductInfo['ASSET_CODE'][Strategy.dfProductInfo['UNDERLYING_ID']==dictOrderInfo['UNDERLYING_ID']].values[0]
            dictOrderInfo['ASSET_CLASS'] = Strategy.dfProductInfo['ASSET_CLASS'][Strategy.dfProductInfo['ASSET_CODE']==dictOrderInfo['ASSET_CODE']].values[0]
            dictOrderInfo['ASSET_NAME'] = Strategy.dfProductInfo['ASSET_NAME'][Strategy.dfProductInfo['ASSET_CODE']==dictOrderInfo['ASSET_CODE']].values[0]
            dictOrderInfo['ASSET_TYPE'] = Strategy.dfProductInfo['ASSET_TYPE'][Strategy.dfProductInfo['ASSET_CODE']==dictOrderInfo['ASSET_CODE']].values[0]
            dictOrderInfo['MATURITY_DATE'] = Strategy.dfCFutMst['최종거래일'][Strategy.dfCFutMst['단축코드']==productCode].values[0]
            dictOrderInfo['MATURITY'] = dictOrderInfo['MATURITY_DATE'][2:6]
            dictOrderInfo['QUANTITY'] = qty * d
            # 일단은 order_type 고려하지 않음 (때문에 price도 고려대상 X)
            dictOrderInfo['ORDER_TYPE'] = order_type
            dictOrderInfo['ORDER_PRICE'] = price

            Strategy.lstOrderInfo.append(dictOrderInfo)


    def executeOrder(interface, PriceInfo):
        acntCode = interface.strAcntCode
        acntPwd = interface.dfAcntInfo['Acnt_Pwd'][0]
        Strategy.nettingOrder() # 네팅해서 주문
        for i in Strategy.lstOrderInfo_Net:
            if i['QUANTITY'] == 0:  # 네팅 수량이 0이면 주문 패스
                logging.info('네팅 수량 0으로 실주문 없음')
                continue
            if PriceInfo == None:
                if i['QUANTITY'] > 0:
                    i['PRICE'] = 0
                    direction = '02'
                elif i['QUANTITY'] < 0:
                    i['PRICE'] = 0
                    direction = '01'
                logging.info('실주문: %s, %s, %s, %s', acntCode, i['PRODUCT_CODE'], i['QUANTITY'], 'M')
                ret = interface.objOrder.order(acntCode, acntPwd, i['PRODUCT_CODE'], abs(i['QUANTITY']), i['PRICE'], direction, 'M')
                if ret is False:
                    logging.warning('주문 실패!')
                    return ret
            else:
                if i['QUANTITY'] > 0: # FIXME: 일단 현재가로 주문 (실거래시 상대호가 등으로 바꿀것)
                    i['PRICE'] = PriceInfo['현재가']
                    direction = '02'
                elif i['QUANTITY'] < 0:
                    i['PRICE'] = PriceInfo['현재가']
                    direction = '01'
                logging.info('실주문: %s, %s, %s, %s', acntCode, i['PRODUCT_CODE'], i['QUANTITY'], i['PRICE'])
                ret = interface.objOrder.order(acntCode, acntPwd, i['PRODUCT_CODE'], abs(i['QUANTITY']), i['PRICE'], direction)
                if ret is False:
                    logging.warning('주문 실패!')
                    return ret

        interface.setTwOrderInfoUI()    # 전략별 주문 요청내역 출력
        Strategy.timer = QTimer()
        Strategy.timer.singleShot(2000, interface.event_loop.quit)
        interface.event_loop.exec_()

        
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
        dictOrderInfo['OCCUR_TIME'] = dt.datetime.now().time()
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

        else:
            ret = Strategy.getHistData(productCode, period)
            if type(ret) == bool:
                if ret == False:
                    dictData = {}
                    # dictData['PRODUCT_N_CODE'] = productNCode
                    dictData['PRODUCT_CODE'] = productCode
                    dictData['PERIOD'] = period
                    dictData['VALUES'] = histData.copy()
                    Strategy.lstMktData.append(dictData)
            

    def getHistData(productCode, period):
        for i in Strategy.lstMktData:
            if (i['PRODUCT_CODE'] == productCode) and (i['PERIOD'] == period):
                return i['VALUES']
        
        return False

    
    def chkPrice(interface, PriceInfo):
        for i, v in enumerate(Strategy.lstPriceInfo):
            if v['단축코드'] == PriceInfo['단축코드']:
                if v['체결시간'].decode()[2:4] != PriceInfo['체결시간'].decode()[2:4]:  # min. 이 바뀌면
                    Strategy.addToHistData(interface, PriceInfo)
                Strategy.lstPriceInfo[i] = PriceInfo.copy()
                return

        Strategy.lstPriceInfo.append(PriceInfo.copy())


    def addToHistData(interface, PriceInfo):
        for i in Strategy.lstMktData:
            if i['PRODUCT_CODE'] == PriceInfo['단축코드'].decode():
                settleMin = int(PriceInfo['체결시간'].decode()[2:4])
                if settleMin == 0:
                    settleMin = 60

                try:
                    p = int(i['PERIOD'])
                except:
                    continue

                if settleMin % p == 0:
                    interface.price.rqHistData(i['PRODUCT_CODE'], '1', i['PERIOD'], Strategy.strStartDate, Strategy.strEndDate, '1')
                    Strategy.timer = QTimer()
                    Strategy.timer.singleShot(2000, interface.event_loop.quit)
                    interface.event_loop.exec_()


    def convertTimeFrame(timeFrame, timeIntrv): # TimeFrame: 'M', 'W', 'D', '1' / TimeInterval(min): '5', '10', ...
        if timeFrame == '1':
            return timeIntrv
        else:
            return timeFrame

    
    def convertNPtoDF(ndarray):
        df = pd.DataFrame(ndarray, columns=ndarray.dtype.names)
        for i in df:    # 신한i Indi특 이진 데이터 Trim
            if str(df[i][0])[:1] == 'b':
                df[i] = list(map(lambda x: str(x).split("'")[1], df[i].values))
        
        return df