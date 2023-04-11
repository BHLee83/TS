from threading import Lock
import pandas as pd
import numpy as np
import datetime as dt
import math
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
    def chkAbnormOrder(acnt_num, ordInfo, direction):
        nChkCnt = 10
        strUnder_ID = Strategy.dfCFutMst['기초자산ID'][Strategy.dfCFutMst['단축코드']==ordInfo['PRODUCT_CODE']].values[0]
        strAsset_Code = Strategy.dfProductInfo['ASSET_CODE'][Strategy.dfProductInfo['UNDERLYING_ID']==strUnder_ID].values[0]
        dictOrder = {}
        dictOrder['ACCOUNT'] = acnt_num
        dictOrder['PRODUCT'] = strAsset_Code
        dictOrder['QUANTITY'] = ordInfo['QUANTITY']
        dictOrder['PRICE'] = ordInfo['PRICE']
        dictOrder['DIRECTION'] = direction
        dictOrder['OCCUR_TIME'] = dt.datetime.now()
        dictOrder['TIME_DIFF'] = dt.timedelta(0)
        Strategy.dfOrderInfo_All = Strategy.dfOrderInfo_All.append(dictOrder, ignore_index=True)
        if len(Strategy.dfOrderInfo_All) > 1:
            if Strategy.dfOrderInfo_All['PRODUCT'].iloc[-1] == Strategy.dfOrderInfo_All['PRODUCT'].iloc[-2]:
                Strategy.dfOrderInfo_All.loc[len(Strategy.dfOrderInfo_All)-1, 'TIME_DIFF'] = Strategy.dfOrderInfo_All['OCCUR_TIME'].iloc[-1] - Strategy.dfOrderInfo_All['OCCUR_TIME'].iloc[-2]

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
            if direction == 'B' or direction == 'BUY' or direction == 'LONG' or direction == 'EXITSHORT' or direction == 'ES':
                d = 1
            elif direction == 'S' or direction == 'SELL' or direction == 'SHORT' or direction == 'EXITLONG' or direction == 'EL':
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
            elif PriceInfo == None:
                if i['QUANTITY'] > 0:
                    i['PRICE'] = 0
                    direction = '02'
                elif i['QUANTITY'] < 0:
                    i['PRICE'] = 0
                    direction = '01'
                logging.info('실주문: %s, %s, %s, %s', acntCode, i['PRODUCT_CODE'], i['QUANTITY'], 'M')
                # ret = interface.objOrder.order(acntCode, acntPwd, i['PRODUCT_CODE'], abs(i['QUANTITY']), i['PRICE'], direction, 'M')
                ret = interface.objOrder.order(acntCode, acntPwd, i, direction, 'M')
                if ret is False:
                    logging.warning('주문 실패!')
                    return ret
            else:
                if i['QUANTITY'] > 0: # TODO: 일단 상대1호가로 주문 (추후 중간가격 주문이나 주문수량 고려 등 필요)
                    i['PRICE'] = PriceInfo['매도1호가']
                    direction = '02'
                elif i['QUANTITY'] < 0:
                    i['PRICE'] = PriceInfo['매수1호가']
                    direction = '01'
                logging.info('실주문: %s, %s, %s, %s', acntCode, i['PRODUCT_CODE'], i['QUANTITY'], i['PRICE'])
                # ret = interface.objOrder.order(acntCode, acntPwd, i['PRODUCT_CODE'], abs(i['QUANTITY']), i['PRICE'], direction)
                ret = interface.objOrder.order(acntCode, acntPwd, i, direction)
                if ret is False:
                    logging.warning('주문 실패!')
                    return ret

        interface.setTwOrderInfoUI()    # 전략별 주문 요청내역 출력
        # Strategy.timer = QTimer()
        # Strategy.timer.singleShot(3000, interface.event_loop.quit)
        # interface.event_loop.exec_()

        
    def nettingOrder(): # 주문 통합하기
        Strategy.lstOrderInfo_Net = []
        for i, v in enumerate(Strategy.lstOrderInfo):
            v['ID'] = str(i)    # 기존 주문 넘버링
            if Strategy.lstOrderInfo_Net == []:
                Strategy.addToNettingOrder(v)
            else:
                for j in Strategy.lstOrderInfo_Net:
                    if all([j['PRODUCT_CODE'] == v['PRODUCT_CODE'], j['ORDER_TYPE'] == v['ORDER_TYPE']]):
                        j['QUANTITY'] += v['QUANTITY']
                        j['NET_ID'] += ',' + v['ID']   # 네팅된 주문의 ID 목록
                        break
                if j == Strategy.lstOrderInfo_Net[-1]:
                    Strategy.addToNettingOrder(v)


    def addToNettingOrder(v):
        dictOrderInfo = {}
        dictOrderInfo['OCCUR_TIME'] = dt.datetime.now().time()
        dictOrderInfo['PRODUCT_CODE'] = v['PRODUCT_CODE']
        dictOrderInfo['QUANTITY'] = v['QUANTITY']
        dictOrderInfo['ORDER_TYPE'] = v['ORDER_TYPE']   # 일단 주문타입(지정가/시장가) 구분만 고려 (시장가 문제없음)
        # TODO: 가격을 고려하지 않아 지정가는 executeOrder에서 정의된 호가로만 주문 발생. 미리 내놓는 주문의 가격 등은 고려되지 않는 문제 있음
        dictOrderInfo['NET_ID'] = v['ID']
        Strategy.lstOrderInfo_Net.append(dictOrderInfo)


    def setSettleInfo(settleInfo):
        # Strategy.lstOrderInfo.clear()
        # Strategy.lstOrderInfo_Net.clear()
        pass


    def setHistData(productCode, timeframe, histData):
        if type(histData) == np.ndarray:    # indi 데이터인 경우
            df = Strategy.convertNPtoDF(histData)   # 타입 변환
            if all([len(df) > 2, timeframe != 'D', timeframe != 'W', timeframe != 'M']):  # 분봉인 경우 동시호가 데이터 제거
                df['시간'] = df['시간'].astype(int)
                t = 1500 + math.ceil(35.0/int(timeframe)) * int(timeframe)
                ix = df[(df['시간'] > t) & (df['시간'] < 1545)].index
                df = df.drop(ix)
            df = df.sort_index(ascending=False).reset_index().drop('index', axis=1)
        else:   # DB 데이터인 경우
            df = histData
        
        for i in Strategy.lstMktData:
            if (i['PRODUCT_CODE'] == productCode) and (i['TIMEFRAME'] == timeframe):    # 기존 데이터 있으면
                if len(df) <= 2:    # 추가 데이터인 경우(분봉 등) Add
                    i['VALUES'] = pd.concat([df.iloc[0], i['VALUES']]).reset_index().drop('index', axis=1)
                    return True
                else:   # TODO: 기존 데이터가 더 적은 경우인지 확인하고 덮어쓸지 판단해야 함
                    return False
                
        # 기존 데이터 없으면 추가
        dictData = {}
        # dictData['PRODUCT_N_CODE'] = productNCode
        dictData['PRODUCT_CODE'] = productCode
        dictData['TIMEFRAME'] = timeframe
        dictData['VALUES'] = df
        Strategy.lstMktData.append(dictData)

        return True
            

    def getHistData(productCode, timeframe, period:int=300):
        # 1. 리스트에 있으면 넘겨주고
        for i in Strategy.lstMktData:
            if (i['PRODUCT_CODE'] == productCode) and (i['TIMEFRAME'] == timeframe):
                return i['VALUES'].iloc[-period-1:].reset_index().drop('index', axis=1)

        # 2. 없으면 DB에서 조회 (조회는 갯수 상관없이 전체 조회)
        underyling_id = Strategy.dfCFutMst['기초자산ID'][Strategy.dfCFutMst['단축코드']==productCode].drop_duplicates().values[0]
        asset_name = Strategy.dfProductInfo['ASSET_NAME'][Strategy.dfProductInfo['UNDERLYING_ID']==underyling_id].values[0]
        table = 'market_data'
        cond = ''
        key = 'base_date'
        if all([timeframe != 'D', timeframe != 'W', timeframe != 'M']): # 분봉인 경우와 일봉 이상인 경우 테이블 나눠짐 (틱은 현재 고려 X)
            table += "_minute"
            cond += "AND timeframe = '" + timeframe + "'"
            key += "time"
        strQuery = f"SELECT * FROM {table} WHERE asset_name = '{asset_name}' AND maturity='0000' " + cond + f" ORDER BY {key}" # 0000: 연결선물
        dfRet = Strategy.instDB.query_to_df(strQuery, 9999)
        if len(dfRet) != 0:
            df = pd.DataFrame(None)
            df['일자'] = pd.to_datetime(dfRet[key.upper()]).dt.strftime('%Y%m%d')   # indi 형식으로 맞춤
            df['시간'] = pd.to_datetime(dfRet[key.upper()]).dt.strftime('%H%M%S')
            df['시가'] = dfRet['OPEN_PRICE'].astype(np.float32)
            df['고가'] = dfRet['HIGH_PRICE'].astype(np.float32)
            df['저가'] = dfRet['LOW_PRICE'].astype(np.float32)
            df['종가'] = dfRet['CLOSE_PRICE'].astype(np.float32)
            df['단위거래량'] = dfRet['VOLUME']
            if df.iloc[-1]['일자'] != Strategy.strToday:    # 계산 편의용 처리
                df.loc[len(df)] = df.iloc[-1]
                df.loc[len(df)-1, '일자'] = Strategy.strToday
            if df.iloc[-1]['시간'] != '000000':
                df.loc[len(df)-1, '시간'] = df.iloc[df[df['시간']==df.iloc[-1]['시간']].index[0]+1]['시간']
            Strategy.setHistData(productCode, timeframe, df)
            return df.iloc[-period-1:].reset_index().drop('index', axis=1)
        else:   # DB에도 없으면
            pass    # TODO: indi 조회
            return False
    

    def getPosition(id:str, code:str, type:str):
        if Strategy.dfPosition.empty:
            return 0
        else:
            try:
                df = Strategy.dfPosition['POSITION'][(Strategy.dfPosition['STRATEGY_ID']==id) \
                        & (Strategy.dfPosition['ASSET_CODE']==code) \
                        & (Strategy.dfPosition['ASSET_TYPE']==type)]
                if df.empty:
                    return 0
                else:
                    return df.values[0]
            except:
                return 0


    def getPrice(PriceInfo):
        dictData = {}
        dictData['일자'] = Strategy.strToday
        dictData['시간'] = PriceInfo['체결시간'].decode()
        dictData['시가'] = PriceInfo['시가']
        dictData['고가'] = PriceInfo['고가']
        dictData['저가'] = PriceInfo['저가']
        dictData['종가'] = PriceInfo['현재가']
        dictData['단위거래량'] = PriceInfo['누적거래량']    # TODO: 분봉의 경우 누적/단위 거래량 구분해야함

        return dictData


    def setPrice(PriceInfo):
        for i in Strategy.lstMktData:   # 데이터 리스트에서
            if i['PRODUCT_CODE'] == PriceInfo['단축코드']:  # 종목 찾아
                if i['TIMEFRAME'] == 'D':   # 일봉인 경우
                    if i['VALUES'].iloc[-1]['일자'] != Strategy.strToday:   # 당일자 정보 없으면
                        i['VALUES'].loc[len(i['VALUES'])] = Strategy.getPrice(PriceInfo)    # 신규 추가
                    else:   # 있으면
                        i['VALUES'].loc[len(i['VALUES'])-1] = Strategy.getPrice(PriceInfo) # 업데이트
                    i['VALUES'].loc[len(i['VALUES'])-1, '시간'] = '000000'
                else:   # 분봉인 경우 (주봉/월봉 일단 제외)
                    if PriceInfo['체결시간'].decode() == '154500':
                        continue

                    tf = int(i['TIMEFRAME'])
                    h = int(PriceInfo['체결시간'][0:2])
                    m = int(PriceInfo['체결시간'][2:4])
                    m = int(m / tf) * tf + tf
                    if m >= 60:
                        h += 1
                        m -= 60
                    t = format(h, '02') + format(m, '02') + '00'
                    if i['VALUES'].iloc[-1]['일자'] != Strategy.strToday:   # 당일자 정보 없으면
                        i['VALUES'].loc[len(i['VALUES'])] = Strategy.getPrice(PriceInfo)    # 신규 추가
                        i['VALUES'].loc[len(i['VALUES'])-1, '시간'] = t
                    else:   # 있는데
                        if i['VALUES'].iloc[-1]['시간'] != t:   # 최신 분봉이 아니면
                            i['VALUES'].loc[len(i['VALUES'])] = Strategy.getPrice(PriceInfo)    # 신규 추가
                            l = len(i['VALUES']) - 1
                            i['VALUES'].loc[l, '시간'] = t
                            i['VALUES'].loc[l, '시가'] = PriceInfo['현재가']
                            i['VALUES'].loc[l, '고가'] = PriceInfo['현재가']
                            i['VALUES'].loc[l, '저가'] = PriceInfo['현재가']
                        else:   # 있으면 업데이트
                            l = len(i['VALUES']) - 1
                            if i['VALUES'].loc[l, '고가'] < PriceInfo['현재가']:
                                i['VALUES'].loc[l, '고가'] = PriceInfo['현재가']
                            if i['VALUES'].loc[l, '저가'] > PriceInfo['현재가']:
                                i['VALUES'].loc[l, '저가'] = PriceInfo['현재가']
                            i['VALUES'].loc[l, '종가'] = PriceInfo['현재가']


    def chkPrice(interface, PriceInfo):
        for i, v in enumerate(Strategy.lstPriceInfo):
            if v['단축코드'] == PriceInfo['단축코드']:
                if v['체결시간'][2:4] != PriceInfo['체결시간'][2:4]:  # min. 이 바뀌면
                    Strategy.addToHistData(interface, PriceInfo)
                Strategy.lstPriceInfo[i] = PriceInfo.copy()
                return

        Strategy.lstPriceInfo.append(PriceInfo.copy())


    def addToHistData(interface, PriceInfo):
        for i in Strategy.lstMktData:
            if i['PRODUCT_CODE'] == PriceInfo['단축코드']:
                settleMin = int(PriceInfo['체결시간'][2:4])
                if settleMin == 0:
                    settleMin = 60

                if all([i['TIMEFRAME'] != 'D', i['TIMEFRAME'] != 'W', i['TIMEFRAME'] != 'M']):
                    p = int(i['TIMEFRAME'])

                    if settleMin % p == 0:
                        interface.price.rqHistData(i['PRODUCT_CODE'], '1', i['TIMEFRAME'], Strategy.strStartDate, Strategy.strEndDate, '2')
                        # Strategy.timer = QTimer()
                        # Strategy.timer.singleShot(2000, interface.event_loop.quit)
                        # interface.event_loop.exec_()


    def convertTimeFrame(timeFrame, timeIntrv): # TimeFrame: 'M', 'W', 'D', '1' / TimeInterval(min): '5', '10', ...
        if timeFrame == '1':
            return timeIntrv
        else:
            return timeFrame

    
    def convertNPtoDF(ndarray):
        df = pd.DataFrame(ndarray, columns=ndarray.dtype.names)
        for i in df:    # 신한i Indi특 이진 데이터 Trim
            if type(df[i][0]) == bytes:
                df[i] = list(map(lambda x: str(x).split("'")[1], df[i].values))
        
        return df