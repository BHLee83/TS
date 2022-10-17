from time import time
from Strategy.indicator import Indicator
import pandas as pd
import numpy as np
from datetime import datetime

class Strategy():
    commonData = pd.DataFrame(None, columns={'Product_Code', 'M', 'W', 'D', '400', '360', '300', '240', '180', '120', '60', '30', '20', '15', '10', '5', '3', '2', '1'})
    
    def __init__(self) -> None:
        pass


class TS_RB_0001(Strategy, Indicator):
    def __init__(self, productCode, timeFrame, timeIntrv, startDate, endDate) -> None:
        super().__init__()

        self.boolRTbypass = True    # 실시간 시세 필요 여부
        self.strProductCode = productCode
        self.strTimeFrame = timeFrame
        self.strTimeIntrv = timeIntrv
        self.strStartDate = startDate
        self.strEndDate = endDate
        
        if self.strTimeFrame == '1':
            self.rqPeriod = self.strTimeIntrv
        else:
            self.rqPeriod = self.strTimeFrame

        self.nPeriod1 = 5
        self.nPeriod2 = 20

    def setHistoricalData(self, historicalData):
        df = super().commonData
        if self.getHistoricalData().empty:
            df.loc[len(super().commonData)] = np.nan
            df['Product_Code'].loc[len(df)-1] = self.strProductCode
        
        df[self.rqPeriod][df['Product_Code']==self.strProductCode] = historicalData
        print(df[self.rqPeriod][df['Product_Code']==self.strProductCode])

    def getHistoricalData(self):
        df = super().commonData
        return df[self.rqPeriod][df['Product_Code']==self.strProductCode]

    def execute(self):
        if True:
            pass
        elif self.boolRTbypass:
            return
        else:
            return