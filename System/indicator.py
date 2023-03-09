import pandas as pd
import numpy as np



class Indicator():
    def __init__(self) -> None:
        pass


    def MA(data:pd.Series, period:int, ascending=True) -> pd.DataFrame:
        if ascending:
            return data.rolling(window=period).mean().copy()
        else:
            tmp = data.sort_index(ascending=False)
            return tmp.rolling(window=period).mean().copy()

    
    def ATR(high:pd.Series, low:pd.Series, close:pd.Series, period:int, ascending=True) -> pd.Series:
        if ascending:
            HIGH = high
            LOW = low
            CLOSE = close
        else:
            HIGH = high.sort_index(ascending=ascending)
            LOW = low.sort_index(ascending=ascending)
            CLOSE = close.sort_index(ascending=ascending)
        
        HIGH_LOW = HIGH - LOW
        HIGH_CLOSE = np.abs(HIGH - CLOSE.shift())
        LOW_CLOSE = np.abs(LOW - CLOSE.shift())
        TR = pd.Series(np.array([HIGH_LOW, HIGH_CLOSE, LOW_CLOSE]).max(axis=0))
        
        return TR.ewm(span=period).mean()