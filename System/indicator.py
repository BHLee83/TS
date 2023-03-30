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


    def ADX(high:pd.Series, low:pd.Series, close:pd.Series, period:int, ascending=True) -> pd.Series:
        if ascending:
            HIGH = high
            LOW = low
            CLOSE = close
        else:
            HIGH = high.sort_index(ascending=ascending)
            LOW = low.sort_index(ascending=ascending)
            CLOSE = close.sort_index(ascending=ascending)

        plus_dm = HIGH.diff()
        minus_dm = LOW.diff()
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm > 0] = 0
        
        atr = Indicator.ATR(HIGH, LOW, CLOSE, period, ascending)
        
        plus_di = 100 * (plus_dm.ewm(alpha = 1/period).mean() / atr)
        minus_di = abs(100 * (minus_dm.ewm(alpha = 1/period).mean() / atr))
        dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di)) * 100
        adx = ((dx.shift(1) * (period - 1)) + dx) / period
        # adx_smooth = adx.ewm(alpha = 1/period).mean()
        # return adx_smooth
        return adx


    def CCI(high:pd.Series, low:pd.Series, close:pd.Series, period:int, ascending=True) -> pd.Series:
        if ascending:
            HIGH = high
            LOW = low
            CLOSE = close
        else:
            HIGH = high.sort_index(ascending=ascending)
            LOW = low.sort_index(ascending=ascending)
            CLOSE = close.sort_index(ascending=ascending)

        p = (HIGH + LOW + CLOSE) / 3
        sma = p.rolling(window=period).mean()
        md = p.rolling(window=period).apply(lambda x: pd.Series(x).mad())
        cci = (p - sma) / (0.015 * md)
        
        return cci