import pandas as pd
import numpy as np



class Indicator():
    def __init__(self) -> None:
        pass


    def MA(data:pd.Series, period:int) -> pd.DataFrame:
            return data.rolling(window=period).mean().copy()

    
    def ATR(high:pd.Series, low:pd.Series, close:pd.Series, period:int) -> pd.Series:
        high_low = high - low
        high_close = np.abs(high - close.shift())
        low_close = np.abs(low - close.shift())
        tr = pd.Series(np.array([high_low, high_close, low_close]).max(axis=0))
        
        return tr.ewm(span=period).mean()


    def ADX(high:pd.Series, low:pd.Series, close:pd.Series, period:int) -> pd.Series:
        plus_dm = high.diff()
        minus_dm = low.diff()
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm > 0] = 0
        
        atr = Indicator.ATR(high, low, close, period)
        
        plus_di = 100 * (plus_dm.ewm(alpha = 1/period).mean() / atr)
        minus_di = abs(100 * (minus_dm.ewm(alpha = 1/period).mean() / atr))
        dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di)) * 100
        adx = ((dx.shift(1) * (period - 1)) + dx) / period
        # adx_smooth = adx.ewm(alpha = 1/period).mean()
        # return adx_smooth
        return adx


    def CCI(high:pd.Series, low:pd.Series, close:pd.Series, period:int) -> pd.Series:
        p = (high + low + close) / 3
        sma = p.rolling(window=period).mean()
        md = p.rolling(window=period).apply(lambda x: pd.Series(x).mad())
        cci = (p - sma) / (0.015 * md)
        
        return cci