import pandas as pd



class Indicator():
    def __init__(self) -> None:
        pass


    def ma(df:pd.DataFrame, idx:str, period:int, ascending=True) -> pd.DataFrame:
        if ascending:
            df[idx] = df[idx].rolling(window=period).mean()
        else:
            tmp = df[idx].sort_index(ascending=False)
            df[idx] = tmp.rolling(window=period).mean()

        return df[idx].copy()