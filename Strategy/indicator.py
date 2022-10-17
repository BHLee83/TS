import pandas as pd


class Indicator():
    def __init__(self) -> None:
        pass

    def ma(self, df, idx, period):
        ma = pd.DataFrame(df)
        return ma[idx].rolling(window=period).mean()