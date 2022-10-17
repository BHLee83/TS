from Strategy.strategy import *


class StrategyRT():
    def __init__(self, lstObj_Strategy) -> None:
        self.lstObj_Strategy = lstObj_Strategy
        
    def setPriceInfo(self, PriceInfo):
        self.runStrategy()

    def runStrategy(self):
        for obj in self.lstObj_Strategy:
            obj.execute()