


class Function():
    def __init__(self) -> None:
        pass


    def CrossUp(lstA, lstB) -> bool:
        if (lstA[1] < lstB[1]) and (lstA[0] > lstB[0]):
            return True

        return False
    

    def CrossDown(lstA, lstB) -> bool:
        if (lstA[1] > lstB[1]) and (lstA[0] < lstB[0]):
            return True
            
        return False