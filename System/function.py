


class Function():
    def __init__(self) -> None:
        pass


    def CrossUp(lstA, lstB, ascending=True) -> bool:
        if len(lstB) > 1:
            if ascending:
                if (lstA[-2] < lstB[-2]) and (lstA[-1] > lstB[-1]):
                    return True
                else:
                    return False
            else:
                if (lstA[1] < lstB[1]) and (lstA[0] > lstB[0]):
                    return True
                else:
                    return False
        else:
            if ascending:
                if (lstA[-2] < lstB) and (lstA[-1] > lstB):
                    return True
                else:
                    return False
            else:
                if (lstA[1] < lstB) and (lstA[0] > lstB):
                    return True
                else:
                    return False
    

    def CrossDown(lstA, lstB, ascending=True) -> bool:
        if len(lstB) > 1:
            if ascending:
                if (lstA[-2] > lstB[-2]) and (lstA[-1] < lstB[-1]):
                    return True
                else:
                    return False
            else:
                if (lstA[1] > lstB[1]) and (lstA[0] < lstB[0]):
                    return True
                else:
                    return False
        else:
            if ascending:
                if (lstA[-2] > lstB) and (lstA[-1] < lstB):
                    return True
                else:
                    return False
            else:
                if (lstA[1] > lstB) and (lstA[0] < lstB):
                    return True
                else:
                    return False
