import sys
# from PyQt5.QtGui import *
# from PyQt5.QtCore import *
# from PyQt5.QAxContainer import *
from PyQt5.QtWidgets import QApplication
from PyQt5.QtWidgets import QMainWindow
from interface import Interface


class IndiWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.instInterface = Interface(self)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    IndiWindow = IndiWindow()
    IndiWindow.show()
    app.exec_()