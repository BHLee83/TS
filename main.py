import sys
import os
from PyQt5.QtGui import QCloseEvent
# from PyQt5.QtCore import *
# from PyQt5.QAxContainer import *
from PyQt5 import uic
from PyQt5.QtWidgets import QApplication
from PyQt5.QtWidgets import QMainWindow

from interface import Interface



ui_path = os.path.dirname(os.path.abspath(__file__))
form_class = uic.loadUiType(os.path.join(ui_path, 'form_main.ui'))[0]


class IndiWindow(QMainWindow, form_class):
    def __init__(self):
        super().__init__()
        self.setupUi(self)
        self.instInterface = Interface(self)


    def closeEvent(self, a0: QCloseEvent) -> None:
        self.instInterface.inputPos2DB()
        return super().closeEvent(a0)

    
if __name__ == "__main__":
    app = QApplication(sys.argv)
    IndiWindow = IndiWindow()
    IndiWindow.show()
    app.exec_()