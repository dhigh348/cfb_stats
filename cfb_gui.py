from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *

import sys
import os

class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()

        # making the main layout
        self.main_layout = QVBoxLayout()

        # button configurations
        button_names = ['Start', 'Stop']
        button_funcs = [self.start_button, self.stop_button]
        button_combos = list(zip(button_names, button_funcs))
        button_box = self.make_button_box('Buttons', button_combos)
        self.main_layout.addWidget(button_box)

        # setting the main widget
        widget = QWidget()
        widget.setLayout(self.main_layout)
        self.setCentralWidget(widget)
        self.setWindowTitle('Test Title')

    @staticmethod
    @pyqtSlot()
    def start_button():
        """
        Function to print a the value start.
        """
        print('Start')

    @staticmethod
    @pyqtSlot()
    def stop_button():
        """
        Function to print the value stop.
        """
        print('Stop')

    @staticmethod
    def make_button(name: str, func: pyqtSlot):
        """
        Function to make a button and connect it to the function.
        :param name: name of the button
        :param func: function to connect to
        """
        button = QPushButton(name)
        button.clicked.connect(func)
        return button

    def make_button_box(self, box_title: str, names_and_funcs: list):
        """
        Function to make the group box that contain both of the buttons.
        :param box_title: name of the box title
        :param names_and_funcs: names of the buttons and their corresponding button functions
        """
        box = QGroupBox()
        box.setTitle(box_title)
        box_layout = QHBoxLayout(box)

        # adding in the buttons
        for button_name, func in names_and_funcs:
            button = self.make_button(button_name, func)
            box_layout.addWidget(button)

        return box


def main():
    """
    Function to run the gui application. This will start and stop the data collection.
    """
    pass


if __name__ == '__main__':
    app = QApplication(sys.argv)

    window = MainWindow()
    window.show()

    # Start the event loop.
    app.exec_()
