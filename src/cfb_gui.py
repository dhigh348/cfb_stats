from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *

from selenium import webdriver
import sys
import os


class MainWindow(QMainWindow):
    choices = ['https://espn.go.com', 'https://dailywire.com']

    def __init__(self):
        super().__init__()

        self.widgets = {}
        self.boxes = {}

        # making the main layout
        self.main_layout = QVBoxLayout()

        # button configurations
        button_names = ['Start', 'Stop']
        button_funcs = [self.start_button, self.stop_button]
        button_combos = list(zip(button_names, button_funcs))

        # widget to hold all buttons
        self.boxes['button_box'] = self._make_button_box('Buttons', button_combos)

        # list box in a listbox
        self.boxes['list_box'] = self._make_list_box('Listbox', self.choices)

        # function to add layouts
        self._add_layouts(self.boxes, self.main_layout)

        # setting the main widget
        widget = QWidget()
        widget.setLayout(self.main_layout)
        self.setCentralWidget(widget)
        self.setWindowTitle('Test Title')

    @pyqtSlot()
    def start_button(self):
        """
        Function to print a the value start.
        """
        for item in self.widgets['list_view'].selectedItems():
            print(item.text())
            self.browser = webdriver.Chrome()
            self.browser.get(item.text())
        print('halo')

    @pyqtSlot()
    def stop_button(self):
        """
        Function to print the value stop.
        """
        self.browser.close()
        print('Stop')

    @staticmethod
    def _make_button(name: str, func: pyqtSlot):
        """
        Function to make a button and connect it to the function.
        :param name: name of the button
        :param func: function to connect to
        """
        button = QPushButton(name)
        button.clicked.connect(func)
        return button

    def _make_button_box(self, box_title: str, names_and_funcs: list):
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
            button = self._make_button(button_name, func)
            box_layout.addWidget(button)

        return box

    def _make_list_box(self, title: str, choices: list):
        """
        Function to make the listbox that will contain the choices to choose from.
        :param title: title of the box container
        :param choices: choices list
        """
        box = QGroupBox()
        box.setTitle(title)
        box_layout = QHBoxLayout(box)
        list_view = QListWidget()
        list_view.setMinimumWidth(250)

        for choice in choices:
            QListWidgetItem(choice, list_view)
        box_layout.addWidget(list_view)
        self.widgets['list_view'] = list_view

        return box

    @staticmethod
    def _add_layouts(widgets, layout):
        """
        Function to add widgets to the layout from the widget container.
        :param widgets: widgets to add
        :param layout: main layout for the application
        """
        for name, widget in widgets.items():
            layout.addWidget(widget)


if __name__ == '__main__':
    app = QApplication(sys.argv)

    window = MainWindow()
    window.show()

    # Start the event loop.
    app.exec_()
