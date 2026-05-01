import sys
from pathlib import Path
try:
    from PyQt6 import QtWidgets, QtCore
except Exception:
    QtWidgets = None
    QtCore = None
from .enhancer import run_enhanced


class Worker(QtCore.QThread if QtCore is not None else object):
    finished = QtCore.pyqtSignal(str) if QtCore is not None else None
    error = QtCore.pyqtSignal(str) if QtCore is not None else None

    def __init__(self, data_dir, slices):
        super().__init__()
        self.data_dir = Path(data_dir)
        self.slices = slices

    def run(self):
        try:
            out = run_enhanced(self.data_dir, n_slices=self.slices)
            if self.finished:
                self.finished.emit(str(out))
        except Exception as e:
            if self.error:
                self.error.emit(str(e))


class MainWindow(QtWidgets.QWidget if QtWidgets is not None else object):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Bathy Pipeline Enhancer')
        self.resize(700, 420)
        layout = QtWidgets.QVBoxLayout()

        self.data_edit = QtWidgets.QLineEdit()
        btn_browse = QtWidgets.QPushButton('Select data folder')
        btn_browse.clicked.connect(self.browse)

        h = QtWidgets.QHBoxLayout()
        h.addWidget(self.data_edit)
        h.addWidget(btn_browse)
        layout.addLayout(h)

        self.slices_spin = QtWidgets.QSpinBox()
        self.slices_spin.setRange(1, 50)
        self.slices_spin.setValue(6)
        layout.addWidget(QtWidgets.QLabel('Number of slices (multi cross-sections)'))
        layout.addWidget(self.slices_spin)

        self.run_btn = QtWidgets.QPushButton('Run Enhanced Pipeline')
        self.run_btn.clicked.connect(self.run)
        layout.addWidget(self.run_btn)

        self.log = QtWidgets.QTextEdit()
        self.log.setReadOnly(True)
        layout.addWidget(self.log)

        self.setLayout(layout)

    def browse(self):
        d = QtWidgets.QFileDialog.getExistingDirectory(self, 'Select data folder')
        if d:
            self.data_edit.setText(d)

    def run(self):
        data_dir = self.data_edit.text() or None
        slices = self.slices_spin.value()
        self.run_btn.setEnabled(False)
        self.log.append('Starting...')
        self.worker = Worker(data_dir, slices)
        self.worker.finished.connect(self.on_finished)
        self.worker.error.connect(self.on_error)
        self.worker.start()

    def on_finished(self, outdir):
        self.log.append('Done. Outputs: ' + outdir)
        self.run_btn.setEnabled(True)

    def on_error(self, msg):
        self.log.append('Error: ' + msg)
        self.run_btn.setEnabled(True)


def main():
    app = QtWidgets.QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
