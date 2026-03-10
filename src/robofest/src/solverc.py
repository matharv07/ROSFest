#!/usr/bin/env python3
"""
VNA Mine Detector — CSV Plotter
Reads scan_cpp-*.csv files produced by the C++ logger and highlights
detection zones and mine-centre minima.

Usage:
    python mine_plotter.py                  # opens file dialog
    python mine_plotter.py scan.csv         # loads directly
"""

import sys
import os
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("qtagg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.backends.backend_qtagg import (
    NavigationToolbar2QT, FigureCanvasQTAgg as FigureCanvas)
from matplotlib.collections import BrokenBarHCollection
from scipy.signal import savgol_filter

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel,
    QVBoxLayout, QHBoxLayout, QPushButton, QFrame,
    QFileDialog, QCheckBox, QSizePolicy, QMessageBox,
    QScrollArea, QTableWidget, QTableWidgetItem, QHeaderView,
    QSplitter, QGroupBox)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QColor, QFont

# ─────────── colour palette (mirrors your dark SDR aesthetic) ───────────
C_BG        = "#0a0f1a"
C_BG2       = "#0d1520"
C_PANEL     = "#111c2e"
C_BORDER    = "#1a3055"
C_SG        = "#5ad0ff"      # Savitzky-Golay — cyan
C_GAUSS     = "#48bb78"      # Gaussian        — green
C_RAW       = "#4a7ab5"      # Raw S21         — muted blue
C_BASELINE  = "#f6c90e"      # Baseline        — amber
C_DETECT    = "#ff8c00"      # Detection zone  — orange
C_MINE      = "#ff3c3c"      # Mine centre     — red
C_DIFF      = "#a78bfa"      # Diff trace      — violet
C_TEXT      = "#c8dff5"
C_SUBTEXT   = "#4a7aaa"

plt.rcParams.update({
    "figure.facecolor":  C_BG,
    "axes.facecolor":    C_BG2,
    "axes.edgecolor":    C_BORDER,
    "axes.labelcolor":   C_TEXT,
    "axes.titlecolor":   C_TEXT,
    "xtick.color":       C_SUBTEXT,
    "ytick.color":       C_SUBTEXT,
    "grid.color":        "#152035",
    "grid.linestyle":    "--",
    "grid.linewidth":    0.6,
    "legend.facecolor":  C_PANEL,
    "legend.edgecolor":  C_BORDER,
    "legend.labelcolor": C_TEXT,
    "text.color":        C_TEXT,
    "font.family":       "monospace",
})

DETECT_THRESH = 5.0   # dB — must match C++ value

# ─────────── helpers ───────────
def _smooth_savgol(y, win=21):
    if win % 2 == 0: win += 1
    poly = min(3, win - 1)
    if len(y) < win or poly < 1: return y
    return savgol_filter(np.nan_to_num(y), win, poly)

def _smooth_gaussian(y, k=15, std=None):
    if k <= 1 or len(y) < k: return y
    if std is None: std = max(k / 6.0, 1.0)
    n  = np.arange(k) - (k - 1) / 2.0
    win = np.exp(-0.5 * (n / std) ** 2)
    win /= win.sum()
    mask = np.isfinite(y).astype(float)
    num  = np.convolve(np.nan_to_num(y), win, "same")
    den  = np.convolve(mask, win, "same")
    out  = np.full_like(y, np.nan)
    good = den > 0
    out[good] = num[good] / den[good]
    return out

def _detect_regions(t, flag):
    """Return list of (t_start, t_end) for consecutive detect=1 runs."""
    regions = []
    in_r, t0 = False, 0.0
    for i, f in enumerate(flag):
        if f and not in_r:
            in_r, t0 = True, t[i]
        elif not f and in_r:
            regions.append((t0, t[i - 1]))
            in_r = False
    if in_r:
        regions.append((t0, t[-1]))
    return regions


# ─────────── main window ───────────
class MinePlotter(QMainWindow):
    def __init__(self, csv_path=None):
        super().__init__()
        self.setWindowTitle("VNA Mine Detector — Scan Plotter")
        self.resize(1400, 860)
        self.df = None
        self._build_ui()
        self._style_window()
        if csv_path and os.path.isfile(csv_path):
            self._load(csv_path)

    # ── UI construction ──────────────────────────────────────────────────
    def _build_ui(self):
        cw = QWidget()
        self.setCentralWidget(cw)
        root = QVBoxLayout(cw)
        root.setContentsMargins(10, 8, 10, 8)
        root.setSpacing(6)

        # ── top toolbar ──
        toolbar = QFrame()
        toolbar.setObjectName("toolbar")
        th = QHBoxLayout(toolbar)
        th.setContentsMargins(10, 6, 10, 6)

        title = QLabel("VNA  MINE  DETECTOR")
        title.setFont(QFont("monospace", 13, QFont.Weight.Bold))
        title.setStyleSheet(f"color: {C_SG}; letter-spacing: 4px;")
        th.addWidget(title)
        th.addSpacing(30)

        self.lbl_file = QLabel("No file loaded")
        self.lbl_file.setStyleSheet(f"color: {C_SUBTEXT}; font-size: 10px;")
        th.addWidget(self.lbl_file)
        th.addStretch()

        # layer toggles
        for attr, label, color, checked in [
            ("cb_sg",       "SavGol",   C_SG,       True),
            ("cb_gauss",    "Gaussian", C_GAUSS,    True),
            ("cb_raw",      "Raw S21",  C_RAW,      False),
            ("cb_baseline", "Baseline", C_BASELINE, True),
            ("cb_diff",     "Diff",     C_DIFF,     True),
        ]:
            cb = QCheckBox(label)
            cb.setChecked(checked)
            cb.setStyleSheet(f"color: {color}; font-size: 11px;")
            cb.stateChanged.connect(self._redraw)
            setattr(self, attr, cb)
            th.addWidget(cb)

        th.addSpacing(20)

        btn_open = QPushButton("Open CSV…")
        btn_open.clicked.connect(self._browse)
        btn_open.setStyleSheet(self._btn_style(C_SG))
        th.addWidget(btn_open)

        self.btn_save_img = QPushButton("Save PNG")
        self.btn_save_img.clicked.connect(self._save_image)
        self.btn_save_img.setStyleSheet(self._btn_style(C_GAUSS))
        self.btn_save_img.setEnabled(False)
        th.addWidget(self.btn_save_img)

        root.addWidget(toolbar)

        # ── stats bar ──
        self.stats_bar = QFrame()
        self.stats_bar.setObjectName("statsbar")
        sh = QHBoxLayout(self.stats_bar)
        sh.setContentsMargins(14, 4, 14, 4)
        self.stat_labels = {}
        for key in ["Samples", "Duration", "Freq", "Detections", "Mine Centres", "OBJ", "Height"]:
            lbl_k = QLabel(f"{key}:")
            lbl_k.setStyleSheet(f"color: {C_SUBTEXT}; font-size: 10px;")
            lbl_v = QLabel("—")
            color = C_MINE if "Mine" in key else C_DETECT if "Detect" in key else C_TEXT
            lbl_v.setStyleSheet(f"color: {color}; font-size: 11px; font-weight: bold;")
            sh.addWidget(lbl_k)
            sh.addWidget(lbl_v)
            sh.addSpacing(18)
            self.stat_labels[key] = lbl_v
        sh.addStretch()
        root.addWidget(self.stats_bar)

        # ── splitter: charts | mine table ──
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # charts
        chart_widget = QWidget()
        cv = QVBoxLayout(chart_widget)
        cv.setContentsMargins(0, 0, 0, 0)
        cv.setSpacing(0)

        self.fig = plt.figure(figsize=(13, 7), tight_layout=False)
        self.fig.subplots_adjust(hspace=0.08, left=0.06, right=0.97, top=0.95, bottom=0.08)
        self.ax_main = self.fig.add_subplot(3, 1, (1, 2))
        self.ax_diff = self.fig.add_subplot(3, 1, 3, sharex=self.ax_main)

        self._init_axes()

        self.canvas = FigureCanvas(self.fig)
        self.canvas.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.canvas.mpl_connect("button_press_event", self._on_click)
        cv.addWidget(self.canvas)
        cv.addWidget(NavigationToolbar2QT(self.canvas, chart_widget))
        splitter.addWidget(chart_widget)

        # mine table
        right_panel = QGroupBox("Mine Events")
        right_panel.setObjectName("minepanel")
        right_panel.setMaximumWidth(280)
        rv = QVBoxLayout(right_panel)
        self.mine_table = QTableWidget(0, 4)
        self.mine_table.setHorizontalHeaderLabels(["#", "Time (s)", "Δ (dB)", "OBJ"])
        self.mine_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.mine_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.mine_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.mine_table.verticalHeader().setVisible(False)
        self.mine_table.cellClicked.connect(self._jump_to_mine)
        rv.addWidget(self.mine_table)
        splitter.addWidget(right_panel)
        splitter.setSizes([1100, 260])

        root.addWidget(splitter, stretch=1)

        self._markers = []

    def _init_axes(self):
        for ax in (self.ax_main, self.ax_diff):
            ax.grid(True)
            ax.tick_params(labelsize=9)

        self.ax_main.set_ylabel("S21  (dB)", fontsize=10)
        self.ax_main.set_title("S21 vs Time", fontsize=11, pad=6)
        plt.setp(self.ax_main.get_xticklabels(), visible=False)

        self.ax_diff.set_ylabel("|Diff|  (dB)", fontsize=10)
        self.ax_diff.set_xlabel("Elapsed Time  (s)", fontsize=10)

    # ── styling ──────────────────────────────────────────────────────────
    def _style_window(self):
        self.setStyleSheet(f"""
            QMainWindow, QWidget {{ background: {C_BG}; color: {C_TEXT}; }}
            QFrame#toolbar  {{ background: {C_PANEL}; border: 1px solid {C_BORDER}; border-radius: 6px; }}
            QFrame#statsbar {{ background: {C_BG2};  border: 1px solid {C_BORDER}; border-radius: 4px; }}
            QGroupBox#minepanel {{
                color: {C_SG}; font-size: 11px; font-weight: bold;
                border: 1px solid {C_BORDER}; border-radius: 6px; margin-top: 10px; padding-top: 8px;
            }}
            QGroupBox#minepanel::title {{ subcontrol-origin: margin; left: 10px; }}
            QTableWidget {{
                background: {C_BG2}; color: {C_TEXT};
                gridline-color: {C_BORDER}; font-size: 10px;
                border: none;
            }}
            QHeaderView::section {{
                background: {C_PANEL}; color: {C_SG};
                border: 1px solid {C_BORDER}; font-size: 10px; padding: 3px;
            }}
            QTableWidget::item:selected {{ background: #1a3a60; }}
            QSplitter::handle {{ background: {C_BORDER}; width: 2px; }}
            QCheckBox {{ spacing: 5px; }}
            QCheckBox::indicator {{ width: 13px; height: 13px;
                border: 1px solid {C_BORDER}; border-radius: 3px; background: {C_BG2}; }}
            QCheckBox::indicator:checked {{ background: {C_SG}; }}
        """)

    @staticmethod
    def _btn_style(color):
        return (f"QPushButton {{ background: transparent; color: {color}; "
                f"border: 1px solid {color}; border-radius: 4px; "
                f"padding: 4px 12px; font-family: monospace; font-size: 11px; }}"
                f"QPushButton:hover {{ background: {color}22; }}"
                f"QPushButton:disabled {{ color: #2a4a6a; border-color: #1a3050; }}")

    # ── file I/O ─────────────────────────────────────────────────────────
    def _browse(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Open Scan CSV", "", "CSV files (*.csv);;All files (*)")
        if path:
            self._load(path)

    def _load(self, path):
        try:
            df = pd.read_csv(path)
            df.columns = df.columns.str.strip()
        except Exception as e:
            QMessageBox.critical(self, "Load Error", str(e))
            return

        # normalise column names — accept both C++ and Python logger formats
        rename = {
            "Time":           "Time",
            "Elapsed Time (s)": "Time",
            "S21_SavGol":     "S21_SavGol",
            "S21 (dB)":       "S21_SavGol",
            "S21_Gaussian":   "S21_Gaussian",
            "S21_dB":         "S21_dB",
            "Raw_dBFS":       "Raw_dBFS",
            "Baseline":       "Baseline",
            "Diff_dB":        "Diff_dB",
            "Detect_Flag":    "Detect_Flag",
            "Minima_Flag":    "Minima_Flag",
            "OBJ":            "OBJ",
            "Height":         "Height",
            "Height (m)":     "Height",
            "Frequency (Hz)": "Freq_Hz",
        }
        df.rename(columns={k: v for k, v in rename.items() if k in df.columns}, inplace=True)

        # synthesise missing columns
        if "S21_SavGol" not in df.columns and "S21_dB" in df.columns:
            df["S21_SavGol"] = _smooth_savgol(df["S21_dB"].values, 21)
        if "S21_Gaussian" not in df.columns and "S21_dB" in df.columns:
            df["S21_Gaussian"] = _smooth_gaussian(df["S21_dB"].values, 15)
        if "Baseline" not in df.columns and "S21_SavGol" in df.columns:
            df["Baseline"] = pd.Series(df["S21_SavGol"]).rolling(100, min_periods=10).median().values
        if "Diff_dB" not in df.columns and "S21_SavGol" in df.columns and "Baseline" in df.columns:
            df["Diff_dB"] = (df["S21_SavGol"] - df["Baseline"]).abs()
        if "Detect_Flag" not in df.columns and "Diff_dB" in df.columns:
            df["Detect_Flag"] = (df["Diff_dB"] > DETECT_THRESH).astype(int)
        if "Minima_Flag" not in df.columns:
            df["Minima_Flag"] = 0

        self.df = df
        self.lbl_file.setText(os.path.basename(path))
        self._update_stats()
        self._populate_mine_table()
        self._redraw()
        self.btn_save_img.setEnabled(True)

    # ── stats ─────────────────────────────────────────────────────────────
    def _update_stats(self):
        df = self.df
        t  = df["Time"].values

        freq_str = "—"
        if "Freq_Hz" in df.columns:
            f = df["Freq_Hz"].iloc[0]
            freq_str = f"{f/1e6:.0f} MHz"

        detect_n = int(df["Detect_Flag"].sum()) if "Detect_Flag" in df.columns else 0
        mine_n   = int(df["Minima_Flag"].sum()) if "Minima_Flag" in df.columns else 0
        obj_str  = df["OBJ"].iloc[-1] if "OBJ" in df.columns else "—"
        h_str    = f"{df['Height'].iloc[-1]:.2f} m" if "Height" in df.columns else "—"

        self.stat_labels["Samples"].setText(str(len(df)))
        self.stat_labels["Duration"].setText(f"{t[-1]:.1f} s")
        self.stat_labels["Freq"].setText(freq_str)
        self.stat_labels["Detections"].setText(str(detect_n))
        self.stat_labels["Mine Centres"].setText(str(mine_n))
        self.stat_labels["OBJ"].setText(str(obj_str))
        self.stat_labels["Height"].setText(h_str)

    def _populate_mine_table(self):
        df = self.df
        self.mine_table.setRowCount(0)
        if "Minima_Flag" not in df.columns:
            return
        mines = df[df["Minima_Flag"] == 1]
        for i, (_, row) in enumerate(mines.iterrows()):
            self.mine_table.insertRow(i)
            self.mine_table.setItem(i, 0, QTableWidgetItem(str(i + 1)))
            self.mine_table.setItem(i, 1, QTableWidgetItem(f"{row['Time']:.3f}"))
            diff_str = f"{row['Diff_dB']:.2f}" if "Diff_dB" in df.columns else "—"
            self.mine_table.setItem(i, 2, QTableWidgetItem(diff_str))
            obj_str = str(row["OBJ"]) if "OBJ" in df.columns else "—"
            self.mine_table.setItem(i, 3, QTableWidgetItem(obj_str))
            for c in range(4):
                item = self.mine_table.item(i, c)
                if item:
                    item.setForeground(QColor(C_MINE))
                    item.setBackground(QColor("#1a0808"))

    def _jump_to_mine(self, row, _col):
        df = self.df
        if df is None or "Minima_Flag" not in df.columns:
            return
        mines = df[df["Minima_Flag"] == 1]
        if row >= len(mines):
            return
        t_mine = mines.iloc[row]["Time"]
        cur_xlim = self.ax_main.get_xlim()
        width = cur_xlim[1] - cur_xlim[0]
        self.ax_main.set_xlim(t_mine - width / 2, t_mine + width / 2)
        self.canvas.draw_idle()

    # ── drawing ──────────────────────────────────────────────────────────
    def _redraw(self):
        if self.df is None:
            return

        df = self.df
        t  = df["Time"].values

        for ax in (self.ax_main, self.ax_diff):
            ax.cla()
        self._init_axes()

        # ── detection zone shading ──
        if "Detect_Flag" in df.columns:
            regions = _detect_regions(t, df["Detect_Flag"].values)
            for t0, t1 in regions:
                self.ax_main.axvspan(t0, t1, color=C_DETECT, alpha=0.12, zorder=1)
                self.ax_diff.axvspan(t0, t1, color=C_DETECT, alpha=0.12, zorder=1)

        # ── baseline ──
        if "Baseline" in df.columns and self.cb_baseline.isChecked():
            self.ax_main.plot(t, df["Baseline"].values,
                              color=C_BASELINE, lw=1.4, ls="--", alpha=0.8,
                              label="Baseline", zorder=2)

        # ── raw S21 ──
        raw_col = "S21_dB" if "S21_dB" in df.columns else ("Raw_dBFS" if "Raw_dBFS" in df.columns else None)
        if raw_col and self.cb_raw.isChecked():
            self.ax_main.plot(t, df[raw_col].values,
                              color=C_RAW, lw=0.8, alpha=0.5, label="Raw S21", zorder=2)

        # ── Gaussian ──
        if "S21_Gaussian" in df.columns and self.cb_gauss.isChecked():
            self.ax_main.plot(t, df["S21_Gaussian"].values,
                              color=C_GAUSS, lw=1.5, ls="-", alpha=0.85,
                              label="Gaussian", zorder=3)

        # ── SavGol ──
        if "S21_SavGol" in df.columns and self.cb_sg.isChecked():
            self.ax_main.plot(t, df["S21_SavGol"].values,
                              color=C_SG, lw=2.0, label="SavGol", zorder=4)

        # ── detection region borders (vertical lines) ──
        if "Detect_Flag" in df.columns:
            regions = _detect_regions(t, df["Detect_Flag"].values)
            for t0, t1 in regions:
                self.ax_main.axvline(t0, color=C_DETECT, lw=0.8, alpha=0.5, zorder=3)
                self.ax_main.axvline(t1, color=C_DETECT, lw=0.8, alpha=0.5, zorder=3)

        # ── mine centre markers ──
        if "Minima_Flag" in df.columns:
            sg_col = "S21_SavGol" if "S21_SavGol" in df.columns else raw_col
            mines  = df[df["Minima_Flag"] == 1]
            if sg_col and not mines.empty:
                mt = mines["Time"].values
                my = mines[sg_col].values
                # glow ring
                self.ax_main.scatter(mt, my, s=220, facecolors="none",
                                     edgecolors=C_MINE, linewidths=1.8,
                                     zorder=6, alpha=0.5)
                # solid dot
                self.ax_main.scatter(mt, my, s=60, color=C_MINE,
                                     zorder=7, label="Mine centre")
                # annotation
                for tx, ty, row in zip(mt, my, mines.itertuples()):
                    diff_v = getattr(row, "Diff_dB", None)
                    ann = f"⊗ MINE\n{tx:.2f}s"
                    if diff_v is not None:
                        ann += f"\n−{diff_v:.1f} dB"
                    self.ax_main.annotate(
                        ann, (tx, ty),
                        xytext=(12, -28), textcoords="offset points",
                        fontsize=8, color=C_MINE,
                        arrowprops=dict(arrowstyle="->", color=C_MINE, lw=0.9),
                        bbox=dict(boxstyle="round,pad=0.3", fc="#1a0808",
                                  ec=C_MINE, alpha=0.85),
                        zorder=8,
                    )

        # ── diff trace ──
        if "Diff_dB" in df.columns and self.cb_diff.isChecked():
            dv = df["Diff_dB"].values
            self.ax_diff.plot(t, dv, color=C_DIFF, lw=1.3, zorder=3)
            self.ax_diff.fill_between(t, dv, alpha=0.15, color=C_DIFF, zorder=2)

            # highlight diff where detected
            if "Detect_Flag" in df.columns:
                flag = df["Detect_Flag"].values.astype(bool)
                dv_det = np.where(flag, dv, np.nan)
                self.ax_diff.fill_between(t, dv_det, alpha=0.4,
                                          color=C_DETECT, zorder=2)

            # threshold line
            self.ax_diff.axhline(DETECT_THRESH, color=C_DETECT, lw=1.0,
                                 ls="--", alpha=0.7, zorder=3,
                                 label=f"Threshold ({DETECT_THRESH:.0f} dB)")
            self.ax_diff.legend(fontsize=8, loc="upper right")

        # ── legends & title ──
        self.ax_main.legend(fontsize=9, loc="lower right",
                            framealpha=0.7, ncol=2)
        freq_str = ""
        if "Freq_Hz" in df.columns:
            freq_str = f"  @  {df['Freq_Hz'].iloc[0]/1e6:.0f} MHz"
        self.ax_main.set_title(f"S21 vs Time{freq_str}", fontsize=11, pad=6)

        self.canvas.draw_idle()

    # ── click markers ────────────────────────────────────────────────────
    def _on_click(self, event):
        if event.inaxes not in (self.ax_main, self.ax_diff):
            return
        if self.df is None:
            return

        if event.button == 1:
            t = self.df["Time"].values
            idx = int(np.argmin(np.abs(t - event.xdata)))

            ax  = event.inaxes
            col = "S21_SavGol" if (event.inaxes == self.ax_main and "S21_SavGol" in self.df.columns) \
                  else ("Diff_dB" if "Diff_dB" in self.df.columns else None)
            if col is None:
                return
            x, y = t[idx], self.df[col].values[idx]
            if np.isnan(y):
                return

            mrk, = ax.plot(x, y, "w+", ms=10, mew=2, zorder=10)
            txt  = ax.annotate(
                f"{y:.2f} dB\n{x:.3f} s",
                (x, y), xytext=(6, 6), textcoords="offset points",
                fontsize=8, color=C_TEXT,
                bbox=dict(boxstyle="round,pad=0.25", fc=C_PANEL,
                          ec=C_BORDER, alpha=0.9),
                zorder=10,
            )
            self._markers.extend([mrk, txt])
            self.canvas.draw_idle()

        elif event.button == 3:
            for m in self._markers:
                try: m.remove()
                except: pass
            self._markers.clear()
            self.canvas.draw_idle()

    # ── export ───────────────────────────────────────────────────────────
    def _save_image(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Save Plot", "mine_scan_plot.png",
            "PNG (*.png);;PDF (*.pdf);;SVG (*.svg)")
        if path:
            self.fig.savefig(path, dpi=200, bbox_inches="tight",
                             facecolor=C_BG)
            QMessageBox.information(self, "Saved", f"Plot saved to:\n{path}")


# ─────────── entry point ───────────
if __name__ == "__main__":
    app = QApplication(sys.argv)
    csv_arg = sys.argv[1] if len(sys.argv) > 1 else None
    win = MinePlotter(csv_path=csv_arg)
    win.show()
    sys.exit(app.exec())
