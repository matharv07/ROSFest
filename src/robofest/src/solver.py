#!/usr/bin/env python3
"""
PlutoSDR VNA Data Plotter - Fixed Indexing
=========================
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import tkinter as tk
from tkinter import filedialog
import sys
from scipy.signal import savgol_filter

def smooth_trace(y, k, polyorder=2, std=None):
    if k <= polyorder or y.size < k:
        return y
    window_length = k if k % 2 != 0 else k + 1
    mask = np.isfinite(y)
    if not np.any(mask):
        return y
    y_filled = y.copy()
    x_indices = np.arange(len(y))
    y_filled[~mask] = np.interp(x_indices[~mask], x_indices[mask], y[mask])
    
    if std is None:
        std = max(k / 6.0, 1.0) 
    n = np.arange(k) - (k - 1) / 2.0
    gauss_win = np.exp(-0.5 * (n / std) ** 2)
    gauss_win /= gauss_win.sum() 
    
    gauss_num = np.convolve(y_filled, gauss_win, 'same')
    gauss_den = np.convolve(np.ones_like(y_filled), gauss_win, 'same')
    y_gauss = gauss_num / gauss_den
    return savgol_filter(y_gauss, window_length, polyorder)

def main():
    root = tk.Tk()
    root.withdraw()

    file_path = filedialog.askopenfilename(
        title="Select VNA Data CSV",
        filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")]
    )

    if not file_path:
        sys.exit(0)

    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

    t = df['Elapsed Time (s)'].values
    freq_mhz = df['Frequency (Hz)'].iloc[0] / 1e6
    s21_raw = df['S21 (dB)'].values
    s11_raw = df['S11 (dB)'].values
    
    # Smoothing
    s21_smooth = smooth_trace(s21_raw, 20)
    s11_smooth = smooth_trace(s11_raw, 20)

    # Rolling Std Dev (Center=True uses 5 points before and 5 points after)
    s21_std = pd.Series(s21_raw).rolling(window=11, center=True, min_periods=1).std().values
    s11_std = pd.Series(s11_raw).rolling(window=11, center=True, min_periods=1).std().values

    # Fixed Metadata Detection: Keep dimensions aligned
    # .ne() compares with previous row. The first row is always True (different from NaN).
    obj_changes = df['OBJ'].ne(df['OBJ'].shift())
    h_changes = df['Height (m)'].ne(df['Height (m)'].shift())
    
    # We combine them and remove the very first index (0) so we don't mark the start of the file
    combined_changes = obj_changes | h_changes
    meta_change_indices = df.index[combined_changes].tolist()
    if 0 in meta_change_indices:
        meta_change_indices.remove(0)

    # Plotting
    fig, (ax21, ax11, ax_std) = plt.subplots(3, 1, figsize=(12, 12), sharex=True)

    # S21 Plot
    ax21.plot(t, s21_raw, 'ro', ms=3, alpha=0.2, label='S21 Raw')
    ax21.plot(t, s21_smooth, 'b-', lw=1.5, label='S21 Smooth')
    ax21.set_ylabel("S21 (dB)")
    ax21.grid(True, alpha=0.3)
    ax21.legend(loc='lower right')

    # S11 Plot
    ax11.plot(t, s11_raw, 'mo', ms=3, alpha=0.2, label='S11 Raw')
    ax11.plot(t, s11_smooth, 'b-', lw=1.5, label='S11 Smooth')
    ax11.set_ylabel("S11 (dB)")
    ax11.grid(True, alpha=0.3)
    ax11.legend(loc='lower right')

    # Std Dev Plot
    ax_std.plot(t, s21_std, 'r-', lw=1, label='S21 Std Dev (±5 pts)')
    ax_std.plot(t, s11_std, 'm-', lw=1, label='S11 Std Dev (±5 pts)')
    ax_std.set_ylabel("Std Dev (dB)")
    ax_std.set_xlabel("Time (s)")
    ax_std.grid(True, alpha=0.3)
    ax_std.legend(loc='upper right')

    # Add Markers
    for idx in meta_change_indices:
        change_t = t[idx]
        label_text = f" OBJ:{df['OBJ'].iloc[idx]}\n H:{df['Height (m)'].iloc[idx]}m"
        for i, ax in enumerate((ax21, ax11, ax_std)):
            ax.axvline(x=change_t, color='orange', linestyle='--', alpha=0.7)
            if i == 0:
                ax.text(change_t, 0.98, label_text, color='darkorange', transform=ax.get_xaxis_transform(),
                        va='top', ha='right', fontsize=8, bbox=dict(boxstyle="round", fc='w', ec='orange', alpha=0.8))

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()
