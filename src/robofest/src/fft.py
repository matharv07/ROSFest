import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import savgol_filter
import tkinter as tk
from tkinter import filedialog
import sys

def main():
    root = tk.Tk()
    root.withdraw()
    
    csv_file = filedialog.askopenfilename(
        title="Select CSV for plotting",
        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
    )

    if not csv_file:
        print("No file selected. Exiting.")
        sys.exit()

    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        print(f"Error reading {csv_file}: {e}")
        return

    required_columns = ['Elapsed Time (s)', 'Frequency (Hz)', 'S11 (dB)', 'S21 (dB)', 'OBJ', 'Height (m)']
    if not all(col in df.columns for col in required_columns):
        print(f"Missing one or more required columns. Expected: {required_columns}")
        return

    t = df['Elapsed Time (s)'].values
    freq_mhz = df['Frequency (Hz)'].iloc[0] / 1e6
    s11 = df['S11 (dB)'].values
    s21 = df['S21 (dB)'].values

    # Savitzky-Golay configuration
    window_length = 21  
    poly_order = 3       

    try:
        s11_smoothed = savgol_filter(np.nan_to_num(s11), window_length, poly_order)
        s21_smoothed = savgol_filter(np.nan_to_num(s21), window_length, poly_order)
    except ValueError as e:
         print(f"Savitzky-Golay filter error: {e}")
         return


    meta_change_indices = []
    for i in range(1, len(df)):
        if (df['OBJ'].iloc[i] != df['OBJ'].iloc[i-1]) or (df['Height (m)'].iloc[i] != df['Height (m)'].iloc[i-1]):
            meta_change_indices.append(i)

    # Plotting
    fig, (ax21, ax11) = plt.subplots(2, 1, figsize=(12, 9))

    ax21.plot(t, s21, 'ro', ms=3, alpha=0.3, label='S21 raw')
    ax21.plot(t, s21_smoothed, 'b-', lw=1.5, label='S21 SavGol Smoothed')
    ax21.set_title(f"S21 vs Time ({freq_mhz} MHz) - Savitzky-Golay")
    ax21.set_ylabel("S21 (dB)")
    ax21.grid(True)
    ax21.legend(loc='lower right')


    ax11.plot(t, s11, 'mo', ms=3, alpha=0.3, label='S11 raw')
    ax11.plot(t, s11_smoothed, 'g-', lw=1.5, label='S11 SavGol Smoothed')
    ax11.set_title(f"S11 vs Time ({freq_mhz} MHz) - Savitzky-Golay")
    ax11.set_ylabel("S11 (dB)")
    ax11.set_xlabel("Elapsed Time (s)")
    ax11.grid(True)
    ax11.legend(loc='lower right')

    for idx in meta_change_indices:
        change_t = t[idx]
        obj_val = df['OBJ'].iloc[idx]
        h_val = df['Height (m)'].iloc[idx]
        label_text = f" Meta\n OBJ:{obj_val}\n H:{h_val}m"

        for ax in (ax21, ax11):
            ax.axvline(x=change_t, color='orange', linestyle='--', alpha=0.8)
            ax.text(change_t, 0.95, label_text, color='orange', 
                    transform=ax.get_xaxis_transform(), va='top', ha='right', fontsize=8,
                    bbox=dict(boxstyle="round,pad=0.2", fc='w', ec='orange', alpha=0.7))

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()
