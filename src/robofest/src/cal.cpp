#include <iostream>
#include <vector>
#include <complex>
#include <cmath>
#include <iomanip>
#include <iio.h>
#include <omp.h>

typedef std::complex<float> cf32;

// ───────────── Configuration ─────────────
const double FS = 8e6;
const size_t NUM_S = 262144; // 2^18 RX Buffer
const size_t TX_NUM_S = 8000; // Exact multiple for 543kHz @ 8MSPS
const double TONE = 543e3;
const double EPS = 1e-15;
const int CAL_SAMPLES = 20;  // Number of buffers to average
const double TX_ATTENUATION = -10.0; // dB

// ───────────── DSP Logic ─────────────
float lockin_omp(const std::vector<cf32>& buf) {
    if (buf.empty()) return 0.0f;
    float real_acc = 0.0f, imag_acc = 0.0f;
    int n = static_cast<int>(buf.size());

    #pragma omp parallel for reduction(+:real_acc, imag_acc)
    for (int i = 0; i < n; ++i) {
        float phase = -2.0f * M_PI * TONE * (i / FS);
        cf32 osc = std::polar(1.0f, phase);
        cf32 mixed = buf[i] * osc;
        real_acc += mixed.real();
        imag_acc += mixed.imag();
    }
    return std::sqrt(real_acc * real_acc + imag_acc * imag_acc) / n;
}

int main(int argc, char** argv) {
    long long f_mhz = (argc > 1) ? std::stoll(argv[1]) : 2000;
    long long freq_hz = f_mhz * 1000000;

    std::cout << "--- Active TX Calibration Checker ---" << std::endl;
    std::cout << "Target Frequency: " << f_mhz << " MHz" << std::endl;
    std::cout << "Connecting to 192.168.2.1..." << std::endl;

    iio_context *ctx = iio_create_network_context("192.168.2.1");
    if (!ctx) {
        std::cerr << "[Fatal] Could not connect to SDR." << std::endl;
        return 1;
    }

    // 1. Find Devices
    iio_device *phy = iio_context_find_device(ctx, "ad9361-phy");
    iio_device *rx_dev = iio_context_find_device(ctx, "cf-ad9361-lpc");
    iio_device *tx_dev = iio_context_find_device(ctx, "cf-ad9361-dds-core-lpc");

    if (!phy || !rx_dev || !tx_dev) {
        std::cerr << "[Fatal] Hardware devices not found." << std::endl;
        return 1;
    }

    // 2. Setup Channels
    iio_channel *rx_i = iio_device_find_channel(rx_dev, "voltage0", false);
    iio_channel *rx_q = iio_device_find_channel(rx_dev, "voltage1", false);
    iio_channel *rx_lo = iio_device_find_channel(phy, "altvoltage0", true);
    
    iio_channel *tx_i = iio_device_find_channel(tx_dev, "voltage0", true);
    iio_channel *tx_q = iio_device_find_channel(tx_dev, "voltage1", true);
    iio_channel *tx_lo = iio_device_find_channel(phy, "altvoltage1", true);
    iio_channel *tx_gain = iio_device_find_channel(phy, "voltage0", true); // TX Attenuation

    // 3. Configure Hardware
    if (rx_lo) iio_channel_attr_write_longlong(rx_lo, "frequency", freq_hz);
    if (tx_lo) iio_channel_attr_write_longlong(tx_lo, "frequency", freq_hz);
    if (tx_gain) iio_channel_attr_write_double(tx_gain, "hardwaregain", TX_ATTENUATION);

    iio_channel_enable(rx_i);
    iio_channel_enable(rx_q);
    iio_channel_enable(tx_i);
    iio_channel_enable(tx_q);

    // 4. Build and Push the TX Cyclic Buffer
    iio_buffer *txbuf = iio_device_create_buffer(tx_dev, TX_NUM_S, true);
    if (!txbuf) {
        std::cerr << "[Fatal] TX Buffer creation failed." << std::endl;
        return 1;
    }

    char *p_dat, *p_end = (char *)iio_buffer_end(txbuf);
    ptrdiff_t p_inc = iio_buffer_step(txbuf);
    char *p_start = (char *)iio_buffer_first(txbuf, tx_i);

    int idx = 0;
    for (p_dat = p_start; p_dat < p_end; p_dat += p_inc, ++idx) {
        float t = (float)idx / FS;
        int16_t i_val = (int16_t)(1432.0f * std::cos(2.0f * M_PI * TONE * t));
        int16_t q_val = (int16_t)(1432.0f * std::sin(2.0f * M_PI * TONE * t));
        ((int16_t*)p_dat)[0] = i_val;
        ((int16_t*)p_dat)[1] = q_val;
    }
    iio_buffer_push(txbuf);
    std::cout << "TX Active: " << TONE/1000 << " kHz Tone at " << TX_ATTENUATION << " dB\n" << std::endl;

    // 5. Start RX Acquisition
    iio_buffer *rxbuf = iio_device_create_buffer(rx_dev, NUM_S, false);
    if (!rxbuf) {
        std::cerr << "[Fatal] RX Buffer creation failed." << std::endl;
        return 1;
    }

    std::cout << "Averaging " << CAL_SAMPLES << " buffers...\n" << std::endl;

    double cal_sum = 0.0;
    std::vector<cf32> rx_complex;
    rx_complex.reserve(NUM_S);

    for (int i = 1; i <= CAL_SAMPLES; ++i) {
        if (iio_buffer_refill(rxbuf) < 0) {
            std::cerr << "Buffer refill error!" << std::endl;
            break;
        }

        rx_complex.clear();
        char *r_dat, *r_end = (char *)iio_buffer_end(rxbuf);
        ptrdiff_t r_inc = iio_buffer_step(rxbuf);
        char *b0 = (char *)iio_buffer_first(rxbuf, rx_i);

        if (b0) {
            for (r_dat = b0; r_dat < r_end; r_dat += r_inc) {
                float i_val = ((int16_t *)r_dat)[0] / 2048.0f;
                float q_val = ((int16_t *)r_dat)[1] / 2048.0f;
                rx_complex.push_back(cf32(i_val, q_val));
            }
        }

        double raw_dbfs = 20.0 * std::log10(std::max((double)lockin_omp(rx_complex), EPS));
        cal_sum += raw_dbfs;

        std::cout << "\rBuffer " << std::setw(2) << i << "/" << CAL_SAMPLES 
                  << " | Current: " << std::fixed << std::setprecision(2) << raw_dbfs << " dBFS" << std::flush;
    }

    double final_ref = cal_sum / CAL_SAMPLES;
    
    std::cout << "\n\n=== CALIBRATION RESULT ===" << std::endl;
    std::cout << "Copy this value into your main script:" << std::endl;
    std::cout << "const double CAL_REF_DBFS = " << std::fixed << std::setprecision(4) << final_ref << ";" << std::endl;
    std::cout << "==========================" << std::endl;

    iio_buffer_destroy(rxbuf);
    iio_buffer_destroy(txbuf);
    iio_context_destroy(ctx);
    return 0;
}
