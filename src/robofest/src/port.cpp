#include <rclcpp/rclcpp.hpp>
#include <std_msgs/msg/float64.hpp>
#include <iostream>
#include <vector>
#include <complex>
#include <cmath>
#include <iomanip>
#include <fstream>
#include <filesystem>
#include <chrono>
#include <deque>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <omp.h>
#include <iio.h>
#include <string>
#include <ctime>
#include <algorithm>

namespace fs = std::filesystem;
typedef std::complex<float> cf32;

class MineDetectorNode : public rclcpp::Node {
public:
    MineDetectorNode() : Node("mine_detector") {
        // ─── ROS Parameters ───
        this->declare_parameter<std::string>("ip", "192.168.2.1");
        this->declare_parameter<int>("f_mhz", 2000);
        this->declare_parameter<std::string>("obj", "MINE_TEST");
        this->declare_parameter<double>("height", 1.25);
        this->declare_parameter<double>("fs", 8e6);
        this->declare_parameter<int>("num_s", 131072);
        this->declare_parameter<double>("tone", 543e3);
        this->declare_parameter<double>("cal_ref_dbfs", 1.8);
        this->declare_parameter<double>("detect_thresh", 8.0);
        this->declare_parameter<int>("median_win", 100);
        this->declare_parameter<int>("baseline_min", 30);

        ip_            = this->get_parameter("ip").as_string();
        f_mhz_         = this->get_parameter("f_mhz").as_int();
        obj_           = this->get_parameter("obj").as_string();
        height_        = this->get_parameter("height").as_double();
        fs_            = this->get_parameter("fs").as_double();
        num_s_         = this->get_parameter("num_s").as_int();
        tone_          = this->get_parameter("tone").as_double();
        cal_ref_dbfs_  = this->get_parameter("cal_ref_dbfs").as_double();
        detect_thresh_ = this->get_parameter("detect_thresh").as_double();
        median_win_    = this->get_parameter("median_win").as_int();
        baseline_min_  = this->get_parameter("baseline_min").as_int();

        // ─── Precompute Oscillator ───
        precomputed_osc_.resize(num_s_);
        for (size_t i = 0; i < num_s_; ++i) {
            precomputed_osc_[i] = std::polar(1.0f, (float)(-2.0 * M_PI * tone_ * (i / fs_)));
        }
        RCLCPP_INFO(this->get_logger(), "[Init] Precomputed %zu oscillator states.", num_s_);

        // ─── Publisher ───
        // Publishes: timestamp (seconds) if mine detected, -1.0 if not
        pub_ = this->create_publisher<std_msgs::msg::Float64>("timestamp_pipeline", 10);

        // ─── Start Threads ───
        keep_running_ = true;
        acq_thread_  = std::thread(&MineDetectorNode::acquisition_thread, this);
        proc_thread_ = std::thread(&MineDetectorNode::processing_thread, this);

        RCLCPP_INFO(this->get_logger(), "Dual-Threaded VNA Mine Detector Active.");
    }

    ~MineDetectorNode() {
        RCLCPP_INFO(this->get_logger(), "Shutting down gracefully...");
        keep_running_ = false;
        queue_cv_.notify_all();
        if (acq_thread_.joinable())  acq_thread_.join();
        if (proc_thread_.joinable()) proc_thread_.join();
        RCLCPP_INFO(this->get_logger(), "Scan complete. Hardware released.");
    }

private:
    struct SdrData {
        std::vector<cf32> rx_complex;
        double timestamp;
    };

    // Parameters
    std::string ip_;
    int f_mhz_;
    std::string obj_;
    double height_;
    double fs_;
    size_t num_s_;
    double tone_;
    double cal_ref_dbfs_;
    double detect_thresh_;
    int median_win_;
    int baseline_min_;

    // Threading
    std::deque<SdrData> data_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::atomic<bool> keep_running_;
    std::vector<cf32> precomputed_osc_;
    std::thread acq_thread_;
    std::thread proc_thread_;

    rclcpp::Publisher<std_msgs::msg::Float64>::SharedPtr pub_;

    // ───────────── Helpers ─────────────

    // Thread-safe queue empty check — prevents data race in processing loop condition
    bool queue_empty_safe() {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return data_queue_.empty();
    }

    std::vector<double> generate_gaussian_kernel(int window_size, double sigma) {
        std::vector<double> coeffs(window_size);
        double sum = 0.0;
        int half = window_size / 2;
        for (int i = 0; i < window_size; ++i) {
            int x = i - half;
            coeffs[i] = std::exp(-(x * x) / (2.0 * sigma * sigma));
            sum += coeffs[i];
        }
        for (int i = 0; i < window_size; ++i) coeffs[i] /= sum;
        return coeffs;
    }

    float lockin_omp(const std::vector<cf32>& buf) {
        if (buf.empty()) return 0.0f;

        // Guard against buffer exceeding precomputed oscillator size
        int n = static_cast<int>(std::min(buf.size(), precomputed_osc_.size()));

        float real_acc = 0.0f, imag_acc = 0.0f;
        #pragma omp parallel for reduction(+:real_acc, imag_acc)
        for (int i = 0; i < n; ++i) {
            cf32 mixed = buf[i] * precomputed_osc_[i];
            real_acc += mixed.real();
            imag_acc += mixed.imag();
        }
        return std::sqrt(real_acc * real_acc + imag_acc * imag_acc) / n;
    }

    // ───────────── Acquisition Thread ─────────────

    void acquisition_thread() {
        RCLCPP_INFO(this->get_logger(), "[Acq] Connecting to Pluto at %s...", ip_.c_str());

        iio_context *ctx = iio_create_network_context(ip_.c_str());
        if (!ctx) {
            RCLCPP_FATAL(this->get_logger(), "Could not connect to SDR.");
            keep_running_ = false;
            queue_cv_.notify_all();
            return;
        }

        iio_device *phy    = iio_context_find_device(ctx, "ad9361-phy");
        iio_device *rx_dev = iio_context_find_device(ctx, "cf-ad9361-lpc");
        iio_device *tx_dev = iio_context_find_device(ctx, "cf-ad9361-dds-core-lpc");

        if (!phy || !rx_dev || !tx_dev) {
            RCLCPP_FATAL(this->get_logger(), "Hardware devices not found.");
            iio_context_destroy(ctx);
            keep_running_ = false;
            queue_cv_.notify_all();
            return;
        }

        long long freq_hz = static_cast<long long>(f_mhz_) * 1000000LL;

        iio_channel *rx_i  = iio_device_find_channel(rx_dev, "voltage0", false);
        iio_channel *rx_q  = iio_device_find_channel(rx_dev, "voltage1", false);
        iio_channel *rx_lo = iio_device_find_channel(phy,    "altvoltage0", true);
        iio_channel *tx_i  = iio_device_find_channel(tx_dev, "voltage0", true);
        iio_channel *tx_q  = iio_device_find_channel(tx_dev, "voltage1", true);
        iio_channel *tx_lo = iio_device_find_channel(phy,    "altvoltage1", true);
        iio_channel *tx_phy= iio_device_find_channel(phy,    "voltage0", true);
        iio_channel *rx_phy= iio_device_find_channel(phy,    "voltage0", false);

        if (rx_lo)  iio_channel_attr_write_longlong(rx_lo,  "frequency", freq_hz);
        if (tx_lo)  iio_channel_attr_write_longlong(tx_lo,  "frequency", freq_hz);
        if (tx_phy) iio_channel_attr_write_double(tx_phy,   "hardwaregain", -10.0);
        if (rx_phy) {
            iio_channel_attr_write(rx_phy, "gain_control_mode", "manual");
            iio_channel_attr_write_double(rx_phy, "hardwaregain", 30.0);
        }

        iio_channel_enable(rx_i); iio_channel_enable(rx_q);
        iio_channel_enable(tx_i); iio_channel_enable(tx_q);

        // ─── TX Cyclic Buffer ───
        const int TX_BUF_SAMPLES = 8000;
        double cycles = tone_ * TX_BUF_SAMPLES / fs_;
        if (std::abs(cycles - std::round(cycles)) > 0.01) {
            RCLCPP_WARN(this->get_logger(),
                "[Acq] TX buffer does not contain an integer number of tone cycles "
                "(%.3f). Phase discontinuity at wrap boundary may introduce noise.", cycles);
        }

        iio_buffer *txbuf = iio_device_create_buffer(tx_dev, TX_BUF_SAMPLES, true);
        if (!txbuf) {
            RCLCPP_FATAL(this->get_logger(), "TX buffer creation failed.");
            iio_context_destroy(ctx);
            keep_running_ = false;
            queue_cv_.notify_all();
            return;
        }

        ptrdiff_t tx_step = iio_buffer_step(txbuf);
        if (tx_step != 4) {
            RCLCPP_WARN(this->get_logger(),
                "[Acq] Unexpected TX iio_buffer_step=%td (expected 4). Sample layout may be wrong.",
                tx_step);
        }

        char *p_start = (char *)iio_buffer_first(txbuf, tx_i);
        for (int i = 0; i < TX_BUF_SAMPLES; ++i) {
            float t = (float)i / fs_;
            int16_t *sample = (int16_t *)(p_start + i * tx_step);
            sample[0] = (int16_t)(1432.0f * std::cos(2.0f * M_PI * tone_ * t));
            sample[1] = (int16_t)(1432.0f * std::sin(2.0f * M_PI * tone_ * t));
        }
        iio_buffer_push(txbuf);
        RCLCPP_INFO(this->get_logger(), "[Acq] TX cyclic buffer pushed.");

        iio_buffer *rxbuf = iio_device_create_buffer(rx_dev, num_s_, false);
        if (!rxbuf) {
            RCLCPP_FATAL(this->get_logger(), "RX buffer creation failed.");
            iio_buffer_destroy(txbuf);
            iio_context_destroy(ctx);
            keep_running_ = false;
            queue_cv_.notify_all();
            return;
        }

        ptrdiff_t rx_step = iio_buffer_step(rxbuf);
        if (rx_step != 4) {
            RCLCPP_WARN(this->get_logger(),
                "[Acq] Unexpected RX iio_buffer_step=%td (expected 4). I/Q layout may be wrong.",
                rx_step);
        }

        auto start_time = std::chrono::steady_clock::now();
        RCLCPP_INFO(this->get_logger(), "[Acq] Hardware ready. Starting capture.");

        while (keep_running_ && rclcpp::ok()) {
            if (iio_buffer_refill(rxbuf) < 0) {
                RCLCPP_ERROR(this->get_logger(), "[Acq] iio_buffer_refill failed. Stopping.");
                break;
            }

            SdrData data;
            data.timestamp = std::chrono::duration<double>(
                std::chrono::steady_clock::now() - start_time).count();
            data.rx_complex.resize(num_s_);

            char *b0 = (char *)iio_buffer_first(rxbuf, rx_i);
            for (int i = 0; i < (int)num_s_; ++i) {
                int16_t *sample = (int16_t *)(b0 + i * rx_step);
                data.rx_complex[i] = cf32(sample[0] / 2048.0f, sample[1] / 2048.0f);
            }

            {
                std::lock_guard<std::mutex> lock(queue_mutex_);
                if (data_queue_.size() > 10) data_queue_.pop_front();
                data_queue_.push_back(std::move(data));
            }
            queue_cv_.notify_one();
        }

        iio_buffer_destroy(rxbuf);
        iio_buffer_destroy(txbuf);
        iio_context_destroy(ctx);
        RCLCPP_INFO(this->get_logger(), "[Acq] Hardware released.");
    }

    // ───────────── Processing Thread ─────────────

    void processing_thread() {
        std::string folder = "VCA/" + std::to_string(f_mhz_) + "MHz";
        fs::create_directories(folder);

        std::time_t rawtime;
        std::time(&rawtime);
        char t_buf[20];
        std::tm tm_info;
        localtime_r(&rawtime, &tm_info);
        std::strftime(t_buf, sizeof(t_buf), "%d-%m-%H%M%S", &tm_info);

        std::string csv_path = folder + "/scan_ros-" + std::string(t_buf) + ".csv";
        std::ofstream csv(csv_path);
        csv << "Time,Raw_dBFS,S21_dB,S21_Gaussian,S21_SavGol,Baseline,Diff_dB,Detect_Flag,Minima_Flag,OBJ,Height\n";

        const int    GAUSS_WIN   = 15;
        const double GAUSS_SIGMA = 2.0;
        const int    SAVGOL_WIN  = 21;
        const int    SG_HIST_WIN = 5;

        std::vector<double> GAUSS_COEFFS = generate_gaussian_kernel(GAUSS_WIN, GAUSS_SIGMA);

        const double SG_COEFFS[21] = {
            -0.0559006, -0.0248447,  0.0029421,  0.0274600,  0.0487087,
             0.0666885,  0.0813991,  0.0928408,  0.1010134,  0.1059170,
             0.1075515,  0.1059170,  0.1010134,  0.0928408,  0.0813991,
             0.0666885,  0.0487087,  0.0274600,  0.0029421, -0.0248447,
            -0.0559006
        };

        struct RawPoint   { double t, dbfs, s21; };
        struct GaussPoint { double t, dbfs, s21, g_val; };

        std::deque<RawPoint>   r_buf;
        std::deque<GaussPoint> g_buf;
        std::deque<double>     baseline_buf;
        std::deque<double>     sg_history;

        bool   is_detecting = false;
        double baseline     = 0.0;
        const double eps    = 1e-15;

        int flush_counter   = 0;
        const int FLUSH_EVERY = 20;

        RCLCPP_INFO(this->get_logger(), "[Proc] Logging to %s", csv_path.c_str());

        // queue_empty_safe() used here to avoid data race on data_queue_ in loop condition
        while ((keep_running_ && rclcpp::ok()) || !queue_empty_safe()) {
            SdrData data;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                queue_cv_.wait_for(lock, std::chrono::milliseconds(100),
                    [this]{ return !data_queue_.empty() || !keep_running_; });
                if (data_queue_.empty()) continue;
                data = std::move(data_queue_.front());
                data_queue_.pop_front();
            }

            double raw_dbfs = 20.0 * std::log10(std::max((double)lockin_omp(data.rx_complex), eps));
            double s21_db   = raw_dbfs - cal_ref_dbfs_;

            r_buf.push_back({data.timestamp, raw_dbfs, s21_db});
            if (r_buf.size() > (size_t)GAUSS_WIN) r_buf.pop_front();
            if (r_buf.size() < (size_t)GAUSS_WIN) continue;

            double g_val = 0.0;
            for (int i = 0; i < GAUSS_WIN; ++i) g_val += r_buf[i].s21 * GAUSS_COEFFS[i];

            const auto& r_mid = r_buf[GAUSS_WIN / 2];
            g_buf.push_back({r_mid.t, r_mid.dbfs, r_mid.s21, g_val});
            if (g_buf.size() > (size_t)SAVGOL_WIN) g_buf.pop_front();
            if (g_buf.size() < (size_t)SAVGOL_WIN) continue;

            double sg = 0.0;
            for (int i = 0; i < SAVGOL_WIN; ++i) sg += g_buf[i].g_val * SG_COEFFS[i];

            if (!is_detecting) {
                baseline_buf.push_back(sg);
                if (baseline_buf.size() > (size_t)median_win_) baseline_buf.pop_front();
            }

            if (baseline_buf.size() >= (size_t)baseline_min_) {
                std::vector<double> tmp(baseline_buf.begin(), baseline_buf.end());
                std::nth_element(tmp.begin(), tmp.begin() + tmp.size() / 2, tmp.end());
                baseline = tmp[tmp.size() / 2];
            }

            double diff    = std::abs(sg - baseline);
            bool triggered = (diff > detect_thresh_) && (baseline_buf.size() >= (size_t)baseline_min_);

            const auto& pt = g_buf[SAVGOL_WIN / 2];

            // ─── ROS 2 Publishing ───
            // Publishes timestamp if mine detected, -1.0 if not
            std_msgs::msg::Float64 msg;
            msg.data = triggered ? pt.t : -1.0;
            pub_->publish(msg);

            // ─── State Transitions ───
            if (triggered && !is_detecting) {
                RCLCPP_WARN(this->get_logger(),
                    "[!!!] TARGET DETECTED at T=%.3fs | Diff: %.2f dB", pt.t, diff);
                is_detecting = true;
            } else if (!triggered && is_detecting) {
                RCLCPP_INFO(this->get_logger(),
                    "[ - ] Signal returned to baseline at T=%.3fs", pt.t);
                is_detecting = false;
            }

            // ─── Local Minima ───
            sg_history.push_back(sg);
            if (sg_history.size() > (size_t)SG_HIST_WIN) sg_history.pop_front();

            bool is_minima = false;
            if (sg_history.size() == (size_t)SG_HIST_WIN && triggered) {
                double c = sg_history[2];
                if (c < sg_history[0] && c < sg_history[1] &&
                    c < sg_history[3] && c < sg_history[4]) {
                    is_minima = true;
                    RCLCPP_WARN(this->get_logger(),
                        "[>>>] MINIMA - MINE CENTER at T=%.3fs", pt.t);
                }
            }

            csv << pt.t     << ","
                << pt.dbfs  << ","
                << pt.s21   << ","
                << pt.g_val << ","
                << sg       << ","
                << baseline << ","
                << diff     << ","
                << (triggered ? 1 : 0) << ","
                << (is_minima ? 1 : 0) << ","
                << obj_     << ","
                << height_  << "\n";

            if (++flush_counter >= FLUSH_EVERY) {
                csv.flush();
                flush_counter = 0;
            }
        }

        csv.flush();
        csv.close();
        RCLCPP_INFO(this->get_logger(), "[Proc] Log saved to %s", csv_path.c_str());
    }
};

int main(int argc, char** argv) {
    rclcpp::init(argc, argv);
    auto node = std::make_shared<MineDetectorNode>();

    rclcpp::executors::MultiThreadedExecutor executor;
    executor.add_node(node);
    executor.spin();

    rclcpp::shutdown();
    return 0;
}
