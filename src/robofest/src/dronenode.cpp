#include <rclcpp/rclcpp.hpp>
#include <std_msgs/msg/float64.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <cmath>
#include <ctime>
#include <iomanip>
#include <filesystem>
#include <algorithm>
#include <chrono>

using namespace std::chrono_literals;

struct DroneLog {float x; float y; float timestamp;};

class DroneNode : public rclcpp::Node {
private:
    rclcpp::Subscription<std_msgs::msg::Float64>::SharedPtr sub_;
    rclcpp::TimerBase::SharedPtr flush_timer_;
    
    std::vector<DroneLog> logs_;            // drone position tracker
    std::vector<DroneLog> detected_mines_;  // tracks detected mines
    std::ofstream csv_file_;

    void initCSV() {
        std::string folder_name = "mines_detected";
        std::filesystem::create_directories(folder_name);

        auto t = std::time(nullptr);
        auto tm = *std::localtime(&t);
        char time_buffer[64];
        std::strftime(time_buffer, sizeof(time_buffer), "%d_%H%M%S", &tm);
        
        std::string filename = folder_name + "/mine@" + std::string(time_buffer) + ".csv";
        
        csv_file_.open(filename, std::ios::out | std::ios::app);
        if (csv_file_.is_open()) {
            csv_file_ << "x,y,timestamp\n";
            RCLCPP_INFO(this->get_logger(), "Logging detections to: %s", filename.c_str());
        } else {
            RCLCPP_ERROR(this->get_logger(), "Failed to open CSV file for writing.");
        }
    }

    // Flushes the old logs array
    void flushOldArray() {
        if (!logs_.empty()) {
            logs_.clear();
            RCLCPP_INFO(this->get_logger(), "Flushed old drone logs array.");
        }
    }

    void pipelineCallback(const std_msgs::msg::Float64::SharedPtr msg) {
        double received_time = msg->data;

        if (received_time == -1.0) {
            return;
        }

        if (logs_.empty()) {
            RCLCPP_WARN(this->get_logger(), "Drone log array is empty. Cannot match timestamp %.2f.", received_time);
            return;
        }

        // Binary search for closest timestamp
        auto it = std::lower_bound(logs_.begin(), logs_.end(), received_time,
            [](const DroneLog& log, double val) {
                return log.timestamp < val;
            });

        DroneLog closest_log;

        if (it == logs_.begin()) {
            closest_log = *it;
        } else if (it == logs_.end()) {
            closest_log = *(it - 1);
        } else {
            auto prev = it - 1;
            if (std::abs(received_time - prev->timestamp) < std::abs(received_time - it->timestamp)) {
                closest_log = *prev;
            } else {
                closest_log = *it;
            }
        }
	//output & save csv of detected mine
        detected_mines_.push_back(closest_log);
        RCLCPP_INFO(this->get_logger(), "MINE DETECTED @ (%.2f, %.2f)", closest_log.x, closest_log.y);
        if (csv_file_.is_open()) {
            csv_file_ << closest_log.x << "," 
                      << closest_log.y << "," 
                      << closest_log.timestamp << "\n";
            csv_file_.flush(); 
        }
        flushOldArray();
        flush_timer_->reset();
    }

//Replace lines 101 tp 123 with code to populate the logs_ array

public:
    DroneNode() : Node("drone_node") {
        // Populating with dummy data for testing purposes. 
        logs_ = {
            {10.5, 20.1, 100.1},
            {11.0, 21.5, 101.5},
            {15.2, 19.8, 105.0},
            {20.0, 25.0, 110.2}
        };

        initCSV();

        sub_ = this->create_subscription<std_msgs::msg::Float64>("/timestamp_pipeline", 10, std::bind(&DroneNode::pipelineCallback, this, std::placeholders::_1));

        flush_timer_ = this->create_wall_timer(30s, std::bind(&DroneNode::flushOldArray, this));
    }

    ~DroneNode() {
        if (csv_file_.is_open()) {
            csv_file_.close();
        }
    }
};

int main(int argc, char** argv) {
    rclcpp::init(argc, argv);
    auto node = std::make_shared<DroneNode>();
    rclcpp::spin(node);
    rclcpp::shutdown();
    return 0;
}
