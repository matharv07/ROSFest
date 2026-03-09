cmake_minimum_required(VERSION 3.8)
project(robofest)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Enforce C++17 for std::filesystem
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ─── Find ROS 2 Packages ───
find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(std_msgs REQUIRED)

# ─── Find System Libraries ───
# Find OpenMP natively via CMake
find_package(OpenMP REQUIRED)

# Find libiio dynamically
find_library(IIO_LIBRARY NAMES iio REQUIRED)
if(NOT IIO_LIBRARY)
  message(FATAL_ERROR "libiio not found! Please install it (e.g., sudo apt install libiio-dev)")
endif()

# ─── Build the Node ───
add_executable(mine_detector src/port.cpp)

# Target Include Directories
target_include_directories(mine_detector PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>)

# Ament standard dependency linking
ament_target_dependencies(mine_detector
  rclcpp
  std_msgs
)

# Link external hardware/optimization libraries
target_link_libraries(mine_detector
  ${IIO_LIBRARY}
  OpenMP::OpenMP_CXX
)

# ─── Installation ───
install(TARGETS mine_detector
  DESTINATION lib/${PROJECT_NAME})

ament_package()
