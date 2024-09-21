# **Task Profiler Package**

## **Overview**

The **Task Profiler** package provides a set of tools to capture and persist performance metrics of tasks within your data pipeline. The main components include:
- **Profile**: A data class that captures the performance metrics of a task.
- **ProfileRepo**: A repository class responsible for CRUD operations related to `Profile` objects, storing them in an SQLite database.
- **profiler**: A decorator that wraps the `run` method of task objects, capturing key metrics (CPU utilization, memory usage, etc.) and saving them using `ProfileRepo`.

## **Contents**
1. [Features](#features)
2. [Installation](#installation)
3. [Usage](#usage)
   - [Profiling Tasks](#profiling-tasks)
   - [Using the Profile Repository](#using-the-profile-repository)
4. [Metrics Captured](#metrics-captured)
5. [Contributing](#contributing)
6. [License](#license)

## **Features**
- **Performance Profiling**: Automatically capture task performance metrics (e.g., runtime, CPU, memory, I/O).
- **Persistence**: Store task profiles in a persistent database using an SQLite-backed repository.
- **Task-Level Granularity**: Capture metrics for individual tasks, which can be retrieved and analyzed.
- **Easy Integration**: Use the `profiler` decorator to profile any task by simply wrapping its `run` method.

## **Installation**
Ensure that you have Python installed along with the necessary dependencies.

1. **Install the required libraries**:
   ```bash
   pip install psutil pandas sqlalchemy
   ```

2. **Clone the repository**:
   ```bash
   git clone https://github.com/your-repo/task-profiler
   cd task-profiler
   ```

## **Usage**

### **Profiling Tasks**

1. **Profile Class**:
   `Profile` is a data class that encapsulates the key metrics collected during the execution of a task. Metrics include runtime, CPU utilization, memory usage, and I/O operations.

   Example:
   ```python
   from discover.domain.service.core.monitor.profile import Profile
   from datetime import datetime

   profile = Profile(
       env="production",
       estage ="data_cleaning",
       process_name="AnonymizeReviewsTask",
       task_start_time=datetime.now(),
       task_end_time=datetime.now(),
       runtime_seconds=10.5,
       cpu_cores=4,
       cpu_user_utilization=0.65,
       cpu_system_utilization=0.15,
       memory_usage_peak_mb=512.4,
       memory_allocations=1000,
       file_read_bytes=10240,
       file_write_bytes=20480,
       io_wait_time_seconds=0.5,
       network_data_sent_bytes=5000,
       network_data_received_bytes=12000,
       exceptions_raised=0,
   )
   ```

2. **ProfileRepo Class**:
   The `ProfileRepo` is used to store and manage `Profile` data in an SQLite database. You can add, retrieve, and delete profiles, as well as check if a profile exists.

   Example:
   ```python
   from discover.repo import ProfileRepo
   from discover.container import DiscoverContainer

   # Obtain the repository from the container
   container = DiscoverContainer()
   profile_repo = container.repo.profile()

   # Add a profile
   profile_repo.add(profile)

   # Check if a profile exists
   exists = profile_repo.exists(profile_id=123)

   # Get all profiles as a pandas DataFrame
   all_profiles = profile_repo.get_all()
   ```

3. **profiler Decorator**:
   Use the `profiler` decorator to automatically profile the `run` method of any task object. The decorator captures performance metrics and stores them in the database.

   Example:
   ```python
   from discover.decorators import profiler

   class SomeTask:
       env = "production"
       stage = "data_cleaning"
       process_name = "SomeTask"

       @profiler
       def run(self):
           # Task logic here
           pass

   # Run the task with profiling
   task = SomeTask()
   task.run()
   ```

### **Using the Profile Repository**
The `ProfileRepo` allows you to interact with the SQLite database for CRUD operations on task profiles. The repository supports methods to:
- **Add a profile**: `profile_repo.add(profile)`
- **Retrieve all profiles**: `profile_repo.get_all()`
- **Retrieve by task name**: `profile_repo.get_by_process(process_name="TaskName")`
- **Check existence by ID**: `profile_repo.exists(profile_id=123)`
- **Delete by ID or task name**: `profile_repo.remove(profile_id=123)`, `profile_repo.remove_by_process(process_name="TaskName")`

## **Metrics Captured**
The **Profile** class collects the following metrics:

- **stage**: Stage in the data pipeline (e.g., data cleaning, analysis).
- **process_name**: Name of the task being profiled.
- **task_start_time**: Start time of the task.
- **task_end_time**: End time of the task.
- **runtime_seconds**: Total runtime of the task in seconds.
- **cpu_cores**: Number of CPU cores available.
- **cpu_user_utilization**: CPU time spent in user mode (as a fraction of total runtime and cores).
- **cpu_system_utilization**: CPU time spent in kernel mode (as a fraction of total runtime and cores).
- **memory_usage_peak_mb**: Peak memory usage in MB during task execution.
- **memory_allocations**: Number of memory allocations during the task (currently a placeholder).
- **file_read_bytes**: Total bytes read from the disk.
- **file_write_bytes**: Total bytes written to the disk.
- **io_wait_time_seconds**: Total time spent waiting for I/O operations to complete.
- **network_data_sent_bytes**: Total data sent over the network.
- **network_data_received_bytes**: Total data received over the network.
- **exceptions_raised**: Number of exceptions raised during task execution.


## **Contributing**
Contributions are welcome! Please submit a pull request or raise an issue if you encounter bugs or want to request features. Before contributing, make sure to run all tests to verify the functionality of your changes.

## **License**
This project is licensed under the MIT License. See the `LICENSE` file for more details.

