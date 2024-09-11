#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/database/schema.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 04:38:38 pm                                               #
# Modified   : Monday September 9th 2024 10:21:49 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
schema = {
    "profile": """
        CREATE TABLE IF NOT EXISTS profile (
            id INTEGER PRIMARY KEY AUTOINCREMENT,          -- Unique identifier for each profiling record
            env TEXT NOT NULL,                             -- Environment (e.g., production, staging, development)
            stage TEXT NOT NULL,                           -- Stage in the pipeline (e.g., data cleaning, analysis)
            task_name TEXT NOT NULL,                       -- Name of the task/class being profiled (e.g., AnonymizeReviewsTask)
            task_start_time DATETIME NOT NULL,             -- Task start time
            task_end_time DATETIME NOT NULL,               -- Task end time
            runtime_seconds REAL,                          -- Total runtime of the task (in seconds)
            cpu_cores INTEGER NOT NULL,                    -- Number of CPU cores available
            cpu_user_utilization REAL,                     -- User CPU time as proportion of total available CPU time
            cpu_system_utilization REAL,                   -- System CPU time as proportion of total available CPU time
            memory_usage_peak_mb REAL,                     -- Peak memory usage (in MB)
            memory_allocations INTEGER,                    -- Number of memory allocations during the task
            file_read_bytes INTEGER,                       -- Total bytes read from files during the task
            file_write_bytes INTEGER,                      -- Total bytes written to files during the task
            io_wait_time_seconds REAL,                     -- Time spent waiting for I/O operations to complete (in seconds)
            network_data_sent_bytes INTEGER,               -- Total data sent over the network (in bytes)
            network_data_received_bytes INTEGER,           -- Total data received over the network (in bytes)
            exceptions_raised INTEGER DEFAULT 0,           -- Number of exceptions raised during task execution
            retry_count INTEGER DEFAULT 0                  -- Number of retries or re-executions of the task
        );
"""
}
