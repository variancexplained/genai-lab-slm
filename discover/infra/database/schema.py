#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/storage/database/schema.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 04:38:38 pm                                               #
# Modified   : Saturday September 14th 2024 06:19:38 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
schema = {
    "profile": """
        CREATE TABLE IF NOT EXISTS profile (
            id INTEGER PRIMARY KEY AUTOINCREMENT,          -- Unique identifier for each profiling record
            runid TEXT NOT NULL,                           -- Unique identifier for each run
            service_type TEXT NOT NULL,                    -- Type of service, i.e., Pipeline or Task
            service_name TEXT NOT NULL,                    -- Name of Pipeline or Task
            stage TEXT NOT NULL,                           -- Stage in the process (e.g., INGEST, CLEAN)
            start_time DATETIME NOT NULL,                  -- Task start time
            end_time DATETIME NOT NULL,                    -- Task end time
            runtime_seconds REAL NOT NULL,                 -- Total runtime of the process (in seconds)
            cpu_cores INTEGER NOT NULL,                    -- Total number of CPU cores in the machine
            cpu_user_utilization REAL NOT NULL,            -- Time spent in user space (in seconds)
            cpu_system_utilization REAL NOT NULL,          -- Time spent in system space (in seconds)
            memory_usage_peak_mb REAL NOT NULL,            -- Peak memory usage (in MB)
            memory_allocations INTEGER NOT NULL,           -- Number of memory allocations during the process
            file_read_bytes INTEGER NOT NULL,              -- Total bytes read from files during the process
            file_write_bytes INTEGER NOT NULL,             -- Total bytes written to files during the process
            io_wait_time_seconds REAL NOT NULL,            -- Time spent waiting for I/O operations to complete (in seconds)
            network_data_sent_bytes INTEGER NOT NULL,      -- Total data sent over the network (in bytes)
            network_data_received_bytes INTEGER NOT NULL,  -- Total data received over the network (in bytes)
            exceptions_raised INTEGER DEFAULT 0,           -- Number of exceptions raised during process execution (default 0)
            retry_count INTEGER DEFAULT 0                  -- Number of retries or re-executions of the task
        );
    """
}
