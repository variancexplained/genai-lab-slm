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
# Modified   : Tuesday September 17th 2024 11:29:49 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
schema = {
    "profile": """
        CREATE TABLE IF NOT EXISTS profile (
            id INTEGER PRIMARY KEY AUTOINCREMENT,          -- Unique identifier for each profiling record
            phase TEXT NOT NULL,                           -- Phase, i.e. DataPrep, Analysis, or Modeling
            stage TEXT NOT NULL,                           -- Stage within Phase, i.e. DQA
            task TEXT NOT NULL,                            -- Task class name.
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
            exceptions_raised INTEGER DEFAULT 0           -- Number of exceptions raised during process execution (default 0)

        );
    """
}
