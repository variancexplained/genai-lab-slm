#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/shared/instrumentation/decorator.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 24th 2024 09:44:04 pm                                                    #
# Modified   : Tuesday August 27th 2024 10:54:14 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Task instrumentation decorator module."""
import logging
from functools import wraps
from typing import Callable

from appvocai.shared.instrumentation.profiler import TaskProfiler
from appvocai.shared.instrumentation.repo import ProfilingDAL

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------------------------ #
def task_profiler():
    """Decorator to capture and log performance metrics for Task objects."""

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):

            # Determine the task name
            task = args[0].__class__.__name__

            # Instantiate and start the profiler
            try:
                profiler = TaskProfiler(task=task, args=args, kwargs=kwargs)
                profiler.start()
            except Exception as e:
                logger.error(f"Error starting profiler: {e}")
                raise

            # Execute the function and capture the result
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error executing function '{func.__qualname__}': {e}")
                raise

            # Compute the metrics
            try:
                profiler.stop(result=result)
                profile = profiler.profile
            except Exception as e:
                logger.error(f"Error stopping profiler: {e}")
                raise

            # Instantiate the DAL and insert the profile into the database.
            try:
                dal = ProfilingDAL.build()
                dal.create(profile=profile)
            except Exception as e:
                logger.error(f"Error logging profile data: {e}")

            return result

        return wrapper

    return decorator
