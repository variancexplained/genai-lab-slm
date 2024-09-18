#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/application/ops/announcer.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 16th 2024 01:13:44 pm                                              #
# Modified   : Wednesday September 18th 2024 07:28:27 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import functools
import logging
from datetime import datetime

from discover.application.ops.utils import find_task, get_object_name
from discover.core.date_time import ThirdDateFormatter

# ------------------------------------------------------------------------------------------------ #
# Instantiating a global instance of the date formatter which will be reused across all calls.
# This is efficient since ThirdDateFormatter presumably doesn't need to be instantiated more than once.
dt4mtr = ThirdDateFormatter()


# ------------------------------------------------------------------------------------------------ #
def pipeline_announcer(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # Creating a logger specific to the class and function name being decorated.
        # This improves logging granularity, making it easier to trace logs back to the exact function.
        logger = logging.getLogger(f"{func.__qualname__}")

        try:
            # Determine if the current instance is a Pipeline or Task for logging purposes.
            line = f"\n{'='*80}"

            # Get the task name
            pipeline_name = get_object_name(obj=self)

            # Formatting the current time using the date formatter in HTTP format.
            # This is logged with the message indicating the start of the method.
            now = dt4mtr.to_HTTP_format(datetime.now())
            logger.info(f"Starting {pipeline_name} at {now}{line}")

            # Execute the original function being decorated, passing all args and kwargs.
            result = func(self, *args, **kwargs)

            # After the function completes, log the completion time.
            now = dt4mtr.to_HTTP_format(datetime.now())
            logger.info(f"Completed {pipeline_name} at {now}{line}")

        except Exception as e:
            # If an exception occurs, prepare the function signature for more informative logging.
            # This includes representations of positional arguments and keyword arguments.
            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
            signature = ", ".join(args_repr + kwargs_repr)

            # Log the exception with a clear message containing the function name and its arguments.
            logger.exception(
                f"Exception occurred in {func.__qualname__} called with {signature}\n{str(e)}"
            )

            # Re-raise the caught exception so that the behavior of the function remains unchanged.
            raise

        return result

    return wrapper


# ------------------------------------------------------------------------------------------------ #
def task_announcer(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # Creating a logger specific to the class and function name being decorated.
        # This improves logging granularity, making it easier to trace logs back to the exact function.
        logger = logging.getLogger(f"{func.__qualname__}")

        try:
            # Find the task among the arguments.
            task = find_task(args, kwargs)

            # Get the task name
            task_name = get_object_name(obj=task)

            # Formatting the current time using the date formatter in HTTP format.
            # This is logged with the message indicating the start of the method.
            now = dt4mtr.to_HTTP_format(datetime.now())
            logger.info(f"Starting {task_name} at {now}")

            # Execute the original function being decorated, passing all args and kwargs.
            result = func(self, *args, **kwargs)

            # After the function completes, log the completion time.
            now = dt4mtr.to_HTTP_format(datetime.now())
            logger.info(f"Completed {task_name} at {now}")

        except Exception as e:
            # If an exception occurs, prepare the function signature for more informative logging.
            # This includes representations of positional arguments and keyword arguments.
            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
            signature = ", ".join(args_repr + kwargs_repr)

            # Log the exception with a clear message containing the function name and its arguments.
            logger.exception(
                f"Exception occurred in {task_name} called with {signature}\n{str(e)}"
            )

            # Re-raise the caught exception so that the behavior of the function remains unchanged.
            raise

        return result

    return wrapper
