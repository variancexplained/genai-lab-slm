#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/service/core/monitor/announcer.py                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 16th 2024 01:13:44 pm                                              #
# Modified   : Monday September 16th 2024 03:21:33 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import datetime
import functools
import logging

from discover.core.date_time import ThirdDateFormatter

# ------------------------------------------------------------------------------------------------ #
# Instantiating a global instance of the date formatter which will be reused across all calls.
# This is efficient since ThirdDateFormatter presumably doesn't need to be instantiated more than once.
dt4mtr = ThirdDateFormatter()


# ------------------------------------------------------------------------------------------------ #
def announcer(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # Creating a logger specific to the class and function name being decorated.
        # This improves logging granularity, making it easier to trace logs back to the exact function.
        logger = logging.getLogger(f"{func.__qualname__}")

        try:
            # Determine if the current instance is a Pipeline or Task for logging purposes.
            what = "Task" if "Task" in self.__class__.__name__ else "Pipeline"
            line = f"\n{'='*80}" if what == "Pipeline" else ""

            # Formatting the current time using the date formatter in HTTP format.
            # This is logged with the message indicating the start of the method.
            now = dt4mtr.to_HTTP_format(datetime.now())
            logger.info(f"Starting {what}: {self.__class__.__name__} on {now}{line}")
            if what == "Pipeline":
                logger.info(f"{'='*40}")

            # Execute the original function being decorated, passing all args and kwargs.
            result = func(self, *args, **kwargs)

            # After the function completes, log the completion time.
            now = dt4mtr.to_HTTP_format(datetime.now())
            logger.info(f"Completed {what}: {self.__class__.__name__} on {now}{line}")

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
