#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/service/logging/task.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 16th 2024 01:13:44 pm                                              #
# Modified   : Friday November 8th 2024 01:11:40 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import functools
import logging
from datetime import datetime

from discover.infra.utils.date_time.format import ThirdDateFormatter
from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
# Instantiating a global instance of the date formatter which will be reused across all calls.
# This is efficient since ThirdDateFormatter presumably doesn't need to be instantiated more than once.
dt4mtr = ThirdDateFormatter()
# ------------------------------------------------------------------------------------------------ #
# This class handles formatted printing.
printer = Printer()


# ------------------------------------------------------------------------------------------------ #
def task_logger(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # Creating a logger specific to the class and function name being decorated.
        # This improves logging granularity, making it easier to trace logs back to the exact function.
        logger = logging.getLogger(f"{func.__qualname__}")

        try:
            # Get the task name
            task_name = self.name

            # Formatting the current time using the date formatter in HTTP format.
            # This is logged with the message indicating the start of the method.
            start = datetime.now()
            start_fmt = dt4mtr.to_HTTP_format(start)

            # Print the task name and its start time
            printer.print_subheader(subtitle=task_name)
            printer.print_kv(k="Start Datetime", v=start_fmt)

            # Execute the original function being decorated, passing all args and kwargs.
            result = func(self, *args, **kwargs)

            # After the function completes, compute stop time and duration
            end = datetime.now()
            end_fmt = dt4mtr.to_HTTP_format(end)
            duration = (end - start).total_seconds()
            duration_fmt = dt4mtr.format_duration(seconds=duration)

            # Print end time, duration, and any task specific information.
            printer.print_kv(k="Complete Datetime", v=end_fmt)
            printer.print_kv(k="Runtime", v=duration_fmt)

            # Log to file
            logger.debug(f"Task: {task_name}")
            logger.debug(f"Started: {start_fmt}")
            logger.debug(f"Completed: {end_fmt}")
            logger.debug(f"Runtime: {duration_fmt}")

            # Print summary if a summary property exists.
            if hasattr(self, "summary"):
                if self.summary:
                    printer.print_kv(k="Summary", v=self.summary)
                    logger.debug(f"Summary: {self.summary}")

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
