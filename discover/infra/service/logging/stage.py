#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/service/logging/stage.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 16th 2024 01:13:44 pm                                              #
# Modified   : Thursday October 24th 2024 03:29:02 am                                              #
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
printer = Printer()


# ------------------------------------------------------------------------------------------------ #
def stage_logger(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # Creating a logger specific to the class and function name being decorated.
        # This improves logging granularity, making it easier to trace logs back to the exact function.
        logger = logging.getLogger(f"{func.__qualname__}")

        try:
            printer.print_header(title=self.stage.description)
            # Formatting the current time using the date formatter in HTTP format.
            # This is logged with the message indicating the start of the method.
            start = datetime.now()
            start_fmt = dt4mtr.to_HTTP_format(start)

            # Execute the original function being decorated, passing all args and kwargs.
            result = func(self, *args, **kwargs)

            # Log runtime.
            end = datetime.now()
            end_fmt = dt4mtr.to_HTTP_format(end)
            duration = (end - start).total_seconds()
            duration_fmt = dt4mtr.format_duration(seconds=duration)

            # Print stage
            printer.print_subheader(subtitle=self.stage.description, linestyle="=")
            printer.print_kv(k="Stage Started", v=start_fmt)
            printer.print_kv(k="Stage Completed", v=end_fmt)
            printer.print_kv(k="Stage Runtime", v=duration_fmt)
            # Check duration to see if cached result was used.
            if duration < 2:
                printer.print_kv(k="Cached Result", v="True")
            printer.print_trailer()

            # Log stage
            logger.debug(f"Stage: {self.stage.description}")
            logger.debug(f"Stage Started: {start_fmt}")
            logger.debug(f"Stage Completed: {end_fmt}")
            logger.debug(f"Stage Runtime: {duration_fmt}")
            if duration < 2:
                logger.debug("Cached Result: True")

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
