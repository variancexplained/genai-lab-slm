#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/infra/service/logging/stage.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 16th 2024 01:13:44 pm                                              #
# Modified   : Saturday February 8th 2025 09:21:22 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import functools
import logging
from datetime import datetime

from genailab.infra.service.logging import PRINTING_PARAMETERS as PP
from genailab.infra.utils.date_time.format import ThirdDateFormatter
from genailab.infra.utils.visual.print import Printer, TablePrinter

# ------------------------------------------------------------------------------------------------ #
# Instantiating a global instance of the date formatter which will be reused across all calls.
# This is efficient since ThirdDateFormatter presumably doesn't need to be instantiated more than once.
dt4mtr = ThirdDateFormatter()
printer = Printer()
table_printer = TablePrinter(
    ncols=PP["ncols"], columns=PP["columns"], colwidths=PP["colwidths"]
)
# ------------------------------------------------------------------------------------------------ #


# ------------------------------------------------------------------------------------------------ #
def stage_logger(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # Creating a logger specific to the class and function name being decorated.
        # This improves logging granularity, making it easier to trace logs back to the exact function.
        logger = logging.getLogger(f"{func.__qualname__}")

        try:
            # Format stage title
            dt = dt4mtr.to_HTTP_format(dt=datetime.now())
            stage_title = f"{self.stage.label} {dt}"
            # Print title and dataframe header
            printer.print_title(title=stage_title)
            # Formatting the current time using the date formatter in HTTP format.
            # This is logged with the message indicating the start of the method.
            start = datetime.now()
            start_fmt = start.strftime(format="%H:%M:%S")

            # Execute the original function being decorated, passing all args and kwargs.
            result = func(self, *args, **kwargs)

            # Log runtime.
            end = datetime.now()
            end_fmt = end.strftime(format="%H:%M:%S")
            runtime = (end - start).total_seconds()
            runtime_fmt = dt4mtr.format_duration(seconds=runtime)

            # Print table total line
            table_printer.print_total_line(
                data=(
                    self.stage.label,
                    start_fmt,
                    end_fmt,
                    runtime_fmt,
                    ""
                )
            )
            # Print summary if the stage has a summarize method
            if hasattr(self, "summarize"):
                printer.print_subtitle(subtitle=self.stage.label + " Summary")
                print(self.summarize())

            # Close the stage output
            printer.print_trailer()

            # Log stage
            logger.debug(f"Stage: {self.stage.label}")
            logger.debug(f"Stage Started: {start_fmt}")
            logger.debug(f"Stage Completed: {end_fmt}")
            logger.debug(f"Stage Runtime: {runtime_fmt}")
            if runtime < 2:
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
