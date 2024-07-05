#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/utils/datetime.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 23rd 2024 02:38:23 pm                                                  #
# Modified   : Friday June 28th 2024 07:29:31 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from datetime import timedelta


# ------------------------------------------------------------------------------------------------ #
def convert_seconds_to_hms(sec: float) -> str:
    """Converts seconds to hours minutes and seconds.

    Args:
        sec (float): Seconds from timedelta

    Returns: String containing seconds converted to hours, minutes, and seconds.

    """
    # create timedelta and convert it into string
    td_str = str(timedelta(seconds=sec))

    # split string into individual component
    x = td_str.split(":")

    # Determine unit of time to report.
    if x[0] != "0":
        return f"{x[0]} Hours {x[1]} Minutes {x[2]} Seconds"
    elif x[1] != "0":
        return f"{x[1]} Minutes {x[2]} Seconds"
    else:
        return f"{x[2]} Seconds"
