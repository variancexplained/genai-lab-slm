#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/utils/date.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 04:52:24 pm                                             #
# Modified   : Tuesday September 10th 2024 04:53:19 pm                                             #
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
