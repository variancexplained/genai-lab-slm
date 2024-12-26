#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/infra/utils/data/format.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 2nd 2024 09:35:10 pm                                                    #
# Modified   : Wednesday December 25th 2024 11:43:46 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import pandas as pd
from pandarallel import pandarallel

# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(progress_bar=False, nb_workers=8, verbose=0)


# ------------------------------------------------------------------------------------------------ #
def format_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """Returns the resulting dataframe with thousands separators."""

    df = df.apply(show_thousands_separator)
    return df


# ------------------------------------------------------------------------------------------------ #
def show_thousands_separator(x):  # pragma: no cover
    """Formats an numbers with thousands separator."""
    try:
        if is_numeric(x):
            return f"{x:,}"
        else:
            return x
    except Exception:
        return x


# ------------------------------------------------------------------------------------------------ #
def is_numeric(x) -> bool:
    try:
        pd.to_numeric(x, errors="raise")
        return True
    except Exception:
        return False


# ------------------------------------------------------------------------------------------------ #
def format_size(size_in_bytes: int) -> str:
    """Formats the size in bytes into a human-readable string with appropriate units.

    Args:
        size_in_bytes (int): The size in bytes.

    Returns:
        str: The formatted size string (e.g., '1.23 MB').
    """
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(size_in_bytes)
    unit_index = 0

    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1

    return f"{size:.2f} {units[unit_index]}"
