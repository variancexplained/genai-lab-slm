#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/utils/file.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday July 3rd 2024 12:43:31 am                                                 #
# Modified   : Wednesday July 3rd 2024 12:43:45 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os


def check_files_start_with(directory: str, start_string: str) -> bool:
    """Check if at least one file in the directory starts with the specified string.

    Args:
        directory (str): The directory path to check.
        start_string (str): The string to check for at the beginning of filenames.

    Returns:
        bool: True if at least one file starts with start_string, False otherwise.
    """
    # Ensure directory path ends with a separator
    directory = os.path.normpath(directory) + os.sep

    # Check each file in the directory
    for filename in os.listdir(directory):
        if filename.startswith(start_string):
            return True

    return False
