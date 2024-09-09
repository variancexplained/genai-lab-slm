#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /appvocai/core/mixins.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 08:50:08 am                                               #
# Modified   : Monday September 9th 2024 09:41:26 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base module for the analysis package """
import logging
import shelve
from abc import ABC

from appvocai.shared.config.env import EnvManager


# TODO: Refactor Cache Management as a Mixin
# ------------------------------------------------------------------------------------------------ #
#                                       ANALYSIS                                                   #
# ------------------------------------------------------------------------------------------------ #
class Analysis(ABC):
    """Abstract base class for analysis classes

    Args:
        repo_cls: (Type[ReviewRepo]): Repository of datasets.
    """

    def __init__(self) -> None:
        super().__init__()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def reset_cache(self):
        # Get the current environment
        env = EnvManager().get_environment()
        # Set the shelve file
        shelf_file = f"cache/{env}/cache"
        # Check if the shelve file exists
        try:
            # Open the shelve file
            with shelve.open(shelf_file) as cache:
                # Get the class name
                class_name = type(self).__name__
                # Iterate over cache keys
                keys_to_remove = [key for key in cache.keys() if class_name in key]
                # Remove keys from the cache
                for key in keys_to_remove:
                    del cache[key]
        except Exception as e:
            msg = f"Exception occurred in reset_cache.\n{e}"
            self.logger.exception(msg)
            raise RuntimeError(msg)
