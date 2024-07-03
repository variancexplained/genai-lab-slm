#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/recovery/base.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday July 2nd 2024 10:23:48 pm                                                   #
# Modified   : Tuesday July 2nd 2024 10:50:27 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod

from appinsight.infrastructure.config.recovery import RecoveryConfig


# ------------------------------------------------------------------------------------------------ #
class Recovery(ABC):
    def __init__(self, config_cls: type[RecoveryConfig] = RecoveryConfig) -> None:
        self._config = config_cls()

    @property
    def config(self) -> str:
        return self._config["recovery"]

    @abstractmethod
    def backup(self, *args, **kwargs) -> None:
        """Executes a backup of an asset."""

    @abstractmethod
    def restore(self, *args, **kwargs) -> None:
        """Executes a restore of an asset."""
