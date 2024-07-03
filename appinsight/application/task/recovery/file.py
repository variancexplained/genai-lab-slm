#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/application/task/recovery/file.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday July 3rd 2024 03:15:18 pm                                                 #
# Modified   : Wednesday July 3rd 2024 03:25:49 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Application Layer File Recover Task"""
import logging

from appinsight.application.base import Task
from appinsight.infrastructure.recovery.file import FileRecovery


# ------------------------------------------------------------------------------------------------ #
class FileBackupTask(Task):
    def __init__(
        self,
        backup_source: str,
        name: str,
        file_recovery_cls: type[FileRecovery] = FileRecovery,
    ) -> None:
        super().__init__()
        self._backup_source = backup_source
        self._name = name
        self._file_recovery = file_recovery_cls()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def execute_task(self) -> None:
        self._file_recovery.backup(backup_source=self._backup_source, name=self._name)
