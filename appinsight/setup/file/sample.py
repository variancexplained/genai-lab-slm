#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/setup/file/sample.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday July 4th 2024 05:40:36 pm                                                  #
# Modified   : Friday July 5th 2024 12:05:19 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Setup Module"""
from prefect import Task

from appinsight.shared.persist.file.filesystem import FileSystem
from appinsight.shared.persist.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
class SampleFileTask(Task):
    """
    Task for sampling a fraction of data from a source file and saving it to a destination file.

    Args:
        source (str): The filepath relative to the project root of the source file.
        destination (str): The filepath relative to the environment root of the destination file.
        frac (float): The proportion of the file to sample.
        io_cls (type[IOService]): The class for handling IO operations. Defaults to IOService.
        fs_cls (type[FileSystem]): The class for handling file system operations. Defaults to FileSystem.
        config_cls (type[Config]): The class for handling configuration settings. Defaults to Config.
        force (bool, optional): If True, forces the sampling and saving even if the destination file already exists. Defaults to False.
    """

    def __init__(
        self,
        source: str,
        destination: str,
        frac: float,
        io_cls: type[IOService] = IOService,
        fs_cls: type[FileSystem] = FileSystem,
        force: bool = False,
        **kwargs,
    ) -> None:
        """
        Initializes the SampleFileTask.

        Sets up the source and destination paths, initializes the IO, FileSystem, and Config handlers,
        and configures the task parameters.
        """
        super().__init__()
        self._source = source
        self._destination = destination
        self._io = io_cls()
        self._fs = fs_cls()
        self._frac = frac
        self._force = force
        self._kwargs = kwargs

    def run(self) -> None:
        """Executes the task to sample data from the source file and save it to the destination file.

        Reads data from the source file, samples a fraction of the data as specified in the configuration,
        and saves the sampled data to the destination file. If `force` is True, the sampling and saving
        occur even if the destination file already exists.
        """
        if self._force or not self._fs.exists(filepath=self._destination):
            # Uses IO instead of FileSystem because the source is outside of all environments.
            df = self._io.read(self._source)
            sample = df.sample(frac=self._frac)
            # Use FileSystem to place the file in the appropriate enviroment.
            self._fs.create(data=sample, filepath=self._destination, **self._kwargs)
