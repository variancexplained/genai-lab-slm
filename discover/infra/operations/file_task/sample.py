#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/infra/file_task/sample.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday July 4th 2024 05:40:36 pm                                                  #
# Modified   : Tuesday September 10th 2024 09:23:39 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Setup Module"""
from discover.domain.service.base.repo import Repo
from discover.domain.service.base.task import Task


# ------------------------------------------------------------------------------------------------ #
class SampleFileTask(Task):
    """
    Task for sampling a fraction of data from a source file and saving it to a destination file.

    Args:
        source_stage (str): The data processing stage of the source file.
        destination_stage (str): The data processing stage of the destination file.
        entity (str): The entity stored in the file.
        frac (float): The proportion of the file to sample.
        repo_cls (type[Repo]): The file repository in which the files reside. Defaults to Repo.
        config_cls (type[Config]): The class for handling configuration settings. Defaults to Config.
        force (bool, optional): If True, forces the sampling and saving even if the destination file already exists. Defaults to False.
    """

    def __init__(
        self,
        source_stage: str,
        destination_stage: str,
        entity: str,
        frac: float,
        repo_cls: type[Repo] = Repo,
        force: bool = False,
        **kwargs,
    ) -> None:
        """
        Initializes the SampleFileTask.

        Sets up the source and destination paths, initializes the IO, FileSystem, and Config handlers,
        and configures the task parameters.
        """
        super().__init__()
        self._source_stage = source_stage
        self._destination_stage = destination_stage
        self._entity = entity
        self._repo = repo_cls()
        self._frac = frac
        self._force = force
        self._kwargs = kwargs

    def run(self) -> None:
        """Executes the task to sample data from the source file and save it to the destination file.

        Reads data from the source file, samples a fraction of the data as specified in the configuration,
        and saves the sampled data to the destination file. If `force` is True, the sampling and saving
        occur even if the destination file already exists.
        """
        if self._force or not self._repo.exists(
            stage=self._destination_stage, entity=self._entity
        ):
            # Uses IO instead of FileSystem because the source is outside of all environments.
            df = self._repo.get(stage=self._source_stage, entity=self._entity)
            sample = df.sample(frac=self._frac)
            # Use FileSystem to place the file in the appropriate enviroment.
            self._repo.add(
                stage=self._destination_stage, entity=self._entity, data=sample
            )
