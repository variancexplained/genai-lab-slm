#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/application/service/data/dqa/dqa.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 18th 2024 09:47:36 pm                                           #
# Modified   : Wednesday September 18th 2024 09:57:27 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""DQA Service Module"""
from typing import Any

from discover.application.service.base.service import ApplicationService
from discover.application.service.data.dqa.config import DQAServiceConfig
from discover.application.service.data.dqa.pipeline import DQAPipelineBuilder


# ------------------------------------------------------------------------------------------------ #
#                                     DQA SERVICE                                                  #
# ------------------------------------------------------------------------------------------------ #
class DQAService(ApplicationService):
    """
    Service class for running the Data Quality Assessment (DQA) pipeline.

    This class is responsible for orchestrating the execution of the DQA pipeline,
    managing the configuration, and triggering the pipeline's execution through the
    `run` method. It uses the `DQAServiceConfig` to configure tasks and parameters
    for the DQA process.

    Attributes:
    -----------
    config : DQAServiceConfig
        The configuration object containing all the necessary settings for the DQA service
        and pipeline tasks. If no config is passed, it defaults to a new instance of `DQAServiceConfig`.

    Inherits:
    ---------
    ApplicationService:
        Inherits core functionality from the `ApplicationService` class, including managing
        configurations and service operations.

    Methods:
    --------
    run() -> Any:
        Creates and runs the DQA pipeline based on the provided configuration. The method
        instantiates the `DQAPipelineBuilder`, creates the pipeline, and executes the
        pipeline's `run` method, returning the pipeline's results.
    """

    def __init__(self, config: DQAServiceConfig = DQAServiceConfig()) -> None:
        super().__init__(config=config)

    def run(self) -> Any:
        """
        Runs the Data Quality Assessment (DQA) pipeline.

        This method creates a `DQAPipelineBuilder` using the configuration provided by the
        service, constructs the DQA pipeline, and runs it. The result of the pipeline
        execution is returned.

        Returns:
        --------
        Any:
            The output of the DQA pipeline after execution, typically the result of the
            data quality checks.
        """
        builder = DQAPipelineBuilder(config=self._config)
        pipeline = builder.create_pipeline()
        return pipeline.run()
