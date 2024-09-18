#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/task/data/dqa/service.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 08:04:14 pm                                            #
# Modified   : Wednesday September 18th 2024 02:41:35 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
from typing import Any, Type

from discover.application.service.io.config import ServiceConfig
from discover.domain.base.service import DomainService
from discover.domain.task.data.dqa.pipeline import DQAPipelineBuilder


# ------------------------------------------------------------------------------------------------ #
#                           DATA QUALITY ASSESSMENT DOMAIN SERVICE                                 #
# ------------------------------------------------------------------------------------------------ #
class DQADomainService(DomainService):
    """
    Domain service responsible for running the DQA pipeline.

    The service is initialized with a configuration object and a pipeline builder class.
    The pipeline builder class can be injected for flexibility and testing purposes.

    Parameters:
    ----------
    config : ServiceConfig
        Configuration object for the service.
    pipeline_builder_cls : Type[DQAPipelineBuilder], optional
        The pipeline builder class to use (default is DQAPipelineBuilder).
    """

    def __init__(
        self,
        config: ServiceConfig,
        pipeline_builder_cls: Type[DQAPipelineBuilder] = DQAPipelineBuilder,
    ) -> None:
        super().__init__(config=config)
        self._pipeline_builder_cls = pipeline_builder_cls
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def run(self) -> Any:
        """
        Creates and runs the DQA pipeline.

        Returns:
        -------
        Any
            The result of running the DQA pipeline.

        Raises:
        ------
        RuntimeError
            If the pipeline fails to execute properly.
        """
        try:
            builder = self._pipeline_builder_cls(config=self._config)
            pipeline = builder.create_pipeline()
            return pipeline.run()
        except Exception as e:
            # Log the error or handle it as necessary
            self._logger.error(f"Error occurred while running the DQA pipeline: {e}")
            raise RuntimeError("Failed to execute the DQA pipeline") from e
