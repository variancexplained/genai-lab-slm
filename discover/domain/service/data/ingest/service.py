#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/service/data/ingest/service.py                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 03:22:51 am                                            #
# Modified   : Saturday September 14th 2024 05:38:41 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Any

from discover.domain.base.service import DomainService
from discover.domain.service.data.ingest.config import IngestConfig
from discover.domain.service.data.ingest.pipeline import IngestPipelineBuilder
from discover.domain.value_objects.context import Context

# ------------------------------------------------------------------------------------------------ #
#                           DATA INGESTION DOMAIN SERVICE                                          #
# ------------------------------------------------------------------------------------------------ #


class DataIngestionDomainService(DomainService):
    """
    Domain service for handling data ingestion.

    This service is responsible for managing the data ingestion process within the domain layer.
    It uses the `IngestPipelineBuilder` to construct a data ingestion pipeline and executes it.
    The pipeline includes tasks for reading, processing, and writing data.

    Methods:
    --------
    run() -> Any:
        Executes the data ingestion pipeline by creating it through the pipeline builder
        and running all the associated tasks.

    Attributes:
    -----------
    _config : IngestConfig
        Configuration object for the ingestion process, containing settings for the pipeline and tasks.
    _context : Context
        Context object that tracks metadata such as service type, name, and stage during task execution.
    """

    def __init__(
        self,
        config: IngestConfig,
        context: Context,
    ) -> None:
        """
        Initializes the DataIngestionDomainService with the provided configuration and context.

        Parameters:
        -----------
        config : IngestConfig
            Configuration object containing settings for the data ingestion pipeline.
        context : Context
            Context object used to track execution metadata, including service type and stage.
        """
        super().__init__(config=config, context=context)

    def run(self) -> Any:
        """
        Executes the data ingestion process.

        This method uses the `IngestPipelineBuilder` to create a pipeline for data ingestion,
        which is then executed. The pipeline consists of tasks for reading, processing,
        and writing data.

        Returns:
        --------
        Any:
            The result of the pipeline's execution.
        """
        builder = IngestPipelineBuilder(config=self._config, context=self._context)
        pipeline = builder.create_pipeline()
        return pipeline.run()
