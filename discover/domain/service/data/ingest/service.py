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
# Modified   : Saturday September 14th 2024 05:22:47 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Any

from discover.domain.base.service import DomainService
from discover.domain.service.data.ingest.pipeline import IngestPipelineBuilder
from discover.domain.value_objects.config import ServiceConfig


# ------------------------------------------------------------------------------------------------ #
#                           DATA INGESTION DOMAIN SERVICE                                          #
# ------------------------------------------------------------------------------------------------ #
class DataIngestionDomainService(DomainService):
    """
    Domain service responsible for executing the data ingestion process.

    This service orchestrates the creation and execution of the ingestion pipeline,
    using the provided configuration to set up the necessary components.

    Methods:
    --------
    __init__(config: DataIngestionServiceConfig) -> None
        Initializes the domain service with the provided configuration.

    run() -> Any
        Executes the data ingestion process by constructing and running the ingestion pipeline.

        Returns:
        --------
        Any:
            The result of the pipeline execution.
    """

    def __init__(
        self,
        config: ServiceConfig,
    ) -> None:
        """
        Initializes the DataIngestionDomainService with the provided configuration.

        Parameters:
        -----------
        config : DataIngestionServiceConfig
            Configuration object for setting up the data ingestion process.
        """
        super().__init__(config=config)

    def run(self) -> Any:
        """
        Executes the data ingestion process by creating and running the ingestion pipeline.

        The pipeline is built using the `IngestPipelineBuilder` and run to process the data.

        Returns:
        --------
        Any:
            The result of the ingestion pipeline execution.
        """
        builder = IngestPipelineBuilder(config=self._config)
        pipeline = builder.create_pipeline()
        return pipeline.run()
