#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/application/service/data/ingest/ingest.py                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 18th 2024 12:25:12 am                                           #
# Modified   : Wednesday September 18th 2024 05:32:19 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #

from typing import Any

from discover.application.service.base.service import ApplicationService
from discover.application.service.data.ingest.config import IngestServiceConfig
from discover.application.service.data.ingest.pipeline import IngestPipelineBuilder


# ------------------------------------------------------------------------------------------------ #
#                         DATA INGESTION APPLICATION SERVICE                                       #
# ------------------------------------------------------------------------------------------------ #
class IngestService(ApplicationService):
    """
    IngestService is an application service responsible for coordinating the execution
    of the data ingestion process. It uses a domain service to perform the ingestion and processing of the data.

    Args:
        config (IngestServiceConfig): Configuration for the data ingestion service. Defaults to
            a new instance of IngestServiceConfig.

    Attributes:
        _domain_service (IngestDomainService): The domain service that performs the actual data ingestion
            logic based on the provided configuration.
    """

    def __init__(self, config: IngestServiceConfig = IngestServiceConfig()) -> None:
        """"""
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
