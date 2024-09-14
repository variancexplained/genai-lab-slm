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
# Modified   : Saturday September 14th 2024 03:33:20 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Any

from discover.domain.service.base.service import DomainService
from discover.domain.service.data.ingest.config import IngestConfig
from discover.domain.service.data.ingest.pipeline import IngestPipelineBuilder
from discover.domain.value_objects.context import Context

# ------------------------------------------------------------------------------------------------ #
#                           DATA INGESTION DOMAIN SERVICE                                          #
# ------------------------------------------------------------------------------------------------ #


class DataIngestionDomainService(DomainService):
    def __init__(
        self,
        config: IngestConfig,
        context: Context,
    ) -> None:
        super().__init__(config=config, context=context)

    def run(self) -> Any:
        builder = IngestPipelineBuilder(config=self._config, context=self._context)
        pipeline = builder.create_pipeline()
        return pipeline.run()
