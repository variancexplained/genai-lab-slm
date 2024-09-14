#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/domain/service/data/ingest/pipeline.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 24th 2024 02:47:03 am                                                    #
# Modified   : Saturday September 14th 2024 03:40:47 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Stage Module"""

from copy import copy

from dotenv import load_dotenv
from pandarallel import pandarallel

from discover.domain.service.base.pipeline import Pipeline, PipelineBuilder
from discover.domain.service.core.io import ReadTask, WriteTask
from discover.domain.service.data.ingest.task import IngestTask
from discover.domain.value_objects.config import ServiceConfig
from discover.domain.value_objects.context import Context
from discover.domain.value_objects.lifecycle import Stage

# ------------------------------------------------------------------------------------------------ #
load_dotenv()
pandarallel.initialize(progress_bar=False, nb_workers=12, verbose=0)


# ------------------------------------------------------------------------------------------------ #
class IngestPipeline(Pipeline):
    __STAGE = Stage.INGEST

    @property
    def stage(self) -> Stage:
        return self.__STAGE


# ------------------------------------------------------------------------------------------------ #
#                                INGEST PIPELINE BUILDER                                            #
# ------------------------------------------------------------------------------------------------ #
class IngestPipelineBuilder(PipelineBuilder):
    """ """

    def __init__(
        self,
        config: ServiceConfig,
        context: Context,
    ) -> None:
        """Initializes the Data Ingestion Pipeline."""
        super().__init__(config=config, context=context)

    def create_pipeline(self) -> IngestPipeline:
        """Creates the pipeline with all the tasks for data quality analysis.

        Returns:
            Pipeline: The configured pipeline with tasks.
        """
        # Instantiate pipeline
        pipe = IngestPipeline(config=self._config, context=self._context)

        # Instantiate Tasks
        load = ReadTask(
            data_config=self._config.source_data_config, context=copy(self._context)
        )
        save = WriteTask(
            data_config=self._config.target_data_config,
            context=copy(self._context),
            partition_cols=self._config.partition_cols,
        )
        ingest = IngestTask(config=self._config, context=copy(self._context))

        # Add tasks to pipeline...
        pipe.add_task(load)
        pipe.add_task(ingest)
        pipe.add_task(save)
        return pipe
