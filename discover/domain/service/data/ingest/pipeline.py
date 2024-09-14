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
# Modified   : Saturday September 14th 2024 05:33:10 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Stage Module"""

from copy import copy

from dotenv import load_dotenv
from pandarallel import pandarallel

from discover.domain.base.pipeline import Pipeline, PipelineBuilder
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
    """
    Pipeline class for managing the data ingestion stage.

    This class represents the data ingestion pipeline, which is responsible for
    executing tasks related to the ingestion of data. It extends the base `Pipeline`
    class and sets the pipeline's stage to INGEST.

    Attributes:
    -----------
    __STAGE : Stage
        The stage of the pipeline, which is set to INGEST.

    Methods:
    --------
    stage() -> Stage:
        Returns the current stage of the pipeline, which is set to INGEST.
    """

    __STAGE = Stage.INGEST

    @property
    def stage(self) -> Stage:
        """
        Returns the current stage of the pipeline.

        Returns:
        --------
        Stage:
            The stage of the pipeline, which is set to INGEST.
        """
        return self.__STAGE


# ------------------------------------------------------------------------------------------------ #
#                                INGEST PIPELINE BUILDER                                            #
# ------------------------------------------------------------------------------------------------ #
class IngestPipelineBuilder(PipelineBuilder):
    """
    Builder class for constructing the data ingestion pipeline.

    This class is responsible for creating a data ingestion pipeline by
    assembling the required tasks (e.g., reading, ingesting, writing)
    and returning a configured pipeline ready for execution. The pipeline
    handles tasks related to data ingestion and quality analysis.

    Methods:
    --------
    create_pipeline() -> IngestPipeline:
        Constructs the data ingestion pipeline, adds the relevant tasks,
        and returns the configured pipeline.
    """

    def __init__(
        self,
        config: ServiceConfig,
        context: Context,
    ) -> None:
        """
        Initializes the IngestPipelineBuilder with the provided configuration and context.

        Parameters:
        -----------
        config : ServiceConfig
            Configuration object that contains settings and parameters for building the pipeline.
        context : Context
            Context object that tracks metadata related to the pipeline's execution.
        """
        super().__init__(config=config, context=context)

    def create_pipeline(self) -> IngestPipeline:
        """
        Creates and configures the ingestion pipeline with the necessary tasks.

        This method sets up the pipeline by adding tasks such as reading from the source,
        ingesting data, and writing to the target. It returns a fully constructed
        pipeline ready for execution.

        Returns:
        --------
        IngestPipeline:
            The fully configured data ingestion pipeline with all tasks.
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

        # Add tasks to pipeline
        pipe.add_task(load)
        pipe.add_task(ingest)
        pipe.add_task(save)

        return pipe
