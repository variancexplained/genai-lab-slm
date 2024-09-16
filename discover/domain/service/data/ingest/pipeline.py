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
# Modified   : Monday September 16th 2024 12:27:24 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Stage Module"""


from dotenv import load_dotenv
from pandarallel import pandarallel

from discover.domain.base.pipeline import Pipeline, PipelineBuilder
from discover.domain.service.core.data import Reader, Writer
from discover.domain.service.data.ingest.task import IngestTask
from discover.domain.value_objects.config import ServiceConfig
from discover.domain.value_objects.lifecycle import Stage

# ------------------------------------------------------------------------------------------------ #
load_dotenv()
pandarallel.initialize(progress_bar=False, nb_workers=12, verbose=0)


# ------------------------------------------------------------------------------------------------ #
class IngestPipeline(Pipeline):
    """
    A pipeline class responsible for handling the ingestion stage of a data processing workflow.
    This class inherits from the base `Pipeline` and specifically sets the stage to `INGEST`.

    Attributes:
    -----------
    __STAGE : Stage
        A constant that defines the pipeline's current stage as the ingestion stage.

    Methods:
    --------
    __init__(config: ServiceConfig) -> None
        Initializes the `IngestPipeline` with the provided service configuration and sets the
        pipeline stage to ingestion.

    Parameters:
    -----------
    config : ServiceConfig
        The configuration object containing necessary settings for the pipeline's execution.
    """

    __STAGE = Stage.INGEST

    def __init__(
        self,
        config: ServiceConfig,
    ) -> None:
        super().__init__(config=config, stage=self.__STAGE)


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
        self, config: ServiceConfig, pipeline_cls: type[Pipeline] = IngestPipeline
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
        super().__init__(config=config, pipeline_cls=pipeline_cls)

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
        pipe = self._pipeline_cls(config=self._config)

        # Instantiate Tasks
        load = Reader(
            config=self._config.source_data_config,
            pipeline_context=pipe.context,
        )
        save = Writer(
            config=self._config.target_data_config,
            pipeline_context=pipe.context,
            partition_cols=self._config.partition_cols,
        )
        ingest = IngestTask(config=self._config, pipeline_context=pipe.context)

        # Add tasks to pipeline
        pipe.add_task(load)
        pipe.add_task(ingest)
        pipe.add_task(save)

        return pipe
