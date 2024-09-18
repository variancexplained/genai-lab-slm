#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/application/service/data/ingest/config.py                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 18th 2024 12:24:56 am                                           #
# Modified   : Wednesday September 18th 2024 03:22:18 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data  Ingestion Application Service Config"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from discover.application.service.io import DataService
from discover.application.service.io.config import (
    ReaderConfig,
    ServiceConfig,
    WriterConfig,
)
from discover.domain.value_objects.lifecycle import DataPrepStage, Phase, Stage


# ------------------------------------------------------------------------------------------------ #
@dataclass
class IngestSourceReaderConfig(ReaderConfig):
    """
    IngestSourceReaderConfig provides configuration for reading the source data during the ingestion process
    in the DATAPREP phase. This configuration is specific to the RAW stage of the data pipeline and is used
    to define how the raw source dataset, such as app reviews, is read before ingestion.

    Attributes:
        phase (Phase): The lifecycle phase, set to Phase.DATAPREP by default, indicating this configuration is
            used during the data preparation phase.

        stage (Stage): The service stage, set to DataPrepStage.RAW by default. This represents the stage where
            the raw source data is being prepared for ingestion.

        io_stage (Stage): The stage from which the data is read, set to DataPrepStage.INGEST by default, indicating
            that the data is being ingested from the raw source.

        name (str): The name of the dataset being read, defaulting to "reviews." This typically refers to the
            raw dataset being ingested, such as app reviews.
    """

    phase: Phase = Phase.DATAPREP
    stage: Stage = DataPrepStage.RAW
    io_stage: Stage = DataPrepStage.INGEST
    name: str = "reviews"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class IngestTargetReaderConfig(ReaderConfig):
    """
    IngestTargetReaderConfig provides configuration for reading target data during the ingestion stage
    of the DATAPREP phase. This configuration is used to check whether the data has already been ingested
    and resides in the target dataset.

    Attributes:
        phase (Phase): The lifecycle phase, set to Phase.DATAPREP by default, indicating this configuration
            is used during the data preparation phase.

        stage (Stage): The service stage, set to DataPrepStage.INGEST by default, representing the stage
            where data ingestion is taking place.

        io_stage (Stage): The stage from which the data is read, set to DataPrepStage.INGEST by default, indicating
            that this configuration is used to check the ingested data in the target stage.

        name (str): The name of the dataset being checked or read, defaulting to "reviews." This typically refers
            to the ingested dataset, such as app reviews.
    """

    phase: Phase = Phase.DATAPREP
    stage: Stage = DataPrepStage.INGEST
    io_stage: Stage = DataPrepStage.INGEST
    name: str = "reviews"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class IngestTargetWriterConfig(WriterConfig):
    """
    IngestTargetWriterConfig provides configuration for writing data to the target during the ingestion stage
    of the DATAPREP phase. This configuration is used to define how ingested datasets, such as app reviews,
    are written to the target destination.

    Attributes:
        phase (Phase): The lifecycle phase, set to Phase.DATAPREP by default, indicating this configuration
            is used during the data preparation phase.

        stage (Stage): The service stage, set to DataPrepStage.INGEST by default, representing the stage
            where data is being ingested and written to the target.

        io_stage (Stage): The stage to which the data is being written, set to DataPrepStage.INGEST by default,
            indicating that the data is being written to the target as part of the ingestion process.

        name (str): The name of the dataset being written, defaulting to "reviews." This typically refers
            to the dataset being written to the target, such as app reviews.
    """

    phase: Phase = Phase.DATAPREP
    stage: Stage = DataPrepStage.INGEST
    io_stage: Stage = DataPrepStage.INGEST
    name: str = "reviews"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataIngestionServiceConfig(ServiceConfig):
    """
    DataIngestionServiceConfig extends ServiceConfig to provide specific configuration for the data ingestion
    service within the DATAPREP phase. This class configures how text data is ingested and processed, including
    settings for text column selection, sample size for encoding detection, and random state for reproducibility.

    Attributes:
        phase (Phase): The lifecycle phase of the service, set to Phase.DATAPREP by default, indicating the configuration
            is used during the data preparation phase.

        stage (Stage): The service stage, set to DataPrepStage.INGEST by default, representing the data ingestion stage.

        text_column (str): The name of the column containing the text data to be processed. Default is "content."

        encoding_sample (float): The proportion of the dataset to sample for encoding detection. Default is 0.01
            (1% of the data).

        random_state (int): A random seed for reproducibility. Default is 22.

        force (bool): Optional flag to force execution, bypassing certain checks (default is False).

    Methods:
        create(cls, source_reader_config, target_reader_config, target_writer_config) -> DataIngestionServiceConfig:
            Factory method for creating a DataIngestionServiceConfig instance. This method initializes the readers
            and writer using the provided configurations. If an error occurs while creating the readers or writer,
            an exception is logged and a RuntimeError is raised.

            Parameters:
                source_reader_config (ReaderConfig): Configuration for the source reader. Defaults to IngestSourceReaderConfig.
                target_reader_config (ReaderConfig): Configuration for the target reader. Defaults to IngestTargetReaderConfig.
                target_writer_config (WriterConfig): Configuration for the target writer. Defaults to IngestTargetWriterConfig.

            Returns:
                DataIngestionServiceConfig: A new instance of the DataIngestionServiceConfig class.

            Raises:
                RuntimeError: If an error occurs while creating the readers or writer.
    """

    phase: Phase = Phase.DATAPREP
    stage: Stage = DataPrepStage.INGEST
    text_column: str = "content"
    encoding_sample: float = 0.01
    random_state: int = 22
    datatypes: dict = field(
        default_factory=lambda: {
            "id": "string",
            "app_id": "string",
            "app_name": "string",
            "category_id": "category",
            "category": "category",
            "author": "string",
            "rating": "int16",
            "content": "string",
            "vote_count": "int64",
            "vote_sum": "int64",
            "date": "datetime64[ms]",
        }
    )
    force: bool = False

    @classmethod
    def create(
        cls,
        source_reader_config: ReaderConfig = IngestSourceReaderConfig(),
        target_reader_config: ReaderConfig = IngestTargetReaderConfig(),
        target_writer_config: WriterConfig = IngestTargetWriterConfig(),
    ) -> DataIngestionServiceConfig:
        ds = DataService()
        try:
            sr = ds.get_reader(config=source_reader_config)
            tr = ds.get_reader(config=target_reader_config)
            tw = ds.get_writer(config=target_writer_config)
        except Exception as e:
            msg = f"Unknown error occured. Unable to create ServiceConfig.\n{e}"
            logging.exception(msg)
            raise RuntimeError(msg)

        return cls(
            source_reader=sr,
            target_reader=tr,
            target_writer=tw,
        )
