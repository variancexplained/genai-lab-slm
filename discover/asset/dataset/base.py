#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/base.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 01:42:59 am                                               #
# Modified   : Friday December 27th 2024 10:26:32 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base Dataset Builder Moddule"""
from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd
from pyspark.sql import DataFrame

from discover.asset.dataset.dataset import Dataset
from discover.core.flow import PhaseEnum, StageEnum


# ------------------------------------------------------------------------------------------------ #
class DatasetBuilder(ABC):
    """
    Abstract base class for building datasets with configurable phases, stages,
    and formats. The builder pattern supports ingesting data from multiple sources,
    configuring internal representations, and exporting to various formats.
    """

    @abstractmethod
    def reset(self) -> None:
        """
        Resets the builder to be ready to construct another Dataset object.
        """
        pass

    @abstractmethod
    def for_phase(self, phase: PhaseEnum) -> DatasetBuilder:
        """
        Configures the dataset for a specific phase (e.g., training, testing).

        Args:
            phase (PhaseEnum): The phase of the dataset lifecycle.

        Returns:
            DatasetBuilder: The builder instance with the phase configuration applied.
        """
        pass

    @abstractmethod
    def for_stage(self, stage: StageEnum) -> DatasetBuilder:
        """
        Configures the dataset for a specific stage (e.g., preprocessing, analysis).

        Args:
            stage (StageEnum): The stage of the dataset lifecycle.

        Returns:
            DatasetBuilder: The builder instance with the stage configuration applied.
        """
        pass

    def with_name(self, name: str) -> DatasetBuilder:
        """
        Assigns a name to the dataset for identification purposes.

        Args:
            name (str): The name of the dataset.

        Returns:
            DatasetBuilder: The builder instance with the name configuration applied.
        """
        pass

    @abstractmethod
    def from_pandas_dataframe(self, data: pd.DataFrame) -> DatasetBuilder:
        """
        Ingests data from an in-memory Pandas DataFrame.

        Args:
            data (pd.DataFrame): The Pandas DataFrame to use as the dataset source.

        Returns:
            DatasetBuilder: The builder instance with the data ingested.
        """
        pass

    @abstractmethod
    def from_spark_dataframe(self, data: DataFrame) -> DatasetBuilder:
        """
        Ingests data from an in-memory Spark DataFrame.

        Args:
            data (DataFrame): The Spark DataFrame to use as the dataset source.

        Returns:
            DatasetBuilder: The builder instance with the data ingested.
        """
        pass

    @abstractmethod
    def from_sparknlp_dataframe(self, data: DataFrame) -> DatasetBuilder:
        """
        Ingests data from a Spark NLP DataFrame.

        Args:
            data (DataFrame): The Spark NLP DataFrame to use as the dataset source.

        Returns:
            DatasetBuilder: The builder instance with the data ingested.
        """
        pass

    @abstractmethod
    def from_parquet_file(self, filepath: str) -> DatasetBuilder:
        """
        Ingests data from a Parquet file.

        Args:
            filepath (str): The path to the Parquet file.

        Returns:
            DatasetBuilder: The builder instance with the data ingested.
        """
        pass

    @abstractmethod
    def from_csv_file(self, filepath: str) -> DatasetBuilder:
        """
        Ingests data from a CSV file.

        Args:
            filepath (str): The path to the CSV file.

        Returns:
            DatasetBuilder: The builder instance with the data ingested.
        """
        pass

    @abstractmethod
    def as_pandas_dataframe(self) -> DatasetBuilder:
        """
        Configures the dataset's internal representation as a Pandas DataFrame.

        Returns:
            DatasetBuilder: The builder instance with the internal form set to Pandas.
        """
        pass

    @abstractmethod
    def as_spark_dataframe(self) -> DatasetBuilder:
        """
        Configures the dataset's internal representation as a Spark DataFrame.

        Returns:
            DatasetBuilder: The builder instance with the internal form set to Spark.
        """
        pass

    @abstractmethod
    def as_sparknlp_dataframe(self) -> DatasetBuilder:
        """
        Configures the dataset's internal representation as a Spark NLP DataFrame.

        Returns:
            DatasetBuilder: The builder instance with the internal form set to Spark NLP.
        """
        pass

    @abstractmethod
    def to_csv_format(self) -> DatasetBuilder:
        """
        Exports the dataset in CSV format.

        Returns:
            DatasetBuilder: The builder instance with the export action completed.
        """
        pass

    @abstractmethod
    def to_parquet_format(self) -> DatasetBuilder:
        """
        Exports the dataset in Parquet format.

        Returns:
            DatasetBuilder: The builder instance with the export action completed.
        """
        pass

    @abstractmethod
    def build(self) -> Dataset:
        """
        Builds and returns the final Dataset object based on the provided configurations.

        Returns:
            Dataset: The fully constructed dataset.
        """
        pass
