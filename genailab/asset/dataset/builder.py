#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/asset/dataset/builder.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 10:20:36 pm                                               #
# Modified   : Sunday February 9th 2025 12:10:08 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Component Builder Module"""
from __future__ import annotations

import logging
from typing import Union

import pandas as pd
from dependency_injector.wiring import Provide, inject
from pyspark.sql import DataFrame

from genailab.asset.base.asset import AssetType
from genailab.asset.base.builder import AssetBuilder
from genailab.asset.dataset.config import DatasetConfig
from genailab.asset.dataset.dataset import Dataset
from genailab.asset.dataset.identity import DatasetPassport
from genailab.asset.dataset.state import DatasetState
from genailab.container import GenAILabContainer
from genailab.core.flow import PhaseDef, StageDef
from genailab.infra.persist.repo.dataset import DatasetRepo
from genailab.infra.persist.repo.file.fao import FAO
from genailab.infra.utils.file.fileset import FileFormat


# ================================================================================================ #
#                                   DATASET BUILDER                                                 #
# ================================================================================================ #
class DatasetBuilder(AssetBuilder):
    """
    A builder class for constructing Dataset objects.

    The DatasetBuilder provides a flexible interface for configuring and constructing
    a Dataset object by chaining method calls for various attributes like configuration,
    creator, source, and the DataFrame.

    Attributes:
        _repo (DatasetRepo): The repository used for generating asset IDs.
        _fao (FAO): The FAO service for interacting with data sources.
        _logger (logging.Logger): The logger for the builder class.
        _asset_type (AssetType): The asset type associated with the dataset.
        _config (DatasetConfig): The dataset configuration.
        _dataframe (Union[pd.DataFrame, DataFrame]): The source DataFrame for the dataset.
        _creator (str): The identifier for the creator of the dataset.
        _source (DatasetPassport): The passport containing source information for the dataset.
        _file_format (FileFormat): The file format for the dataset.

    """

    __ASSET_TYPE = AssetType.DATASET

    @inject
    def __init__(
        self,
        repo: DatasetRepo = Provide[GenAILabContainer.io.repo],
        fao: FAO = Provide[GenAILabContainer.io.fao],
    ) -> None:
        """
        Initializes the DatasetBuilder instance with the provided repository and FAO services.

        Args:
            repo (DatasetRepo): The dataset repository for asset ID generation.
            fao (FAO): The FAO service for interacting with data sources.
        """
        super().__init__()
        self._repo = repo
        self._fao = fao

        self.reset()

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def reset(self) -> None:
        """
        Resets the builder's internal state.

        Clears all attributes, preparing the builder for a new configuration.
        """
        self._asset_type = self.__ASSET_TYPE
        self._config = DatasetConfig
        self._dataframe = None
        self._creator = None
        self._source = None
        self._file_format = FileFormat.PARQUET

    def from_config(self, config: DatasetConfig) -> "DatasetBuilder":
        """
        Configures the dataset based on a DatasetConfig object.

        Args:
            config (DatasetConfig): The configuration for the dataset.

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._config = config
        return self

    def creator(self, creator: str) -> "DatasetBuilder":
        """
        Sets the creator of the dataset.

        Args:
            creator (str): The creator's identifier (e.g., class or system initiating the build).

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._creator = creator
        return self

    def dataframe(self, dataframe: Union[pd.DataFrame, DataFrame]) -> "DatasetBuilder":
        """
        Sets the source DataFrame for the dataset.

        Args:
            dataframe (Union[pd.DataFrame, DataFrame]): Pandas or Spark DataFrame.

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._dataframe = dataframe
        return self

    def source(self, source: str) -> "DatasetBuilder":
        """
        Sets the dataset's source asset_id.

        Args:
            source (str): Source dataset asset_id

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._source = source
        return self

    def build(self) -> Dataset:
        """
        Constructs and returns the final Dataset object.

        This method validates the current attributes and builds a dataset using the configured
        parameters. The dataset includes a passport with metadata and a state object.

        Returns:
            Dataset: The constructed dataset object.

        Raises:
            TypeError: If validation errors occur.
        """
        self._validate()

        passport = self._build_passport()

        state = DatasetState(asset_id=passport.asset_id, creator=self._creator)

        return Dataset(
            dataframe=self._dataframe,
            passport=passport,
            repo=self._repo,
            state=state,
        )

    def _build_passport(self) -> DatasetPassport:
        """
        Constructs the DatasetPassport for the dataset.

        Uses the configuration to generate a passport containing metadata for the dataset
        including asset ID, creator, source, file format, and other relevant information.

        Returns:
            DatasetPassport: The passport containing metadata for the dataset.
        """
        asset_id = self._repo.gen_asset_id(
            phase=self._config.phase, stage=self._config.stage, name=self._config.name
        )
        passport = DatasetPassport(
            assert_type=self._asset_type,
            asset_id=asset_id,
            phase=self._config.phase,
            stage=self._config.stage,
            name=self._config.name,
            description=self._config.description,
            creator=self._creator,
            source=self._source,
            dftype=self._config.dftype,
            file_format=self._config.file_format,
        )
        self._logger.debug(passport)
        return passport

    def _validate(self) -> None:
        """
        Validates the dataset attributes before building.

        Ensures that required attributes are correctly set and raise errors if validation fails.

        Raises:
            TypeError: If validation errors are found.
        """
        errors = []
        if not isinstance(self._config.name, str):
            errors.append("Dataset name must be a string.")
        if not isinstance(self._config.phase, PhaseDef):
            errors.append("Dataset phase must be a PhaseDef instance.")
        if not isinstance(self._config.stage, StageDef):
            errors.append("Dataset stage must be a StageDef instance.")
        if not isinstance(self._config.file_format, FileFormat):
            errors.append("File format must be a FileFormat instance.")
        if not isinstance(
            self._dataframe, (pd.DataFrame, pd.core.frame.DataFrame, DataFrame)
        ):
            errors.append("DataFrame must be pyspark or pandas dataframe.")

        if errors:
            raise TypeError(f"Validation errors: {', '.join(errors)}")
