#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/builder.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 10:20:36 pm                                               #
# Modified   : Friday January 3rd 2025 06:14:01 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Component Builder Module"""
from __future__ import annotations

import inspect
import logging
from datetime import datetime
from typing import Optional, Union

import pandas as pd
from dependency_injector.wiring import Provide, inject
from pyspark.sql import DataFrame, SparkSession

from discover.asset.base.atype import AssetType
from discover.asset.base.builder import AssetBuilder
from discover.asset.dataset.dataset import Dataset
from discover.asset.dataset.identity import DatasetPassport
from discover.container import DiscoverContainer
from discover.core.dtypes import DFType
from discover.core.file import FileFormat
from discover.core.flow import PhaseDef, StageDef
from discover.infra.persist.file.fao import FAO
from discover.infra.workspace.service import Workspace


# ================================================================================================ #
#                                   DATASET BUILDER                                                 #
# ================================================================================================ #
class DatasetBuilder(AssetBuilder):

    @inject
    def __init__(
        self,
        workspace: Workspace = Provide[DiscoverContainer.workspace.service],
        fao: FAO = Provide[DiscoverContainer.io.fao],
    ) -> None:
        super().__init__()
        self._workspace = workspace
        self._fao = fao

        self._passport = None
        self._dataframe = None
        self._dataset = None
        self._filepath = None
        self._dftype = None
        self._file_format = None
        self._source_filepath = None
        self._source_file_format = None
        self._spark = None

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def reset(self) -> None:
        """
        Resets the builder's internal state.

        Clears all attributes, preparing the builder for a new configuration.
        """
        self._passport = None
        self._dataframe = None
        self._dataset = None
        self._filepath = None
        self._dftype = None
        self._file_format = None
        self._source_filepath = None
        self._source_file_format = None
        self._spark = None

    # -------------------------------------------------------------------------------------------- #
    @property
    def dataset(self) -> Dataset:
        """
        Retrieves the constructed `Dataset` object and resets the builder's state.

        Returns:
            Dataset: The constructed dataset object.
        """
        dataset = self._dataset
        self.reset()
        return dataset

    # -------------------------------------------------------------------------------------------- #
    def passport(self, passport: DatasetPassport) -> DatasetBuilder:
        """
        Sets the dataset passport

        Args:
            passport (DatasetPassport): The passport for the data source.

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._passport = passport
        return self

    # -------------------------------------------------------------------------------------------- #
    def from_source_filepath(
        self, filepath: str, file_format: FileFormat
    ) -> DatasetBuilder:
        """
        Sets the source filepath, format, and DataFrame type.

        Args:
            filepath (str): Path to source file.
            file_format (FileFormat): The format of the source file

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._source_filepath = filepath
        self._source_file_format = file_format
        return self

    # -------------------------------------------------------------------------------------------- #
    def from_dataframe(
        self, dataframe: Union[pd.DataFrame, DataFrame]
    ) -> DatasetBuilder:
        """
        Sets dataframe on the dataset

        Args:
            dataframe (Union[pd.DataFrame, DataFrame]): Dataframe containing the data

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._dataframe = dataframe
        if isinstance(dataframe, DataFrame):
            self._dftype = DFType.SPARK
        else:
            self._dftype = DFType.PANDAS
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                 DATAFRAME TYPE                                               #
    # -------------------------------------------------------------------------------------------- #
    def as_pandas(self) -> DatasetBuilder:
        """
        Sets the dataframe type to pandas

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.PANDAS
        return self

    # -------------------------------------------------------------------------------------------- #
    def as_spark(self) -> DatasetBuilder:
        """
        Sets the dataframe type to spark

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.SPARK
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                  FILE FORMAT                                                 #
    # -------------------------------------------------------------------------------------------- #
    def to_csv(self) -> DatasetBuilder:
        """
        Sets the file format to csv

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._file_format = FileFormat.CSV
        return self

    # -------------------------------------------------------------------------------------------- #
    def to_parquet(self) -> DatasetBuilder:
        """
        Sets the file format to parquet

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._file_format = FileFormat.PARQUET
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                       SPARK                                                  #
    # -------------------------------------------------------------------------------------------- #
    def spark(self, spark: SparkSession) -> DatasetBuilder:
        """
        Sets the spark session required to read spark dataframes.

        Args:
            spark (SparkSession): The spark session

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._spark = spark
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                      BUILD                                                   #
    # -------------------------------------------------------------------------------------------- #
    def build(self) -> DatasetBuilder:

        self._validate()

        if self._source_filepath:
            self._dataframe = self._read_data()

        self._filepath = self._get_filepath()

        self._dataset = Dataset(
            workspace=self._workspace,
            passport=self._passport,
            dftype=self._dftype,
            filepath=self._filepath,
            file_format=self._file_format,
            dataframe=self._dataframe,
        )

        self._workspace.dataset_repo.add(asset=self._dataset)
        return self

    # -------------------------------------------------------------------------------------------- #
    def _get_dftype(self) -> str:
        """Returns the DataFrame type `dftype` of the DataFrame"""
        if isinstance(self._dataframe, (pd.DataFrame, pd.core.frame.DataFrame)):
            return DFType.PANDAS
        elif isinstance(self._dataframe, DataFrame):
            return DFType.SPARK
        else:
            msg = f"Invalid DataFrame type. Expected a pandas or spark DataFrame object. Received a {type(self._dataframe)} type."
            self._logger.error(msg)
            raise TypeError(msg)

    # -------------------------------------------------------------------------------------------- #
    def _get_filepath(self) -> str:
        """
        Generates the file path for the data component using the workspace and passport.

        Returns:
            str: The file path for the data component.
        """
        return self._workspace.get_filepath(
            asset_type=self._passport.asset_type,
            asset_id=self._passport.asset_id,
            phase=self._passport.phase,
            file_format=self._file_format,
        )

    # -------------------------------------------------------------------------------------------- #
    def _read_data(self) -> Union[pd.DataFrame, DataFrame]:

        if self._dftype == DFType.SPARK:
            try:
                return self._fao.read(
                    filepath=self._source_filepath,
                    dftype=self._dftype,
                    spark=self._spark,
                )
            except Exception as e:
                msg = f"Exception encountered while reading from {self._source_filepath}.\n{e}"
                self._logger.error(msg)
                raise Exception(msg) from e
        else:
            return self._fao.read(filepath=self._source_filepath, dftype=self._dftype)

    # -------------------------------------------------------------------------------------------- #
    def _validate(self) -> None:
        errors = []

        # Validate file format
        if not isinstance(self._file_format, FileFormat):
            errors.append(
                f"File format must be a FileFormat type. Received a {type(self._file_format)} type."
            )

        # Validate dataframe format
        if not isinstance(self._dftype, DFType):
            errors.append("DataFrame type `dftype` is required for the DatasetBuildr.")

        # Validate the target passport before performing checks that depend upon valid passport.
        if not isinstance(self._passport, DatasetPassport):
            errors.append(
                "A Dataset requires a target DatasetPassport object for the Dataset."
            )

        # Ensure a source filepath or source passport is provided
        if not self._source_filepath and self._dataframe is None:
            errors.append(
                "Either a source filepath or a source dataframe must be provided to the DatasetBuilder."
            )

        # Ensure spark session is provided for spark dataframes
        if self._dftype == DFType.SPARK and self._spark is None:
            errors.append("For spark dataframes, a spark context must be provided.")

        # Ensure dataframe is a valid type
        if self._dataframe is not None:
            if not isinstance(
                self._dataframe, (DataFrame, pd.core.frame.DataFrame, pd.DataFrame)
            ):
                errors.append(
                    f"DataFrame type error. Expected a spark or pandas dataframe. Received a {type(self._dataframe)} object."
                )

        if errors:
            self._report_validation_errors(errors=errors)


# ================================================================================================ #
#                                DATASET PASSPORT BUILDER                                          #
# ================================================================================================ #
class DatasetPassportBuilder(AssetBuilder):
    """
    Builder class for constructing DatasetPassport objects.

    Args:
        workspace (Workspace): The workspace instance to use for ID generation.
    """

    __ASSET_TYPE: AssetType = AssetType.DATASET

    @inject
    def __init__(
        self,
        workspace: Workspace = Provide[DiscoverContainer.workspace.service],
    ) -> None:
        self._workspace = workspace
        self._asset_type = self.__ASSET_TYPE
        self._asset_id: Optional[str] = None
        self._phase: Optional[PhaseDef] = None
        self._stage: Optional[StageDef] = None
        self._name: Optional[str] = None
        self._version: Optional[str] = None
        self._creator: Optional[str] = None
        self._created: Optional[datetime] = None
        self._source: Optional[Dataset] = None
        self._parent: Optional[DatasetPassport] = None
        self._file_format = FileFormat.PARQUET
        self._dftype: Optional[DatasetPassport] = None
        self._dataset_passport: Optional[DatasetPassport] = None

    @property
    def passport(self) -> DatasetPassport:
        """
        Returns the constructed DatasetPassport and resets the builder.

        Returns:
            DatasetPassport: The constructed passport.

        Raises:
            ValueError: If `build()` was not called before accessing this property.
        """
        if not self._dataset_passport:
            raise ValueError("Passport has not been built. Call `build()` first.")
        passport = self._dataset_passport
        self.reset()
        return passport

    def reset(self) -> None:
        """Resets the builder to its initial state."""
        self._asset_id = None
        self._phase = None
        self._stage = None
        self._name = None
        self._creator = None
        self._created = None
        self._version = None
        self._source = None
        self._parent = None
        self._file_format = FileFormat.PARQUET
        self._dftype = None
        self._dataset_passport = None

    # -------------------------------------------------------------------------------------------- #
    #                                     IDENTITY                                                 #
    # -------------------------------------------------------------------------------------------- #
    def name(self, name: str) -> DatasetPassportBuilder:
        """Sets the name for the dataset."""
        self._name = name
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                      FLOW                                                    #
    # -------------------------------------------------------------------------------------------- #
    def phase(self, phase: PhaseDef) -> DatasetPassportBuilder:
        """Sets the phase for the dataset."""
        self._phase = phase
        return self

    def stage(self, stage: StageDef) -> DatasetPassportBuilder:
        """Sets the stage for the dataset."""
        self._stage = stage
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                    PROVENANCE                                                #
    # -------------------------------------------------------------------------------------------- #
    def source(self, source: DatasetPassport) -> DatasetPassportBuilder:
        """Sets the source dataset."""
        self._source = source
        return self

    def parent(self, parent: DatasetPassport) -> DatasetPassportBuilder:
        """Sets the parent passport."""
        self._parent = parent
        return self

    def creator(self, creator: str) -> DatasetPassportBuilder:
        """Sets the creator of the passport."""
        self._creator = creator
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                    FILE FORMAT                                               #
    # -------------------------------------------------------------------------------------------- #

    def csv(self) -> DatasetPassportBuilder:
        """
        Sets the file format to CSV and determines the file path.

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._file_format = FileFormat.CSV
        return self

    def parquet(self) -> DatasetPassportBuilder:
        """
        Sets the file format to Parquet and determines the file path.

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._file_format = FileFormat.PARQUET
        return self

    # -------------------------------------------------------------------------------------------- #
    #                              IN-MEMORY DATA STRUCTURE                                        #
    # -------------------------------------------------------------------------------------------- #
    def as_pandas(self) -> DatasetPassportBuilder:
        """
        Sets the file format to CSV and determines the file path.

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.PANDAS
        return self

    def as_spark(self) -> DatasetPassportBuilder:
        """
        Sets the file format to Parquet and determines the file path.

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.SPARK
        return self

    def build(self) -> DatasetPassportBuilder:
        """
        Builds the DatasetPassport after validating all fields.

        Returns:
            DatasetPassportBuilder: The builder instance for chaining.

        Raises:
            ValueError: If validation fails.
        """
        self._validate()
        self._created = datetime.now()
        self._asset_id, self._version = tuple(self._get_asset_id().as_dict().values())
        self._creator = self._creator or self._get_caller()
        self._dataset_passport = DatasetPassport.create(
            asset_type=self._asset_type,
            asset_id=self._asset_id,
            phase=self._phase,
            stage=self._stage,
            name=self._name,
            creator=self._creator,
            created=self._created,
            version=self._version,
            source=self._source,
            parent=self._parent,
            file_format=self._file_format,
            dftype=self._dftype,
        )
        return self

    def _get_asset_id(self) -> str:
        """Generates a unique asset ID using the workspace."""
        return self._workspace.gen_asset_id(
            asset_type=self._asset_type,
            phase=self._phase,
            stage=self._stage,
            name=self._name,
        )

    def _get_caller(self) -> str:
        # Get the stack frame of the caller
        stack = inspect.stack()
        caller_frame = stack[2]  # The caller of the caller of this method

        # Get the `self` object from the caller's local variables
        caller_self = caller_frame.frame.f_locals.get("self", None)
        # Get the class name if available.
        if caller_self:
            return caller_self.__class__.__name__
        else:
            return "User (not identified)"

    def _validate(self) -> None:
        """Validates that all required fields are set and properly typed."""
        errors = []
        if not isinstance(self._phase, PhaseDef):
            errors.append(f"Invalid phase: {self._phase} (Expected: PhaseDef)")
        if not isinstance(self._stage, StageDef):
            errors.append(f"Invalid stage: {self._stage} (Expected: StageDef)")
        if not isinstance(self._name, str):
            errors.append(f"Invalid name: {self._name} (Expected: str)")
        if not isinstance(self._file_format, FileFormat):
            errors.append(
                f"Invalid file_format: {self._file_format} (Expected: FileFormat)"
            )
        if not isinstance(self._dftype, DFType):
            errors.append(f"Invalid dftype: {self._dftype} (Expected: DFType)")
        if errors:
            raise ValueError("\n".join(errors))
