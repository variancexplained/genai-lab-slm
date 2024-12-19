#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/assets/data/factory.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 18th 2024 03:26:09 pm                                            #
# Modified   : Thursday December 19th 2024 04:48:42 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Factory Module"""
from typing import Optional, Type

import pandas as pd
from dependency_injector.wiring import Provide, inject
from pydantic import validate_call
from pyspark.sql import DataFrame

from discover.assets.data.dataset import Dataset
from discover.assets.idgen.dataset import DatasetIDGen
from discover.container import DiscoverContainer
from discover.core.data_structure import DataFrameType
from discover.core.flow import PhaseDef, StageDef
from discover.infra.persistence.repo.dataset import DatasetRepo
from discover.infra.persistence.repo.fileset import FilesetRepo
from discover.infra.utils.file.copy import Copy

# ------------------------------------------------------------------------------------------------ #
copier = Copy()


# ------------------------------------------------------------------------------------------------ #
#                                     DATASET FACTORY                                              #
# ------------------------------------------------------------------------------------------------ #
class DatasetFactory:
    @inject
    def __init__(
        self,
        dataset_repo: DatasetRepo = Provide[
            DiscoverContainer.object_persistence.dataset_repo
        ],
        fileset_repo: FilesetRepo = Provide[
            DiscoverContainer.fileset_persistence.fileset_repo
        ],
        idgen: Type[DatasetIDGen] = DatasetIDGen,
    ) -> None:
        self._dataset_repo = dataset_repo
        self._fileset_repo = fileset_repo
        self._idgen = idgen

    # -------------------------------------------------------------------------------------------- #
    #                                    FROM FILE                                                 #
    # -------------------------------------------------------------------------------------------- #
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_parquet_file(
        self,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        filepath: str,
        description: Optional[str] = None,
        dataframe_type: DataFrameType = DataFrameType.PANDAS,
    ) -> Dataset:
        """Create Dataset from a parquet file.

        Used for files outside of the workspace and not yet associated with a Dataset.

        """
        # Obtain a file access object reader for the dataframe type and read the data
        fao = self._fileset_repo._get_read_fao(dataframe_type=dataframe_type)
        data = fao.read(filepath=filepath)

        return self._from_df(
            phase=phase,
            stage=stage,
            name=name,
            data=data,
            dataframe_type=dataframe_type,
            description=description,
        )

    # -------------------------------------------------------------------------------------------- #
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_delimited_file(
        self,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        filepath: str,
        description: Optional[str] = None,
        dataframe_type: DataFrameType = DataFrameType.PANDAS,
    ) -> Dataset:
        """Create Dataset from a parquet file.

        Used for files outside of the workspace and not yet associated with a Dataset. The files
        will be copied to the workspace from lazy loading.

        """
        return self._from_file(
            phase=phase,
            stage=stage,
            name=name,
            filepath=filepath,
            description=description,
            dataframe_type=dataframe_type,
        )

    # -------------------------------------------------------------------------------------------- #
    #                                    FROM DATAFRAME                                            #
    # -------------------------------------------------------------------------------------------- #
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_pandas(
        self,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        data: pd.DataFrame,
        description: Optional[str] = None,
        dataframe_type: DataFrameType = DataFrameType.PANDAS,
    ) -> Dataset:
        return self._from_df(
            phase=phase,
            stage=stage,
            name=name,
            data=data,
            description=description,
            dataframe_type=dataframe_type,
        )

    # -------------------------------------------------------------------------------------------- #
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_spark(
        self,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        data: DataFrame,
        description: Optional[str] = None,
        dataframe_type: DataFrameType = DataFrameType.SPARK,
    ) -> Dataset:
        return self._from_df(
            phase=phase,
            stage=stage,
            name=name,
            data=data,
            description=description,
            dataframe_type=dataframe_type,
        )

    # -------------------------------------------------------------------------------------------- #
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_sparknlp(
        self,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        data: DataFrame,
        description: Optional[str] = None,
        dataframe_type: DataFrameType = DataFrameType.SPARKNLP,
    ) -> Dataset:
        return self._from_df(
            phase=phase,
            stage=stage,
            name=name,
            data=data,
            description=description,
            dataframe_type=dataframe_type,
        )

    # -------------------------------------------------------------------------------------------- #
    #                                       HELPERS                                                #
    # -------------------------------------------------------------------------------------------- #
    @validate_call(config=dict(arbitrary_types_allowed=True))
    def _from_df(
        self,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        data: pd.DataFrame,
        dataframe_type: DataFrameType,
        description: Optional[str] = None,
        **kwargs,
    ) -> Dataset:
        asset_id = self._idgen.generate_asset_id(phase=phase, stage=stage, name=name)
        dataset = Dataset(
            asset_id=asset_id,
            phase=phase,
            stage=stage,
            name=name,
            data=data,
            description=description,
            dataframe_type=dataframe_type,
        )
        self._dataset_repo.add(dataset=dataset)
        self._fileset_repo.add(asset_id=asset_id, data=data)
        return dataset
