#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/data/factory.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 18th 2024 03:26:09 pm                                            #
# Modified   : Wednesday December 18th 2024 06:28:45 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
from typing import Optional, Type, Union

import pandas as pd
from dependency_injector.wiring import Provide, inject
from pyspark.sql import DataFrame

from discover.asset.data.dataset import Dataset
from discover.asset.idgen.dataset import DatasetIDGen
from discover.container import DiscoverContainer
from discover.core.data_structure import DataFrameType
from discover.core.flow import PhaseDef, StageDef
from discover.infra.persistence.repo.dataset import DatasetRepo
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
        repo_dataset: DatasetRepo = Provide[
            DiscoverContainer.object_persistence.dataset_repo
        ],
        repo_fileset: DatasetRepo = Provide[
            DiscoverContainer.file_persistence.fileset_repo,
        ],
        idgen: Type[DatasetIDGen] = DatasetIDGen,
    ) -> None:
        self._repo_dataset = repo_dataset
        self._repo_fileset = repo_fileset
        self._idgen = idgen

    # -------------------------------------------------------------------------------------------- #
    #                                    FROM FILE                                                 #
    # -------------------------------------------------------------------------------------------- #
    def from_parquet_file(
        self,
        phase: PhaseDef,
        stage: Union[str, StageDef],  # May be an experiment (str) or a StageDef object.
        name: str,
        filepath: str,
        description: Optional[str] = None,
        dataframe_type: DataFrameType = DataFrameType.PANDAS,
    ) -> Dataset:
        """Create Dataset from a parquet file.

        Used for files outside of the workspace and not yet associated with a Dataset. The files
        will be copied to the workspace from lazy loading.

        """
        return self._from_df(
            phase=phase,
            stage=stage,
            name=name,
            filepath=filepath,
            description=description,
            dataframe_type=dataframe_type,
        )

    def from_delimited_file(
        self,
        phase: PhaseDef,
        stage: Union[str, StageDef],  # May be an experiment (str) or a StageDef object.
        name: str,
        filepath: str,
        description: Optional[str] = None,
        dataframe_type: DataFrameType = DataFrameType.PANDAS,
    ) -> Dataset:
        """Create Dataset from a parquet file.

        Used for files outside of the workspace and not yet associated with a Dataset. The files
        will be copied to the workspace from lazy loading.

        """
        return self._from_df(
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
    def from_pandas(
        self,
        phase: PhaseDef,
        stage: Union[str, StageDef],  # May be an experiment (str) or a StageDef object.
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
    def from_spark(
        self,
        phase: PhaseDef,
        stage: Union[str, StageDef],  # May be an experiment (str) or a StageDef object.
        name: str,
        data: DataFrame,
        description: Optional[str] = None,
        dataframe_type: DataFrameType = Union[
            DataFrameType.SPARK, DataFrameType.SPARKNLP
        ],
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
    def _from_df(
        self,
        phase: PhaseDef,
        stage: Union[str, StageDef],  # May be an experiment (str) or a StageDef object.
        name: str,
        data: pd.DataFrame,
        description: Optional[str] = None,
        dataframe_type: DataFrameType = Union[
            DataFrameType.PANDAS, DataFrameType.SPARK, DataFrameType.SPARKNLP
        ],
        **kwargs,
    ) -> Dataset:
        dataset = Dataset(
            asset_id=self._get_asset_id(phase=phase, stage=stage, name=name),
            name=name,
            data=data,
            description=description,
            dataframe_type=dataframe_type,
        )
        self._repo.add(dataset=dataset)
        return dataset

    # -------------------------------------------------------------------------------------------- #
    def _from_file(
        self,
        phase: PhaseDef,
        stage: Union[str, StageDef],  # May be an experiment (str) or a StageDef object.
        name: str,
        filepath: str,
        description: Optional[str] = None,
        dataframe_type: DataFrameType = DataFrameType.PANDAS,
    ) -> Dataset:
        asset_id = self._idgen.generate_asset_id(phase=phase, stage=stage, name=name)
        # Get target path for the asset_id in the workspace.
        target = self._repo_fileset.get_filepath(asset_id=asset_id)
        # Copy the files to the workspace
        self._copy_to_workspace(source=filepath, target=target, overwrite=False)
        # Create dataset
        dataset = Dataset(
            asset_id=asset_id,
            name=name,
            description=description,
            dataframe_type=dataframe_type,
        )
        self._repo.add(dataset=dataset)
        return dataset

    # -------------------------------------------------------------------------------------------- #
    def _get_asset_id(
        self, phase: PhaseDef, stage: Union[str, StageDef], name: str
    ) -> str:
        stage = stage if isinstance(stage, str) else stage.value
        return self._idgen.generate_asset_id(phase=phase.value, stage=stage, name=name)

    # -------------------------------------------------------------------------------------------- #
    def _copy_to_workspace(
        self, source: str, target: str, overwrite: bool = False
    ) -> None:
        if os.path.isfile(source) and os.path.isfile(target):
            copier.file(source=source, target=target, overwrite=overwrite)
        elif os.path.isdir(source) and os.path.isdir(target):
            copier.directory(source=source, target=target, overwrite=overwrite)
