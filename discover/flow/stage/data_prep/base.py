#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/stage/data_prep/base.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 20th 2024 08:14:05 pm                                              #
# Modified   : Wednesday December 25th 2024 05:26:19 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data preparation stage base module."""
from __future__ import annotations

import logging
from typing import Type, Union

import pandas as pd
from pyspark.sql import DataFrame

from discover.asset.dataset import (
    DataFrameStructure,
    Dataset,
    DatasetFactory,
    deserialize_dataset_config,
)
from discover.core.flow import PhaseDef, StageDef
from discover.flow.stage.base import Stage
from discover.infra.service.logging.stage import stage_logger


# ------------------------------------------------------------------------------------------------ #
#                                    DATAPREP STAGE                                                #
# ------------------------------------------------------------------------------------------------ #
class DataPrepStage(Stage):
    """Represents a data preparation stage in a pipeline.

    This stage is responsible for preparing data by applying a sequence of tasks to a source dataset
    and creating a transformed destination dataset.

    Args:
        phase (PhaseDef): The phase of the pipeline to which this stage belongs.
        stage (StageDef): The specific stage within the pipeline.
        source_config (dict): Configuration for the source dataset.
        destination_config (dict): Configuration for the destination dataset.
        dataset_factory_cls (Type[DatasetFactory], optional): Factory class for creating datasets. Defaults to DatasetFactory.
        force (bool, optional): Whether to force execution, overriding default conditions. Defaults to False.
        **kwargs: Additional keyword arguments for extended functionality.

    Attributes:
        _dataset_factory (DatasetFactory): Factory for creating datasets.
        _source_config (dict): Deserialized configuration for the source dataset.
        _destination_config (dict): Deserialized configuration for the destination dataset.
        _source_asset_id (str): Asset ID of the source dataset.
        _destination_asset_id (str): Asset ID of the destination dataset.
        _logger (logging.Logger): Logger for the stage.
    """

    def __init__(
        self,
        phase: PhaseDef,
        stage: StageDef,
        source_config: dict,
        destination_config: dict,
        dataset_factory_cls: Type[DatasetFactory] = DatasetFactory,
        force: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(phase=phase, stage=stage, force=force, **kwargs)

        self._source_config = source_config
        self._destination_config = destination_config

        self._dataset_factory = dataset_factory_cls()

        # Obtain asset IDs for the source and destination datasets.
        self._source_asset_id = self._workspace_service.get_asset_id(
            **self._source_config
        )
        self._destination_asset_id = self._workspace_service.get_asset_id(
            **self._destination_config
        )

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @stage_logger
    def run(self) -> Dataset:
        """Executes the data preparation stage.

        If the destination dataset already exists and `force` is not enabled, the existing dataset is returned.
        Otherwise, the stage processes the source dataset and generates a new destination dataset.

        Returns:
            Dataset: The resulting dataset after executing the stage.
        """
        if (
            self._workspace_service.dataset_repo.exists(
                asset_id=self._destination_asset_id
            )
            and not self._force
        ):
            return self._workspace_service.dataset_repo.get(
                asset_id=self._destination_asset_id
            )
        else:
            return self._run()

    def _run(self) -> Dataset:
        """Processes the source dataset and applies tasks to generate the destination dataset.

        Returns:
            Dataset: The newly created destination dataset.
        """
        # Obtain the source data in the specified dataframe structure
        data = self._get_data(
            asset_id=self._source_asset_id,
            dataframe_structure=self._source_config["dataframe_structure"],
        )

        # Apply tasks to the data sequentially.
        for task in self._tasks:
            data = task.run(data=data)

        # Create, persist and return the output (destination) dataset.
        return self._create_destination_dataset(data=data)

    def _get_data(
        self, asset_id: str, dataframe_structure: DataFrameStructure
    ) -> Union[pd.DataFrame, DataFrame]:
        # Obtain the source dataset and extract the DataFrame in the specified structure.
        dataset = self._workspace_service.dataset_repo.get(asset_id=asset_id)
        return dataset.as_df(dataframe_structure=dataframe_structure)

    def _create_destination_dataset(
        self, data: Union[pd.DataFrame, DataFrame]
    ) -> Dataset:

        # Delete existing destination dataset, if it exists.
        if self._workspace_service.dataset_repo.exists(
            asset_id=self._destination_asset_id
        ):
            self._workspace_service.dataset_repo.remove(
                asset_id=self._destination_asset_id
            )

        # Create the new destination dataset and return it.
        return self._dataset_factory.from_df(
            phase=self._destination_config["phase"],
            stage=self._destination_config["stage"],
            name=self._destination_config["name"],
            data=data,
            dataframe_structure=self._destination_config["dataframe_structure"],
            file_format=self._destination_config["file_format"],
        )

    @classmethod
    def build(
        cls,
        stage_config: dict,
        force: bool = False,
        **kwargs,
    ) -> Stage:
        """Builds a DataPrepStage instance from a configuration dictionary.

        Args:
            stage_config (dict): Configuration dictionary containing stage, source, destination, and task definitions.
            force (bool, optional): Whether to force execution, overriding default conditions. Defaults to False.
            **kwargs: Additional keyword arguments for extended functionality.

        Returns:
            Stage: An instance of DataPrepStage.

        Raises:
            ValueError: If a required key is missing from the configuration.
            RuntimeError: If an error occurs during stage creation.
        """
        # Deserialize source and destination dataset config.
        stage_config["source_config"] = deserialize_dataset_config(
            config=stage_config["source_config"]
        )
        stage_config["destination_config"] = deserialize_dataset_config(
            config=stage_config["destination_config"]
        )
        try:
            # Instantiate the Stage object
            stage = cls(
                phase=PhaseDef.from_value(stage_config["phase"]),
                stage=StageDef.from_value(stage_config["stage"]),
                source_config=stage_config["source_config"],
                destination_config=stage_config["destination_config"],
                force=force,
                **kwargs,
            )

            # Construct the Stage's Task objects
            tasks = [
                cls._task_builder.build(
                    phase=PhaseDef.from_value(stage_config["phase"]),
                    stage=StageDef.from_value(stage_config["stage"]),
                    task_config=task_config,
                )
                for task_config in stage_config["tasks"]
            ]

            # Add Tasks to the Stage object
            for task in tasks:
                stage.add_task(task)

            return stage

        except KeyError as e:
            raise ValueError(
                f"Missing required configuration key in stage_config: {str(e)}"
            ) from e
        except Exception as e:
            raise RuntimeError(
                f"Failed to build stage from configuration: {str(e)}"
            ) from e
