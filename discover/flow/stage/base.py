#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/stage/base.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 20th 2024 08:14:05 pm                                              #
# Modified   : Wednesday November 20th 2024 03:49:43 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Stage Base Class  Module"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Optional, Type, Union

import pandas as pd
import pyspark
from dependency_injector.wiring import Provide, inject

from discover.assets.dataset import Dataset
from discover.assets.idgen import AssetIDGen
from discover.container import DiscoverContainer
from discover.core.flow import PhaseDef, StageDef
from discover.core.namespace import NestedNamespace
from discover.flow.task.base import Task, TaskBuilder
from discover.infra.persistence.repo.dataset import DatasetRepo
from discover.infra.utils.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
#                                        STAGE                                                     #
# ------------------------------------------------------------------------------------------------ #
class Stage(ABC):
    """Abstract base class representing a stage in a data pipeline.

    This class defines the structure and functionality for a pipeline stage. Each stage
    contains a series of tasks, operates on source and destination configurations, and
    interfaces with a dataset repository. The class is designed to be extended to implement
    specific stages of a data pipeline.

    Args:
        phase (PhaseDef): The phase to which the stage belongs.
        stage (StageDef): The specific stage identifier within the pipeline.
        source_config (dict): The configuration for the source data.
        destination_config (dict): The configuration for the destination data.
        asset_idgen (Type[AssetIDGen], optional): The class responsible for generating asset IDs.
            Defaults to AssetIDGen.
        repo (DatasetRepo, optional): The repository interface for dataset operations.
            Defaults to `DiscoverContainer.repo.dataset_repo`.
        return_dataset (bool, optional): Indicates whether the dataset should be returned after processing.
            Defaults to False.
        force (bool, optional): If true, forces execution even if the destination dataset exists.
            Defaults to False.
        **kwargs: Additional keyword arguments for stage customization.
    """

    _task_builder = TaskBuilder

    @inject
    def __init__(
        self,
        phase: PhaseDef,
        stage: StageDef,
        source_config: dict,
        destination_config: dict,
        asset_idgen: Type[AssetIDGen] = AssetIDGen,
        repo: DatasetRepo = Provide[DiscoverContainer.repo.dataset_repo],
        return_dataset: bool = False,
        force: bool = False,
        **kwargs,
    ) -> None:
        self._phase = phase
        self._stage = stage
        self._source_config = NestedNamespace(source_config)
        self._destination_config = NestedNamespace(destination_config)
        self._asset_idgen = asset_idgen
        self._repo = repo
        self._return_dataset = return_dataset
        self._force = force
        self._tasks = []
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def phase(self) -> PhaseDef:
        """Returns the phase of the pipeline."""
        return self._phase

    @property
    def stage(self) -> StageDef:
        """Returns the specific stage within the pipeline."""
        return self._stage

    def add_task(self, task: Task) -> None:
        """Adds a task to the stage and assigns the stage identifier to the task.

        Args:
            task (DataEnhancerTask): The task to be added to the stage.

        Raises:
            TypeError: If the task is not an instance of the expected Task class.
        """
        if not isinstance(task, Task):
            raise TypeError(
                f"Expected an instance or subclass of Task. Received type {type(task)}"
            )
        self._tasks.append(task)

    @abstractmethod
    def run(self) -> str:
        """Executes the stage and returns the resulting asset ID.

        This method orchestrates the execution of the tasks defined in the stage,
        processes the source data, and saves the result to the destination.

        Returns:
            str: The asset ID of the generated dataset.

        Raises:
            RuntimeError: If an error occurs during the execution of the stage.
        """
        pass

    @classmethod
    def build(cls, stage_config: dict, force: bool = False, **kwargs) -> Stage:
        """Creates and returns a new stage instance from the provided configuration.

        Args:
            stage_config (dict): The configuration dictionary containing source, destination, and tasks details.
            force (bool, optional): Whether to force execution even if the destination dataset exists. Defaults to False.

        Returns:
            Stage: A new instance of the Stage class, configured with the provided settings.

        Raises:
            KeyError: If the required keys are missing from the stage_config.
            ValueError: If tasks cannot be built from the configuration.
            RuntimeError: If there is an error creating the stage.
        """
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
                    task_config=task_config,
                    stage=StageDef.from_value(stage_config["stage"]),
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

    def _get_asset_id(self, config: NestedNamespace) -> str:
        """Returns the asset id given an asset configuration.

        Args:
            config (NestedNamespace): An asset configuration.

        Returns:
            str: The asset_id for the asset.
        """
        return self._asset_idgen.get_asset_id(
            asset_type=config.asset_type,
            phase=PhaseDef.from_value(config.phase),
            stage=StageDef.from_value(config.stage),
            name=config.name,
        )

    def _load_file(self, filepath: str) -> pd.DataFrame:
        """
        Loads a dataset from a file.

        Args:
            filepath (str): The path to the file to be loaded.

        Returns:
            pd.DataFrame: The loaded dataset as a Pandas DataFrame.

        Raises:
            FileNotFoundError: If the file is not found.
        """
        return IOService.read(filepath=filepath)

    def _dataset_exists(self, asset_id: str) -> bool:
        """Checks if the dataset exists in the repository.

        Args:
            asset_id (str): The identifier for the dataset.

        Returns:
            bool: True if the dataset exists, False otherwise.

        Raises:
            RuntimeError: If an error occurs while checking for dataset existence.
        """
        try:
            return self._repo.exists(asset_id=asset_id)
        except Exception as e:
            msg = f"Error checking dataset {asset_id} existence.\n{str(e)}"
            self._logger.error(msg)
            raise RuntimeError(msg)

    def _load_dataset(
        self, asset_id: str, config: NestedNamespace
    ) -> Union[pd.DataFrame, pyspark.sql.DataFrame]:
        """Loads a dataset from the repository.

        Args:
            asset_id (str): The identifier for the dataset asset.
            config (NestedNamespace): Dataset configuration specifying whether to
                load the dataset as a Pandas or PySpark DataFrame and if NLP dependencies are needed.

        Returns:
            Union[pd.DataFrame, pyspark.sql.DataFrame]: Pandas or PySpark DataFrame.

        Raises:
            FileNotFoundError: If the source dataset cannot be found or loaded.
            RuntimeError: If an unrecognized error occurs during loading.
        """
        try:
            dataset = self._repo.get(
                asset_id=asset_id,
                distributed=config.distributed,
                nlp=config.nlp,
            )

            if config.distributed and "__index_level_0__" in dataset.content.columns:
                # Rename the pandas index column if it exists in the PySpark DataFrame
                dataset.content = dataset.content.withColumnRenamed(
                    "__index_level_0__", "pandas_index"
                )
            return dataset
        except FileNotFoundError as e1:
            msg = f"Dataset {asset_id} not found in the repository.\n{e1}"
            self._logger.error(msg)
            raise FileNotFoundError(msg)
        except Exception as e2:
            msg = f"RuntimeError encountered while attempting to load dataset {asset_id}.\n{e2}"
            self._logger.error(msg)
            raise RuntimeError(msg)

    def _load_dataset_metadata(self, asset_id: str) -> bool:
        """Loads the metadata for a dataset.

        Args:
            asset_id (str): The identifier for the dataset.

        Returns:
            bool: Metadata of the dataset if found.

        Raises:
            RuntimeError: If an error occurs while loading the metadata.
        """
        try:
            return self._repo.get_dataset_metadata(asset_id=asset_id)
        except Exception as e:
            msg = f"Error reading dataset {asset_id} metadata.\n {str(e)}"
            self._logger.error(msg)
            raise RuntimeError(msg)

    def _save_dataset(self, dataset: Dataset, replace_if_exists: bool = False) -> None:
        """Saves a dataset to the repository.

        Args:
            dataset (Dataset): The dataset to be saved.

        Raises:
            RuntimeError: If saving the dataset fails.
        """
        try:
            if self._repo.exists(asset_id=dataset.asset_id) and replace_if_exists:
                self._repo.remove(asset_id=dataset.asset_id)
            self._repo.add(dataset=dataset)
        except Exception as e:
            raise RuntimeError(f"Failed to save dataset\n{dataset} {str(e)}")

    def _save_dataset_metadata(self, dataset: Dataset) -> None:
        """Saves dataset metadata to the repository.

        Args:
            dataset (Dataset): The dataset whose metadata is to be saved.

        Raises:
            RuntimeError: If saving the metadata fails.
        """
        try:
            self._repo.update_dataset_metadata(dataset=dataset)
        except Exception as e:
            raise RuntimeError(f"Failed to save dataset metadata\n{dataset}\n{str(e)}")

    def _remove_dataset(self, asset_id: str) -> None:
        """Removes the dataset with the specified asset_id from the repository.

        Args:
            asset_id (str): The asset ID for the dataset to be removed.

        Raises:
            RuntimeError: If removing the dataset fails.
        """
        try:
            self._repo.remove(asset_id=asset_id)
        except Exception as e:
            raise RuntimeError(
                f"Failed to remove dataset with asset_id: {asset_id}\n {str(e)}"
            )

    def _get_return_value(
        self,
        dataset: Optional[Dataset] = None,
        asset_id: Optional[str] = None,
        config: Optional[NestedNamespace] = None,
        return_dataset: bool = False,
    ) -> Union[Dataset, str]:
        """Obtains and returns a value based on the boolean `return_dataset` instance variable."""
        return_dataset = return_dataset or self._return_dataset
        config = config or self._destination_config  # Config for the returned dataset.

        if return_dataset:
            try:
                return dataset or self._load_dataset(asset_id=asset_id, config=config)
            except Exception as e:
                msg = f"Unable to return dataset. The dataset or a valid asset_id and configuration must be provided.\n{e}"
                raise ValueError(msg)
        else:
            try:
                return asset_id or dataset.asset_id
            except Exception as e:
                msg = f"Unable to return an asset_id. Either the  asset_id or a dataset with an asset_id must be provided.\n{e}"
                raise ValueError(msg)
