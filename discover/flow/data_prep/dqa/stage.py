#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/dqa/stage.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday October 19th 2024 12:59:20 pm                                              #
# Modified   : Thursday October 24th 2024 01:01:17 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
from typing import List

import pandas as pd
from dependency_injector.wiring import inject

from discover.assets.idgen import AssetIDGen
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.flow.base.task import Task
from discover.flow.data_prep.stage import DataPrepStage
from discover.infra.service.logging.stage import stage_logger

# ------------------------------------------------------------------------------------------------ #
#                              DATA PREP STAGE W/ CACHE                                            #
# ------------------------------------------------------------------------------------------------ #


class DQAStage(DataPrepStage):
    """
    A stage class for preparing datasets, handling loading, processing, and saving of data.

    The `DataPrepStage` class orchestrates the execution of data preparation tasks,
    including loading source datasets, applying a series of tasks, and saving the processed
    data to a destination. It uses a repository for dataset persistence and can be configured
    to force execution even if the destination dataset already exists.

    Parameters
    ----------
    source_config : dict
        Configuration for the source dataset, including details like phase, stage, and name.
    destination_config : dict
        Configuration for the destination dataset, including details like phase, stage, and name.
    tasks : List[Task]
        A list of tasks to execute as part of the data preparation stage.
    force : bool, optional
        Whether to force execution if the destination dataset endpoint already exists (default is False).
    repo : DatasetRepo, optional
        A repository for dataset persistence, injected via dependency injection (default is `DiscoverContainer.repo.dataset_repo`).
    **kwargs : dict
        Additional keyword arguments for stage configuration.

    Attributes
    ----------
    _repo : DatasetRepo
        The repository instance used for dataset persistence.
    _source_asset_id : str
        The generated asset ID for the source dataset based on the configuration.
    _destination_asset_id : str
        The generated asset ID for the destination dataset based on the configuration.
    _logger : logging.Logger
        Logger instance for logging events related to the data preparation stage.

    Methods
    -------
    run() -> None
        Executes the stage by loading the source dataset, applying tasks, and saving the result.
    _create_destination_dataset(data: Union[pd.DataFrame, pyspark.sql.DataFrame]) -> Dataset
        Creates the destination dataset with the processed data and configuration details.
    _load_source_dataset() -> Dataset
        Loads the source dataset from the repository using the source asset ID.
    _save_destination_dataset(dataset: Dataset) -> None
        Saves the processed dataset to the repository using the destination asset ID.
    _endpoint_exists(asset_id: str) -> bool
        Checks if the dataset endpoint already exists in the repository.

    Examples
    --------
    >>> source_config = {'phase': 'preprocessing', 'stage': 'normalization', 'name': 'raw_data'}
    >>> destination_config = {'phase': 'preprocessing', 'stage': 'normalized', 'name': 'cleaned_data'}
    >>> tasks = [Task1(), Task2()]
    >>> data_prep_stage = DataPrepStage(
    ...     source_config=source_config,
    ...     destination_config=destination_config,
    ...     tasks=tasks,
    ...     force=True
    ... )
    >>> data_prep_stage.run()

    Notes
    -----
    The `DataPrepStage` class leverages dependency injection to retrieve a dataset repository instance.
    It ensures that datasets are properly loaded and saved based on the specified configurations.
    """

    @inject
    def __init__(
        self,
        source_config: dict,
        destination_config: dict,
        tasks: List[Task],
        force: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(
            source_config=source_config,
            destination_config=destination_config,
            tasks=tasks,
            force=force,
        )

        self._destination_asset_id = AssetIDGen.get_asset_id(
            asset_type=self._destination_config.asset_type,
            phase=PhaseDef.from_value(value=self._destination_config.phase),
            stage=DataPrepStageDef.from_value(value=self._destination_config.stage),
            name=self._destination_config.name,
        )

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @stage_logger
    def run(self) -> str:
        """Executes the stage by loading the source dataset, applying tasks, and saving the result.

        Returns:
            asset_id (str): Returns the asset_id for the asset created.
        """
        if (
            self._endpoint_exists(asset_id=self._destination_asset_id)
            and not self._force
        ):
            return self._destination_asset_id
        else:
            if self._repo.exists(asset_id=self._destination_asset_id):
                self._repo.remove(asset_id=self._destination_asset_id)

            data = self._load_source_data()
            results = []

            for task in self._tasks:
                result = task.run(data=data)
                results.append(result)

            data = self._merge_data_results(data=data, results=results)

            dataset = self._create_destination_dataset(data=data)

            self._save_destination_dataset(dataset=dataset)

            return self._destination_asset_id

    def _merge_data_results(self, data: pd.DataFrame, results: list) -> pd.DataFrame:
        """Merges the data with the results of the data quality assessment."""
        results = pd.concat(results, axis=1)
        data = pd.concat((data, results), axis=1)
        return data
