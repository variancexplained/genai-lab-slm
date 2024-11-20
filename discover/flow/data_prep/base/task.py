#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/base/task.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday November 20th 2024 01:22:52 am                                            #
# Modified   : Wednesday November 20th 2024 07:09:55 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from discover.flow.base.task import Task


# ------------------------------------------------------------------------------------------------ #
#                                  DATA ENHANCER TASK                                              #
# ------------------------------------------------------------------------------------------------ #
class DataEnhancerTask(Task):
    """
    A task class for enhancing data by adding or modifying attributes in a data pipeline.

    This class facilitates the creation of new columns within a data processing pipeline.
    It prefixes the new column name with the stage's unique identifier to ensure consistency
    and traceability across different stages.

    Attributes:
        new_column (str): The fully-qualified name of the new column, prefixed with the stage ID.

    Args:
        new_column (str): The base name of the new column to be added.
        stage (Optional[StageDef]): The stage definition that this task belongs to. Defaults to None.
        **kwargs: Additional arguments passed to the parent `Task` class.
    """

    def __init__(self, new_column: str, return_dataset: bool = False, **kwargs) -> None:
        super().__init__(**kwargs)
        self._new_column = f"{self.stage.id}_{new_column}"
        self._return_dataset = return_dataset

    @property
    def new_column(self) -> str:
        """The fully-qualified name of the new column, prefixed with the stage ID."""
        return self._new_column


# ------------------------------------------------------------------------------------------------ #
#                                    DATA PREP TASK                                                   #
# ------------------------------------------------------------------------------------------------ #
class DataPrepTask(Task):
    """
    A task class for modifying existing data within a data pipeline.

    This class applies transformations or adjustments to data that has already been
    generated or processed by other tasks or stages in the pipeline. It serves to refine,
    standardize, or augment data in preparation for subsequent stages.

    Args:
        stage (Optional[StageDef]): The stage definition that this task belongs to. Defaults to None.
        **kwargs: Additional arguments passed to the parent `Task` class.
    """

    def __init__(self, return_dataset: bool = False, **kwargs) -> None:
        super().__init__(**kwargs)
        self._return_dataset = return_dataset
