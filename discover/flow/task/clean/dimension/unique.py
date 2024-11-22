#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/clean/dimension/unique.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday November 22nd 2024 01:08:57 am                                               #
# Modified   : Friday November 22nd 2024 01:43:21 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Literal, Union

from discover.flow.task.clean.dimension.anomaly import NominalAnomaly


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairUniquenessTask(NominalAnomaly):
    """
    Task for detecting or repairing uniqueness anomalies in a dataset.

    This task identifies or resolves duplicate values in a specified column
    based on the configured detection and repair strategies. It supports both
    local and distributed execution modes.

    Args:
        column (str): The name of the column to analyze for duplicates.
            Defaults to "id".
        new_column (str): The name of the column where the results of the
            detection or repair will be stored. Defaults to "contains_duplicate_id".
        mode (str): The mode of operation, either "detect" or "repair".
            Defaults to "detect".
        distributed (bool): Whether to run the task in distributed mode. Defaults to True.
        detect_strategy (str): The strategy used to detect uniqueness anomalies.
            Defaults to "unique".
        repair_strategy (str): The strategy used to repair uniqueness anomalies.
            Defaults to "unique".
        threshold (Union[float, int]): An optional threshold for flagging anomalies.
            Can be used for count-based or proportion-based thresholds. Defaults to None.
        threshold_type (Literal["count", "proportion"]): The type of threshold to apply.
            Options are "count" or "proportion". Defaults to None.
        unit (Literal["word", "character"]): The unit of measurement for thresholds.
            Options are "word" or "character". Defaults to None.
        **kwargs: Additional keyword arguments passed to the base class.
    """

    def __init__(
        self,
        column: str = "id",
        new_column: str = "contains_duplicate_id",
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "unique",
        repair_strategy: str = "unique",
        threshold: Union[float, int] = None,
        threshold_type: Literal["count", "proportion"] = None,
        unit: Literal["word", "character"] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )
