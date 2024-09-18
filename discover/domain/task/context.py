#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/value_objects/context.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 13th 2024 02:12:54 pm                                              #
# Modified   : Tuesday September 17th 2024 01:34:21 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass

from discover.core.data import DataClass
from discover.domain.value_objects.lifecycle import Phase, Stage


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Context(DataClass):
    """
    Represents the contextual information for a particular task within a workflow.

    This class provides details about the current phase, stage, and task in a data processing
    or analysis pipeline. It helps to track the state and progress of a task in the pipeline
    by associating it with a specific phase and stage.

    Attributes:
    -----------
    phase : Phase
        The current phase of the workflow, represented by the Phase enum (e.g., Data Preparation, Analysis).
    stage : Stage
        The current stage within the phase, represented by the Stage enum (e.g., RAW, CLEAN, EDA).
    task : str
        A string describing the specific task being executed at the current stage.

    Examples:
    ---------
    >>> context = Context(phase=Phase.DATAPREP, stage=DataPrepStage.RAW, task="Ingesting raw data")
    >>> print(context)
    Context(phase=<Phase.DATAPREP: ('data_prep', 'Data Preparation Phase')>, stage=<DataPrepStage.RAW: ('00_raw', 'Raw Stage')>, task='Ingesting raw data')
    """

    phase: Phase
    stage: Stage
    task: str
