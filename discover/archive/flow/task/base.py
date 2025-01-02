#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/base.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 04:49:44 pm                                             #
# Modified   : Friday December 27th 2024 05:21:00 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Classes for Task classes """
from __future__ import annotations

import importlib
import logging
from abc import ABC, abstractmethod
from typing import Any

from discover.core.flow import DataPrepStageDef


# ------------------------------------------------------------------------------------------------ #
#                                           TASK                                                   #
# ------------------------------------------------------------------------------------------------ #
class Task(ABC):
    """
    An abstract base class for defining tasks in a pipeline.
    All tasks must implement the `run` method and provide a
    `name` property based on the class name.

    Args:
        phase (PhaseDef): The phase of the data pipeline.
        stage (DataPrepStageDef): The specific stage within the data pipeline.

    Methods:
    --------
    name -> str
        Returns the name of the task, which is the class name of the task instance.

    run(*args, data: Any, **kwargs) -> Any
        Abstract method that must be implemented by any subclass. It represents
        the main logic of the task. Subclasses should specify the expected inputs
        and outputs.
    """

    def __init__(self, phase: PhaseDef, stage: DataPrepStageDef):
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._phase = phase
        self._stage = stage

    @property
    def name(self) -> str:
        """
        Returns the name of the task, which is the class name of the task instance.

        Returns:
        --------
        str
            The name of the task.
        """
        return self.__class__.__name__

    @property
    def phase(self) -> PhaseDef:
        """Returns the phase of the pipeline."""
        return self._phase

    @property
    def stage(self) -> DataPrepStageDef:
        """Returns the specific stage within the pipeline."""
        return self._stage

    @abstractmethod
    def run(self, *args, data: Any, **kwargs) -> Any:
        """
        The core logic of the task. Must be implemented by any subclass.

        Parameters:
        -----------
        *args : tuple
            Positional arguments that the task may require.
        data : Any
            The input data for the task. The specific type of data will depend
            on the implementation of the subclass.
        **kwargs : dict
            Additional keyword arguments that the task may require.

        Returns:
        --------
        Any
            The output of the task, as defined by the subclass implementation.
        """
        pass


# ------------------------------------------------------------------------------------------------ #
def instantiate_class(
    module: str,
    class_name: str,
    params: dict,
):
    """
    Dynamically imports and instantiates a class with the given parameters.

    This function loads a module, retrieves the specified class, and creates an
    instance of the class by passing the provided parameters and stage definition.

    Args:
        module (str): The name of the module containing the class to be instantiated.
        class_name (str): The name of the class to instantiate.
        params (dict): A dictionary of keyword arguments to pass to the class constructor.

    Returns:
        Any: An instance of the specified class.

    Raises:
        ImportError: If the specified module cannot be imported.
        AttributeError: If the specified class is not found in the module.
        TypeError: If the class cannot be instantiated with the provided arguments.
    """
    module = importlib.import_module(module)
    cls = getattr(module, class_name)
    return cls(
        **params,
    )


# ------------------------------------------------------------------------------------------------ #
#                                      TASK BUILDER                                                #
# ------------------------------------------------------------------------------------------------ #
class TaskBuilder:
    """
    A utility class for constructing task instances dynamically.

    This class provides functionality to create task instances based on a configuration
    dictionary, enabling dynamic task instantiation within a data pipeline. It uses
    module and class names specified in the configuration to instantiate the appropriate
    task class.
    """

    @staticmethod
    def build(phase: PhaseDef, stage: DataPrepStageDef, task_config: dict):
        """
        Constructs a task instance based on the given configuration.

        This method dynamically loads the specified module and class, then instantiates
        the class with the provided parameters and stage definition.

        Args:
            task_config (dict): A dictionary containing the task configuration.
                It must include the following keys:
                - "module" (str): The module containing the task class.
                - "class_name" (str): The name of the task class to instantiate.
                - "params" (dict): A dictionary of parameters to pass to the task's constructor.

        Returns:
            Any: An instance of the specified task class.

        Raises:
            KeyError: If the required keys ("module", "class_name", or "params") are missing
                      from the task_config dictionary.
            ImportError: If the specified module cannot be imported.
            AttributeError: If the specified class is not found in the module.
        """
        module = task_config["module"]
        class_name = task_config["class_name"]
        params = task_config["params"]
        params["phase"] = phase
        params["stage"] = stage
        return instantiate_class(
            module=module,
            class_name=class_name,
            params=params,
        )
