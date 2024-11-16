#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/base/task.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 04:49:44 pm                                             #
# Modified   : Saturday November 16th 2024 02:31:40 am                                             #
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


# ------------------------------------------------------------------------------------------------ #
#                                           TASK                                                   #
# ------------------------------------------------------------------------------------------------ #
class Task(ABC):
    """
    An abstract base class for defining tasks in a pipeline.
    All tasks must implement the `run` method and provide a
    `name` property based on the class name.

    Methods:
    --------
    name -> str
        Returns the name of the task, which is the class name of the task instance.

    run(*args, data: Any, **kwargs) -> Any
        Abstract method that must be implemented by any subclass. It represents
        the main logic of the task. Subclasses should specify the expected inputs
        and outputs.
    """

    def __init__(self):
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

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
def instantiate_class(module: str, class_name: str, params: dict):
    """
    Dynamically imports a module and instantiates a class with the given parameters.

    Parameters
    ----------
    module : str
        The name of the module from which to import the class (e.g., 'mypackage.mymodule').
    class_name : str
        The name of the class to instantiate from the module.
    params : dict
        A dictionary of additional parameters to pass to the class constructor.

    Returns
    -------
    object
        An instance of the specified class with the provided parameters.

    Raises
    ------
    ModuleNotFoundError
        If the specified module cannot be found.
    AttributeError
        If the specified class does not exist in the module.
    TypeError
        If the class constructor does not accept the provided parameters.

    Examples
    --------
    >>> obj = instantiate_class(
    ...     module='mypackage.mymodule',
    ...     class_name='Normalizer',
    ...     params={'param1': value1, 'param2': value2}
    ... )
    """
    module = importlib.import_module(module)
    cls = getattr(module, class_name)
    return cls(**params)


# ------------------------------------------------------------------------------------------------ #
#                                      TASK BUILDER                                                #
# ------------------------------------------------------------------------------------------------ #
class TaskBuilder:
    """
    A builder class for constructing task instances from configuration data.

    The `TaskBuilder` class provides a static method, `build`, which reads task configuration
    data and dynamically creates an instance of the specified task class using the provided
    parameters. This allows for flexible and dynamic task instantiation based on configuration.

    Methods
    -------
    build(task_config: dict) -> object
        Constructs and returns an instance of a task class using the specified configuration.

    Examples
    --------
    >>> task_config = {
    ...     'module_name': 'mypackage.mymodule',
    ...     'class_name': 'NormalizationTask',
    ...     'params': {'param1': value1, 'param2': value2}
    ... }
    >>> task_instance = TaskBuilder.build(task_config)
    """

    @staticmethod
    def build(task_config):
        """
        Builds and returns an instance of a task class based on the provided configuration.

        Parameters
        ----------
        task_config : dict
            A dictionary containing task configuration with the following keys:
                - 'phase' (str): The phase associated with the task (e.g., 'preprocessing').
                - 'stage' (str): The stage within the phase (e.g., 'normalization').
                - 'module_name' (str): The name of the module containing the task class.
                - 'class_name' (str): The name of the task class to instantiate.
                - 'params' (dict): Additional parameters to pass to the task's constructor.

        Returns
        -------
        object
            An instance of the specified task class initialized with the provided parameters.

        Raises
        ------
        ModuleNotFoundError
            If the specified module cannot be found.
        AttributeError
            If the specified class does not exist in the module.
        TypeError
            If the class constructor does not accept the provided parameters.

        Examples
        --------
        >>> task_config = {
        ...     'module_name': 'mypackage.mymodule',
        ...     'class_name': 'NormalizationTask',
        ...     'params': {'param1': value1, 'param2': value2}
        ... }
        >>> task_instance = TaskBuilder.build(task_config)
        """
        module = task_config["module"]
        class_name = task_config["class_name"]
        params = task_config["params"]
        return instantiate_class(
            module=module,
            class_name=class_name,
            params=params,
        )
