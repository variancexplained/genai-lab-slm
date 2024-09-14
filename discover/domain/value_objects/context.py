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
# Modified   : Saturday September 14th 2024 05:50:52 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Optional, Union

from discover.core.data import DataClass
from discover.domain.base.identity import IDXGen
from discover.domain.value_objects.lifecycle import Stage


# ------------------------------------------------------------------------------------------------ #
class Context(DataClass):
    """
    Context class for tracking service metadata and generating run IDs.

    The `Context` class manages metadata such as service type, service name,
    and stage for a pipeline or task. It also generates unique run IDs
    through an injected ID generation strategy (`IDXGen`). This ensures each
    run of a service or task is uniquely tracked.

    Attributes:
    -----------
    _idxgen : IDXGen
        The ID generation strategy used to generate unique run IDs.
    service_type : Optional[str]
        The type of the service (e.g., "Pipeline", "Task").
    service_name : Optional[str]
        The name of the service being executed.
    stage : Optional[Stage]
        The current stage of the service (e.g., INGEST, TRANSFORM).
    runid : Optional[str]
        The unique identifier for the current run of the service.

    Methods:
    --------
    create_run(owner: Union[type, object]) -> None:
        Generates a new run ID for the given owner (either a class or instance).
        The `runid` is generated by invoking the ID generation strategy (`IDXGen`).
    """

    def __init__(self, idxgen_cls: IDXGen) -> None:
        """
        Initializes the Context with the given ID generation class.

        Parameters:
        -----------
        idxgen_cls : IDXGen
            The class used to generate unique run IDs for services or tasks.
        """
        self._idxgen = idxgen_cls()
        self.service_type: Optional[str] = None
        self.service_name: Optional[str] = None
        self.stage: Optional[Stage] = None
        self.runid: Optional[str] = None

    def create_run(self, owner: Union[type, object]) -> None:
        """
        Generates a new run ID for the given owner.

        Parameters:
        -----------
        owner : Union[type, object]
            The class or instance for which the run ID is being generated.
        """
        self.runid = self._idxgen.get_next_id(owner=owner)
