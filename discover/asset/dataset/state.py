#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/state.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 22nd 2025 01:20:36 am                                             #
# Modified   : Saturday January 25th 2025 01:15:06 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Dataset State Module"""
from __future__ import annotations

import logging
from dataclasses import field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

import pandas as pd
from explorify import DataClass
from pydantic.dataclasses import dataclass

from discover.core.dtypes import IMMUTABLE_TYPES, SEQUENCE_TYPES


# ------------------------------------------------------------------------------------------------ #
class DatasetStateDef(Enum):
    CREATED = ("created", "Dataset Created")
    PUBLISHED = ("published", "Dataset Published to Repository")
    CONSUMED = ("consumed", "Dataset Consumed")

    @classmethod
    def from_value(cls, value) -> DatasetStateDef:
        """Finds the enum member based on a given value"""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for value: {value}")

    def __new__(cls, name: str, label: str):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.label = label
        return obj


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetState(DataClass):
    """
    Represents the state of a dataset, including its lifecycle events and timestamps.

    Args:
        asset_id (str): The identifier for the dataset.
        creator (str): The creator of the dataset.

    Attributes:
        creator (str): The entity responsible for creating the dataset.
        status (DatasetStateDef): The current status of the dataset.
        created (datetime): The timestamp of when the dataset was created.
        published (bool): Boolean indicates whether dataset has been published.
        consumed (bool): Boolean indicates whether dataset has been consumed.
        accessed (Optional[datetime]): The timestamp of when the dataset was last accessed.
        modified (datetime): The timestamp of the last modification to the dataset.
        _eventlog (list[dict]): A log of events associated with the dataset.
    """

    asset_id: str
    creator: str
    created = None
    accessed = None
    modified = None
    published: bool = False
    consumed: bool = False
    status: Optional[DatasetStateDef] = DatasetStateDef.CREATED
    _eventlog: list = field(default_factory=list)

    _logger = logging.getLogger(f"{__name__}.DatasetState")

    def __post_init__(self) -> None:
        self.created = datetime.now()
        event = f"{self.status.label} by {self.creator}"
        self.add_event(entity=self.creator, event=event)
        self._logger.debug(f"{self.creator} created dataset at {self.created}.")

    @classmethod
    def _export_config(
        cls,
        v: Any,
    ) -> Any:
        """Returns v with Configs converted to dicts, recursively."""
        if isinstance(v, IMMUTABLE_TYPES):
            return v
        elif isinstance(v, SEQUENCE_TYPES):
            return type(v)(map(cls._export_config, v))
        elif isinstance(v, dict):
            return v
        elif isinstance(v, datetime):
            return v.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(v, DatasetStateDef):
            return v.label
        elif hasattr(v, "as_dict"):
            return v.as_dict()
        else:
            return dict()

    def publish(self, entity: str) -> None:
        """
        Marks the dataset as published and logs the event.

        Args:
            entity (str): The entity that publishes the dataset.
        """
        self.status = DatasetStateDef.PUBLISHED
        self.modified = datetime.now()
        event = f"{self.status.label} by {entity}"
        self.add_event(entity=entity, event=event)
        self.published = True
        self._logger.debug(f"Dataset published by {entity} at {self.modified}")

    def consume(self, entity: str) -> None:
        """
        Marks the dataset as consumed and logs the event.

        Args:
            entity (str): The entity that consumes the dataset.
        """
        self.status = DatasetStateDef.CONSUMED
        self.modified = datetime.now()
        event = f"{self.status.label} by {entity}"
        self.add_event(entity=entity, event=event)
        self.consumed = True
        self._logger.debug(f"Dataset consumed by {entity} at {self.modified}")

    def access(self, entity: str) -> None:
        """
        Records the timestamp of when the dataset was accessed and logs the event.

        Args:
            entity (str): The entity that accessed the dataset.
        """
        self.accessed = datetime.now()
        event = f"Accessed by {entity}"
        self.add_event(entity=entity, event=event)
        self._logger.debug(f"Dataset accessed by {entity} at {self.accessed}")

    def add_event(self, entity: str, event: str) -> None:
        """
        Adds an event to the event log with a timestamp.

        Args:
            entity (str): The entity responsible for the event.
            event (str): A description of the event.
        """
        event_entry = {
            "timestamp": datetime.now(),
            "entity": entity,
            "event": event,
        }
        self._eventlog.append(event_entry)
        self.modified = datetime.now()
        self._logger.debug(f"Added {event_entry} to the event log at {self.modified}.")

    def get_events(self) -> pd.DataFrame:
        """
        Returns the event log as a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing the event log with timestamps,
            entities, and event descriptions.
        """
        return pd.DataFrame(self._eventlog)
