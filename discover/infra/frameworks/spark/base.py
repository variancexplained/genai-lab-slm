#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/frameworks/spark/base.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 24th 2024 12:50:08 am                                             #
# Modified   : Tuesday September 24th 2024 02:14:26 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #


import atexit
import logging
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from discover.core.flow import PhaseDef
from discover.infra.config.reader import ConfigReader


# ------------------------------------------------------------------------------------------------ #
# Register shutdown hook
def shutdown(session: SparkSession):
    print("Shutting down Spark session...")
    session.stop()


# ------------------------------------------------------------------------------------------------ #
class SparkSessionPool(ABC):
    """
    Base class for managing and pooling Spark sessions.

    This class provides a pool of Spark sessions for different phases (e.g., Data Prep,
    Analysis) and ensures that sessions are reused where possible. It manages session creation,
    reusability, and cleanup using a shutdown hook.

    Attributes:
        _config_reader: An instance of the `ConfigReader` class that retrieves configuration values.
        _spark_config: Contains a mapping of phases to session names, defining the session to be used for each phase.
        _session_config: Contains session-specific configurations like memory and row group sizes.
        _leviathan: A Spark session object for the Leviathan phase.
        _modestia: A Spark session object for the Modestia phase.
        _paul: A Spark session object for the Paul phase.
        _logger: A logger instance used to log warnings and errors.

    Methods:
        get_or_create(phase: PhaseDef) -> SparkSession:
            Returns an existing Spark session or creates a new one for the specified phase.
        create_session(name: str, memory: str, row_group_size: int, retries: int) -> SparkSession:
            Abstract method to create a Spark session with specified configurations.
        _get_session_name(phase: PhaseDef) -> str:
            Returns the session name associated with the specified phase.

    Example usage:
        >>> pool = SparkSessionPoolStandard()
        >>> session = pool.get_or_create(PhaseDef.DATAPREP)
        >>> print(session)
        <pyspark.sql.session.SparkSession object ...>

        # If you want an NLP session pool:
        >>> pool = SparkSessionPoolNLP()
        >>> session = pool.get_or_create(PhaseDef.ANALYSIS)
        >>> print(session)
        <pyspark.sql.session.SparkSession object ...>

        # Phase not supported:
        >>> session = pool.get_or_create(PhaseDef.UNSUPPORTED_PHASE)
        WARNING: Phase unsupported_phase not supported by the SparkSession factory. Returning Modestia session.
    """

    def __init__(self, config_reader_cls: type[ConfigReader] = ConfigReader) -> None:
        self._config_reader = config_reader_cls()
        # Contains mapping of phases to session names.
        self._spark_config = self._config_reader.get_config(
            section="spark", namespace=False
        )
        # Contains mapping of sessions to row_group_size. Used by subclasses
        # to configure, construct, and return spark sessions
        self._session_config = self._config_reader.get_config(section="session")

        # These objects will hold the sessions in the pool
        self._leviathan = None
        self._modestia = None
        self._paul = None

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")


def get_or_create(self, phase: PhaseDef = None) -> SparkSession:
    """
    Retrieves an existing Spark session or creates a new one based on the specified phase.

    If no phase is provided or an unsupported phase is given, the Leviathan session is used as
    the default fallback since it is the most frequently used session.

    Args:
        phase (PhaseDef, optional): The phase for which a Spark session is required. If not
                                    provided or if the phase is unsupported, Leviathan is used.

    Returns:
        SparkSession: The Spark session corresponding to the specified phase. If no phase or
                      an unsupported phase is provided, the Leviathan session is returned.

    Raises:
        RuntimeError: If an error occurs while creating a session.

    Example usage:
        >>> pool = SparkSessionPoolStandard()
        >>> session = pool.get_or_create(PhaseDef.DATAPREP)
        >>> print(session)
        <pyspark.sql.session.SparkSession object ...>

        >>> session = pool.get_or_create()
        WARNING: No phase provided. Defaulting to Leviathan session.
        >>> print(session)
        <pyspark.sql.session.SparkSession object ...>
    """

    if phase is None:
        self._logger.warning("No phase provided. Defaulting to Leviathan session.")
        phase = PhaseDef.LEVIATHAN

    session_name = self._get_session_name(phase=phase)

    if session_name == "leviathan":
        if not self._leviathan:
            self._leviathan = self.create_session(
                name=session_name,
                memory=self._session_config.memory,
                row_group_size=self._session_config.leviathan,
                retries=self._session_config.retries,
            )
            atexit.register(shutdown, self._leviathan)
        return self._leviathan

    elif session_name == "modestia":
        if not self._modestia:
            self._modestia = self.create_session(
                name=session_name,
                memory=self._session_config.memory,
                row_group_size=self._session_config.modestia,
                retries=self._session_config.retries,
            )
            atexit.register(shutdown, self._modestia)
        return self._modestia

    elif session_name == "paul":
        if not self._paul:
            self._paul = self.create_session(
                name=session_name,
                memory=self._session_config.memory,
                row_group_size=self._session_config.paul,
                retries=self._session_config.retries,
            )
            atexit.register(shutdown, self._paul)
        return self._paul
    else:
        msg = f"The SparkSession pool does not have support for the {session_name} session. Falling back to the Leviathan session."
        self._logger.warning(msg)
        if not self._leviathan:
            self._leviathan = self.create_session(
                name="leviathan",
                memory=self._session_config.memory,
                row_group_size=self._session_config.leviathan,
                retries=self._session_config.retries,
            )
        return self._leviathan

    def _get_session_name(self, phase: PhaseDef) -> str:
        try:
            return self._spark_config[phase.value]
        except KeyError:
            msg = f"Phase {phase.value} not supported by the SparkSession factory. Returning Modestia session."
            self._logger.warning(msg)
            return "modestia"

    @abstractmethod
    def create_session(
        self, name: str, memory: str, row_group_size: int, retries: int
    ) -> SparkSession:
        """Returns a SparkSession with the designated row group size."""
