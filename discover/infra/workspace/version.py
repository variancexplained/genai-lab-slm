#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/workspace/version.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 30th 2024 02:41:08 am                                               #
# Modified   : Monday December 30th 2024 03:52:15 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
import shelve


# ------------------------------------------------------------------------------------------------ #
class VersionManager:
    def __init__(self, version_db_path: str):
        """
        Manages versioning using a shelve database.

        Args:
            version_db_path (str): Path to the shelve database for version tracking.
        """
        os.makedirs(os.path.dirname(version_db_path), exist_ok=True)
        self._version = version_db_path

    def get_next_version(self, phase, stage):
        """
        Combines phase and stage IDs into a key and retrieves the next sequence number for that key.

        Args:
            phase (Enum): Enum representing the phase, with an 'id' attribute.
            stage (Enum): Enum representing the stage, with an 'id' attribute.

        Returns:
            str: The next version in the format "v{phase_id}.{stage_id}.{sequence}".
        """
        if not hasattr(phase, "id") or not hasattr(stage, "id"):
            raise ValueError("Phase and Stage must have 'id' attributes.")

        # Combine phase and stage IDs into a key
        version_key = f"v{phase.id}.{stage.id}"

        # Open the shelve database
        with shelve.open(self._version) as db:
            # Retrieve the current sequence number or start at 0
            sequence = db.get(version_key, 0)

            # Increment and store the next sequence number
            db[version_key] = sequence + 1

        # Return the full version string
        return f"{version_key}.{sequence + 1}"
