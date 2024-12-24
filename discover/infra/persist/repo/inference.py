#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/repo/inference.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 23rd 2024 02:46:53 pm                                               #
# Modified   : Monday December 23rd 2024 03:19:09 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Inference Repo Module"""
from discover.infra.persist.object.base import DAO
from discover.infra.persist.repo.base import AssetRepo


# ------------------------------------------------------------------------------------------------ #
class InferenceRepo(AssetRepo):
    """
    A repository class for managing inference-related assets.

    This class provides an interface for accessing and manipulating inference-related
    assets, such as predictions, inference models, or metadata, using a Data Access Object (DAO).
    It extends the functionality of the `AssetRepo` base class.

    Args:
        dao (DAO): The Data Access Object used to interact with the underlying data store.
    """

    def __init__(self, dao: DAO) -> None:
        """
        Initializes the InferenceRepo with the specified DAO.

        Args:
            dao (DAO): The Data Access Object used to interact with the underlying data store.
        """
        super().__init__(dao=dao)
