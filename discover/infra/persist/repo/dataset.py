#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/repo/dataset.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 23rd 2024 02:46:53 pm                                               #
# Modified   : Monday December 23rd 2024 03:23:16 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Repo Module"""

from discover.infra.persist.object.base import DAO
from discover.infra.persist.repo.base import AssetRepo


# ------------------------------------------------------------------------------------------------ #
class DatasetRepo(AssetRepo):
    """
    A repository class for managing dataset-related assets.

    This class provides an interface for accessing and manipulating dataset-related
    assets, such as raw datasets, processed datasets, or metadata, using a Data Access Object (DAO).
    It extends the functionality of the `AssetRepo` base class.

    Args:
        dao (DAO): The Data Access Object used to interact with the underlying data store.
    """

    def __init__(self, dao: DAO) -> None:
        """
        Initializes the DatasetRepo with the specified DAO.

        Args:
            dao (DAO): The Data Access Object used to interact with the underlying data store.
        """
        super().__init__(dao=dao)
