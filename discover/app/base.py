#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/app/base.py                                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 18th 2024 11:07:32 am                                                #
# Modified   : Sunday November 10th 2024 08:31:37 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Analysis Class"""
from __future__ import annotations

from abc import ABC

import pandas as pd
from dependency_injector.wiring import Provide, inject

from discover.container import DiscoverContainer
from discover.infra.persistence.repo.dataset import DatasetRepo
from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
printer = Printer()


# ------------------------------------------------------------------------------------------------ #
class Analysis(ABC):

    @inject
    def __init__(
        self, repo: DatasetRepo = Provide[DiscoverContainer.repo.dataset_repo]
    ) -> None:
        self._repo = repo

    def load_data(self, asset_id: str) -> pd.DataFrame:
        return self._repo.get(asset_id=asset_id, distributed=False, nlp=False)
