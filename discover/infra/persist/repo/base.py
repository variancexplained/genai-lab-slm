#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/repo/base.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 23rd 2024 02:33:35 pm                                               #
# Modified   : Monday December 23rd 2024 02:46:24 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import pandas as pd

from discover.asset.base import Asset
from discover.asset.repo import Repo
from discover.infra.persist.object.base import DAO


# ------------------------------------------------------------------------------------------------ #
class AssetRepo(Repo):
    def __init__(self, dao: DAO) -> None:
        self._dao = dao

    def add(self, asset: Asset) -> None:
        self._dao.create(asset=asset)

    def get(self, asset_id: str) -> Asset:
        return self._dao.read(asset_id=asset_id)

    def remove(self, asset_id: str) -> None:
        self._dao.delete(asset_id=asset_id)

    def exists(self, asset_id: str):
        return self._dao.exists(asset_id=asset_id)

    def list(self) -> pd.DataFrame:
        assets = self._dao.read_all()
        asset_list = []
        for k, v in assets.items():
            asset_list.append(v.as_dict())
        return pd.DataFrame(data=asset_list)
