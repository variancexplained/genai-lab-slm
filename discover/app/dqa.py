#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/app/dqa.py                                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 18th 2024 10:43:56 am                                                #
# Modified   : Friday October 18th 2024 06:57:26 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Quality Analysis Module"""

import pandas as pd

from discover.app.base import Analysis
from discover.assets.idgen import AssetIDGen
from discover.core.flow import DataPrepStageDef, PhaseDef

# ------------------------------------------------------------------------------------------------ #
idgen = AssetIDGen()


# ------------------------------------------------------------------------------------------------ #
#                             DATA QUAAITY SERVICE                                                 #
# ------------------------------------------------------------------------------------------------ #
class DataQualityAnalysis(Analysis):
    def __init__(self) -> None:
        super().__init__()

        self._asset_id = idgen.get_asset_id(
            asset_type="dataset",
            phase=PhaseDef.DATAPREP,
            stage=DataPrepStageDef.DQA,
            name="review",
        )
        self._data = self._load_data(asset_id=self._asset_id)

    def summarize(self) -> None:
        cols = [col for col in self._data.columns if col.startswith("dqa")]
        dqa = self._data[cols]
        df = dqa.sum(axis=0)
        df = pd.DataFrame(df, columns=["n"])
        df["%"] = round(dqa.sum(axis=0) / self._data.shape[0] * 100, 2)
        return df.sort_values(by="n", ascending=False)

    def get_random_text(self) -> pd.DataFrame:
        """Returns high entropy review text."""
        return self._data[self._data["dqa_entropy"] is True]["content"]

    def get_duplicate_reviews(self) -> pd.DataFrame:
        """Returns high entropy review text."""
        return self._data[self._data["dqa_duplicate_review"] is True]["content"]

    def get_non_english_reviews(self) -> pd.DataFrame:
        """Returns high entropy review text."""
        return self._data.loc[self._data["dqa_non_english_review"]]["content"]

    def get_non_english_apps(self) -> pd.DataFrame:
        """Returns high entropy review text."""
        return self._data.loc[self._data["dqa_non_english_app_name"]]["app_name"]

    def get_emoji(self) -> pd.DataFrame:
        """Returns high entropy review text."""
        return self._data[self._data["dqa_has_emoji"] is True]["content"]

    def get_special_chars(self) -> pd.DataFrame:
        """Returns high entropy review text."""
        return self._data[self._data["dqa_excessive_special_chars"] is True]["content"]

    def get_profanity(self) -> pd.DataFrame:
        """Returns high entropy review text."""
        return self._data[self._data["dqa_has_profanity"] is True]["content"]

    def get_email(self) -> pd.DataFrame:
        """Returns high entropy review text."""
        return self._data[self._data["dqa_contains_email"] is True]["content"]

    def get_url(self) -> pd.DataFrame:
        """Returns high entropy review text."""
        return self._data[self._data["dqa_contains_url"] is True]["content"]

    def get_phone(self) -> pd.DataFrame:
        """Returns high entropy review text."""
        return self._data[self._data["dqa_contains_phone_number"] is True]["content"]

    def _load_data(self, asset_id: str) -> pd.DataFrame:
        return super()._load_data(asset_id=asset_id)
