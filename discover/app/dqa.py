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
# Modified   : Tuesday October 29th 2024 07:47:34 pm                                               #
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

    def get_duplicate_review_ids(self, n=20, random_state: int = None) -> pd.DataFrame:
        grouped = self._data.loc[self._data["dqa_identical_review_id"]].groupby(by="id")
        return [group for name, group in grouped][:n]

    def get_duplicate_review_content(self) -> pd.DataFrame:
        # Obtain the data
        cols = [col for col in self._data.columns if not col.startswith("dqa")]

        grouped = self._data.loc[
            self._data["dqa_identical_review_content"], cols
        ].groupby(by="content")

        # Summarize the data
        summary = (
            grouped.size()
            .reset_index(name="count")
            .sort_values(by="count", ascending=False)
        )
        return summary, grouped

    def get_missing_reviews(
        self, n: int = 20, random_state: int = None
    ) -> pd.DataFrame:
        df = self._data.loc[self._data["dqa_review_missing"]]
        return self._subset_data(df=df, n=n, random_state=random_state)

    def get_non_english_reviews(self, n=20, random_state: int = None) -> pd.DataFrame:
        df = self._data.loc[self._data["dqa_non_english_review"], "content"]
        return self._subset_data(df=df, n=n, random_state=random_state)

    def get_non_english_apps(self, n=20, random_state: int = None) -> pd.DataFrame:
        df = self._data.loc[self._data["dqa_non_english_app_name"], "app_name"]
        return self._subset_data(df=df, n=n, random_state=random_state)

    def get_emoji(self, n=20, random_state: int = None) -> pd.DataFrame:
        df = self._data.loc[self._data["dqa_has_emoji"], "content"]
        return self._subset_data(df=df, n=n, random_state=random_state)

    def get_email(self, n=20, random_state: int = None) -> pd.DataFrame:
        df = self._data.loc[self._data["dqa_contains_email"], "content"]
        return self._subset_data(df=df, n=n, random_state=random_state)

    def get_url(self, n=20, random_state: int = None) -> pd.DataFrame:
        df = self._data.loc[self._data["dqa_contains_url"], "content"]
        return self._subset_data(df=df, n=n, random_state=random_state)

    def get_phone(self, n=20, random_state: int = None) -> pd.DataFrame:
        df = self._data.loc[self._data["dqa_contains_phone_number"], "content"]
        return self._subset_data(df=df, n=n, random_state=random_state)

    def get_non_ascii_chars(self, n=20, random_state: int = None) -> pd.DataFrame:
        df = self._data.loc[self._data["dqa_contains_non_ascii_chars"], "content"]
        return self._subset_data(df=df, n=n, random_state=random_state)

    def get_excessive_numbers(self, n=20, random_state: int = None) -> pd.DataFrame:
        df = self._data.loc[self._data["dqa_contains_excessive_numbers"], "content"]
        return self._subset_data(df=df, n=n, random_state=random_state)

    def get_control_chars(self, n=20, random_state: int = None) -> pd.DataFrame:
        df = self._data.loc[self._data["dqa_contains_control_chars"], "content"]
        return self._subset_data(df=df, n=n, random_state=random_state)

    def get_excessive_whitespace(self, n=20, random_state: int = None) -> pd.DataFrame:
        df = self._data.loc[self._data["dqa_contains_excessive_whitespace"], "content"]
        return self._subset_data(df=df, n=n, random_state=random_state)

    def get_html_chars(self, n=20, random_state: int = None) -> pd.DataFrame:
        df = self._data.loc[self._data["dqa_contains_HTML_chars"], "content"]
        return self._subset_data(df=df, n=n, random_state=random_state)

    def get_inconsistent_app_id_name(
        self, n=20, random_state: int = None
    ) -> pd.DataFrame:
        df = self._data.loc[self._data["dqa_contains_inconsistent_app_id_name"]]
        return self._subset_data(df=df, n=n, random_state=random_state)

    def _load_data(self, asset_id: str) -> pd.DataFrame:
        return super()._load_data(asset_id=asset_id)

    def _subset_data(self, df: pd.DataFrame, n: int, random_state: int) -> pd.DataFrame:
        n = min(n, len(df))
        return df.sample(n=n, random_state=random_state)
