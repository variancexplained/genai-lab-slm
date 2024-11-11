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
# Modified   : Monday November 11th 2024 02:27:14 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Quality Analysis Module"""

from typing import Optional

import pandas as pd

from discover.app.base import Analysis
from discover.assets.idgen import AssetIDGen
from discover.core.flow import DataPrepStageDef, PhaseDef

labels = {
    "dqa_accents": "Contains Accents and Diacritics.",
    "dqa_ctrl_chars": "Contains Control Characters",
    "dqa_duplicate_review_id": "Contains Duplicate Review Id",
    "dqa_elongation": "Contains Elongation",
    "dqa_email": "Contains Email Addresses",
    "dqa_excess_special_chars": "Contains Excessive Special Characters",
    "dqa_excess_whitespace": "Contains Excessive Whitespace",
    "dqa_html_chars": "Contains HTML Characters",
    "dqa_non_english_app_name": "Contains Non-English App Name",
    "dqa_non_english_text": "Contains Non-English Review Text",
    "dqa_phone": "Contains Phone Number(s)",
    "dqa_short_review": "Contains Short Review < 3 Words",
    "dqa_unicode_chars": "Contains Unicode Characters",
    "dqa_url": "Contains URL(s)",
}


# ------------------------------------------------------------------------------------------------ #
#                         DATA QUAAITY ANALYSIS SERVICE                                            #
# ------------------------------------------------------------------------------------------------ #
class DQA(Analysis):
    def __init__(self, name: str = "review") -> None:
        super().__init__()
        # Obtain the dataset asset id
        asset_id = AssetIDGen().get_asset_id(
            asset_type="dataset",
            phase=PhaseDef.DATAPREP,
            stage=DataPrepStageDef.DQA,
            name=name,
        )
        # Load the dataset
        self._df = self.load_data(asset_id=asset_id).content
        print(self._df.info())
        # Extract the columns containing the binary indicators of defects
        self._cols = [col for col in self._df.columns if col.startswith("dqa")]

    def summarize(self) -> None:
        # Extract the dqa data
        dqa = self._df[self._cols]
        # Sum the indicator variables
        df = dqa.sum(axis=0)
        # Create a dataframe of counts
        df = pd.DataFrame(df, columns=["n"])
        # Add relative counts
        df["%"] = round(dqa.sum(axis=0) / self._df.shape[0] * 100, 2)
        # Reset the index and expose the defect column
        df = df.reset_index(names=["Defect"])
        # Convert columns to labels
        df["Defect"] = df["Defect"].apply(self._convert_labels)
        # Select and order columns
        df = df[["Defect", "n", "%"]]
        print(df.head())
        return df.sort_values(by="n", ascending=False).reset_index(drop=True)

    def get_defects(
        self, defect: str, n: int = 10, random_state: int = None
    ) -> pd.DataFrame:
        col = self._get_column_name(substring=defect)
        df = self._df.loc[self._df[col]][[col, "content"]]
        n = min(n, len(df))
        return df.sample(n=n, random_state=random_state)

    def subset_df(self, n: int = 10, random_state: int = None) -> pd.DataFrame:
        n = min(n, len(self._df))
        return self._df.sample(n=n, random_state=random_state)

    def completeness(self) -> float:
        nulls = len(self._df[self._df.isna().any(axis=1)])
        nullness = nulls / self._df.shape[0]
        return 1 - nullness

    def _get_column_name(self, substring) -> Optional[str]:
        return [col for col in self._cols if substring in col][0]

    def _convert_labels(self, txt) -> str:
        """Converts column names to Title case labels."""
        txt = txt.replace("dqa_", "")
        txt = txt.replace("_", " ")
        txt = txt.title()
        txt = "Contains " + txt
        return txt
