#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/analytics/summary.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday January 16th 2025 03:37:17 pm                                              #
# Modified   : Monday January 20th 2025 02:42:34 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""DataFrame Summary Module"""

from decimal import ROUND_HALF_UP, Decimal
from typing import Type

import numpy as np
import pandas as pd
from pandarallel import pandarallel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, countDistinct, length, max, mean, min

from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
printer = Printer()
pandarallel.initialize(progress_bar=False, nb_workers=18, verbose=0)


# ------------------------------------------------------------------------------------------------ #
class DatasetSummarizer:
    def __init__(self, printer_cls: Type[Printer] = Printer) -> None:
        self._printer = printer_cls()

    def summarize_pandas(self, df: pd.DataFrame) -> None:
        """Prints a summary of the app review dataset.

        The summary includes:
        - Number of reviews, authors, apps, and categories.
        - Proportion of influential and repeat reviewers.
        - Average review length and reviews per app.
        - Memory usage and date range of the reviews.
        """
        n = df.shape[0]
        p = df.shape[1]
        n_auth = df["author"].nunique()
        n_auth_inf = df.loc[df["vote_count"] > 0]["author"].nunique()
        p_auth_inf = round(n_auth_inf / n_auth * 100, 2)
        n_repeat_auth = int((df["author"].value_counts() > 1).sum())
        p_repeat_auth = round(n_repeat_auth / n_auth * 100, 2)
        n_apps = df["app_id"].nunique()
        n_categories = df["category"].nunique()
        ave_reviews_per_app = round(n / n_apps, 2)

        review_lengths = df["content"].parallel_apply(lambda n: len(n.split()))
        min_review_length = min(review_lengths)
        max_review_length = max(review_lengths)
        avg_review_length = np.mean(review_lengths)

        mem = df.memory_usage(deep=True).sum().sum()
        dt_first = df["date"].min()
        dt_last = df["date"].max()
        d = {
            "Number of Reviews": n,
            "Number of Reviewers": n_auth,
            "Number of Repeat Reviewers": f"{n_repeat_auth:,} ({p_repeat_auth:.1f}%)",
            "Number of Influential Reviewers": f"{n_auth_inf:,} ({p_auth_inf:.1f}%)",
            "Number of Apps": n_apps,
            "Average Reviews per App": f"{ave_reviews_per_app:.1f}",
            "Number of Categories": n_categories,
            "Features": p,
            "Min Review Length": min_review_length,
            "Max Review Length": max_review_length,
            "Average Review Length": round(avg_review_length, 2),
            "Memory Size (Mb)": round(mem / (1024 * 1024), 2),
            "Date of First Review": dt_first,
            "Date of Last Review": dt_last,
        }
        title = "AppVoCAI Dataset Summary"
        self._printer.print_dict(title=title, data=d)

    def summarize_spark(self, df: DataFrame) -> None:
        """Prints a summary of the app review dataset.

        The summary includes:
        - Number of reviews, authors, apps, and categories.
        - Proportion of influential and repeat reviewers.
        - Average review length and reviews per app.
        - Date range of the reviews.
        """
        # Count total number of reviews
        n = df.count()

        # Number of distinct authors
        n_auth = df.select(countDistinct("author")).collect()[0][0]

        # Number of influential reviewers (with vote_count > 0)
        n_auth_inf = (
            df.filter(col("vote_count") > 0)
            .select(countDistinct("author"))
            .collect()[0][0]
        )

        # Proportion of influential reviewers
        p_auth_inf = round(n_auth_inf / n_auth * 100, 2) if n_auth > 0 else 0

        # Number of repeat reviewers (authors with more than one review)
        repeat_auth_df = df.groupBy("author").count().filter(col("count") > 1)
        n_repeat_auth = repeat_auth_df.count()
        p_repeat_auth = round(n_repeat_auth / n_auth * 100, 2) if n_auth > 0 else 0

        # Number of distinct apps and categories
        n_apps = df.select(countDistinct("app_id")).collect()[0][0]
        n_categories = df.select(countDistinct("category")).collect()[0][0]

        # Average reviews per app
        ave_reviews_per_app = round(n / n_apps, 2) if n_apps > 0 else 0

        # Review Length
        df = df.withColumn("review_length", length(col("content")))
        # Extract min, max and average review lengths
        min_review_length = df.select(min("review_length")).collect()[0][0]
        max_review_length = df.select(max("review_length")).collect()[0][0]
        avg_review_length = df.select(mean("review_length")).collect()[0][0]

        # Round average review length
        if avg_review_length is not None:
            avg_review_length = Decimal(avg_review_length).quantize(
                Decimal("0.00"), rounding=ROUND_HALF_UP
            )

        # Date range of reviews
        dt_first = df.select(min("date")).collect()[0][0]
        dt_last = df.select(max("date")).collect()[0][0]

        # Summary data dictionary
        d = {
            "Number of Reviews": n,
            "Number of Reviewers": n_auth,
            "Number of Repeat Reviewers": f"{n_repeat_auth:,} ({p_repeat_auth:.1f}%)",
            "Number of Influential Reviewers": f"{n_auth_inf:,} ({p_auth_inf:.1f}%)",
            "Number of Apps": n_apps,
            "Average Reviews per App": f"{ave_reviews_per_app:.1f}",
            "Number of Categories": n_categories,
            "Min Review Length": min_review_length,
            "Max Review Length": max_review_length,
            "Average Review Length": (
                float(avg_review_length) if avg_review_length is not None else None
            ),
            "Date of First Review": dt_first,
            "Date of Last Review": dt_last,
        }

        # Print summary
        title = "AppVoCAI Dataset Summary"
        self._printer.print_dict(title=title, data=d)
