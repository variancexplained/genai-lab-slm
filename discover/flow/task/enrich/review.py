#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/enrich/review.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 01:47:02 pm                                             #
# Modified   : Thursday December 19th 2024 01:40:46 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Review Enrichment Module"""
import pandas as pd
from pandarallel import pandarallel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

from discover.flow.task.enrich.base import EnrichmentTask
from discover.infra.service.logging.task import task_logger

# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(progress_bar=False, nb_workers=18, verbose=0)


# ------------------------------------------------------------------------------------------------ #
#                                COMPUTE REVIEW LENGTH                                             #
# ------------------------------------------------------------------------------------------------ #
class ComputeReviewLength(EnrichmentTask):
    """
    Task for adding a new column to a Pandas DataFrame that contains the word count of a specified text column.

    This task calculates the word count for each entry in the specified text column and adds it as a new column
    in the DataFrame. It uses parallel processing to optimize the computation for large datasets.

    Args:
        column (str): The name of the column in the DataFrame containing the text to analyze. Defaults to "content".
        new_column (str): The name of the new column to store the word count. Defaults to "review_length".

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Calculates the word count for each entry in the specified column and adds it to a new column in the DataFrame.
    """

    def __init__(
        self, column: str = "content", new_column: str = "review_length", **kwargs
    ) -> None:
        super().__init__(column=column, new_column=new_column, **kwargs)
        self._new_column = (
            new_column  # We override review_length as it shouldn't be prefixed.
        )

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates the word count for each entry in the specified text column and adds it as a new column.

        This method splits the text in the specified column by whitespace and counts the number of words
        in each entry. The results are stored in a new column in the DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame containing the text data.

        Returns:
            pd.DataFrame: The DataFrame with the new column added containing word counts.
        """

        data[self._new_column] = data[self._column].parallel_apply(
            lambda x: len(x.split())
        )
        return data


# ------------------------------------------------------------------------------------------------ #
#                                COMPUTE REVIEW LENGTH (PYSPARK)                                   #
# ------------------------------------------------------------------------------------------------ #


class ComputeReviewLengthPS(EnrichmentTask):
    """
    Task for adding a new column to a pyspark DataFrame that contains the word count of a specified text column.

    This task calculates the word count for each entry in the specified text column and adds it as a new column
    in the DataFrame. It uses parallel processing to optimize the computation for large datasets.

    Args:
        column (str): The name of the column in the DataFrame containing the text to analyze. Defaults to "content".
        new_column (str): The name of the new column to store the word count. Defaults to "review_length".

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Calculates the word count for each entry in the specified column and adds it to a new column in the DataFrame.
    """

    def __init__(
        self, column: str = "content", new_column: str = "review_length", **kwargs
    ) -> None:
        super().__init__(column=column, new_column=new_column, **kwargs)
        self._new_column = (
            new_column  # We override review_length as it shouldn't be prefixed.
        )

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates the word count for each entry in the specified text column and adds it as a new column.

        This method splits the text in the specified column by whitespace and counts the number of words
        in each entry. The results are stored in a new column in the DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame containing the text data.

        Returns:
            pd.DataFrame: The DataFrame with the new column added containing word counts.
        """

        # Define a UDF (User-Defined Function) to calculate the length of split strings
        def count_words(text):
            return len(text.split())

        # Register the UDF
        count_words_udf = udf(count_words, IntegerType())

        # Apply UDF to dataframe.
        data = data.withColumn(self._new_column, count_words_udf(col(self._column)))

        # Return the updated DataFrame
        return data


# ------------------------------------------------------------------------------------------------ #
#                                    COMPUTE REVIEW AGE                                            #
# ------------------------------------------------------------------------------------------------ #
class ComputeReviewAgeTask(EnrichmentTask):
    """
    A task to compute the "review age" of each entry in a specified date column of a PySpark DataFrame.

    The review age is calculated as the difference in days between the latest date in the column and each
    individual date, providing a measure of how old each review is relative to the most recent one.

    Attributes:
        column (str): The name of the date column to calculate review age from. Defaults to "date".
        new_column (str): The name of the new column to store the review age. Default is 'review_age'.

    Methods:
        run(data: DataFrame) -> DataFrame:
            Calculates the review age for each row in the specified date column and returns the DataFrame
            with the new "review_age" column.
    """

    def __init__(
        self, column: str = "date", new_column: str = "review_age", **kwargs
    ) -> None:
        super().__init__(column=column, new_column=new_column, **kwargs)

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Executes the review age calculation on the specified date column.

        The function first identifies the maximum date within the column and then calculates the number of days
        between each review date and this maximum date, storing the result in a new "review_age" column.

        Args:
            data (DataFrame): The input PySpark DataFrame containing the specified date column.

        Returns:
            DataFrame: The input DataFrame with an additional "review_age" column representing the
            review age in days.
        """
        # Step 1: Find the maximum date in the specified column
        max_date = data.agg(F.max(self._column)).collect()[0][0]

        # Step 2: Calculate the "review age" as the difference between max_date and each row's date
        data = data.withColumn(
            self._new_column, F.datediff(F.lit(max_date), F.col(self._column))
        )

        return data


# ------------------------------------------------------------------------------------------------ #
#                                     REVIEW MONTH                                                 #
# ------------------------------------------------------------------------------------------------ #
class ComputeReviewMonthTask(EnrichmentTask):
    """
    Task to compute the month from a review date and add it as a new column in the DataFrame.

    This task extracts the month from the specified date column using PySpark's
    built-in `month` function and appends the result as a new column.

    Attributes:
        column (str): The name of the column containing date information. Defaults to "date".
        new_column (str): The name of the new column to be created with the extracted month.
            Defaults to "review_month".
    """

    def __init__(
        self, column: str = "date", new_column: str = "review_month", **kwargs
    ) -> None:
        super().__init__(column=column, new_column=new_column, **kwargs)

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Runs the task to compute the review month and add it to the DataFrame.

        Args:
            data (DataFrame): The input PySpark DataFrame containing a date column.

        Returns:
            DataFrame: The DataFrame with the added column for the review month.
        """
        # Use PySpark's `withColumn` to compute the month from the date column
        data = data.withColumn(self._new_column, F.month(self._column))
        return data


# ------------------------------------------------------------------------------------------------ #
#                                 REVIEW DAY OF WEEK                                               #
# ------------------------------------------------------------------------------------------------ #
class ComputeReviewDayofWeekTask(EnrichmentTask):
    """
    Task to compute the day of the week from a review date and add it as a new column in the DataFrame.

    This task extracts the day of the week from the specified date column using PySpark's
    built-in `dayofweek` function and appends the result as a new column. The day of the week
    is represented as an integer, where 1 corresponds to Monday and 7 to Sunday.

    Attributes:
        column (str): The name of the column containing date information. Defaults to "date".
        new_column (str): The name of the new column to be created with the extracted day of the week.
            Defaults to "review_day_of_week".
    """

    def __init__(
        self, column: str = "date", new_column: str = "review_day_of_week", **kwargs
    ) -> None:
        super().__init__(column=column, new_column=new_column, **kwargs)

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Runs the task to compute the review day of the week and add it to the DataFrame.

        Args:
            data (DataFrame): The input PySpark DataFrame containing a date column.

        Returns:
            DataFrame: The DataFrame with the added column for the day of the week.
        """
        # Extract day of the week from the date
        # The day of the week is returned as an integer (1 = Monday, 7 = Sunday)
        data = data.withColumn(self._new_column, F.dayofweek(self._column))
        return data


# ------------------------------------------------------------------------------------------------ #
#                                 REVIEW HOUR OF DAY                                               #
# ------------------------------------------------------------------------------------------------ #
class ComputeReviewHourTask(EnrichmentTask):
    """
    Task to compute the hour from a review date and add it as a new column in the DataFrame.

    This task extracts the hour from the specified date column using PySpark's
    built-in `hour` function and appends the result as a new column. The hour is
    represented as an integer from 0 to 23, indicating the hour of the day.

    Attributes:
        column (str): The name of the column containing date information. Defaults to "date".
        new_column (str): The name of the new column to be created with the extracted hour.
            Defaults to "review_hour".
    """

    def __init__(
        self, column: str = "date", new_column: str = "review_hour", **kwargs
    ) -> None:
        super().__init__(column=column, new_column=new_column, **kwargs)

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Runs the task to compute the review hour and add it to the DataFrame.

        Args:
            data (DataFrame): The input PySpark DataFrame containing a date column.

        Returns:
            DataFrame: The DataFrame with the added column for the hour of the day.
        """
        # Extract hour of the day from the date
        data = data.withColumn(self._new_column, F.hour(self._column))
        return data


# ------------------------------------------------------------------------------------------------ #
#                               COMPUTE PCT DEVIATION                                              #
# ------------------------------------------------------------------------------------------------ #
class ComputePercentDeviationTask(EnrichmentTask):
    """
    A task to compute the percent deviation of a specified column from the average
    grouped by a given level and store the result in a new column.

    Attributes:
        column (str): The name of the column for which to compute percent deviation.
        by (str): The level by which the average is computed for the deviation calculation.
    """

    def __init__(self, column: str, by: str = "category", **kwargs) -> None:
        new_column = f"{column}_deviation_from_average"
        super().__init__(column=column, new_column=new_column, **kwargs)
        self._by = by

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Computes the percent deviation of the specified column from the average
        grouped by the 'by' variable and adds it as a new column to the DataFrame.

        Args:
            data (DataFrame): The input PySpark DataFrame containing the data.

        Returns:
            DataFrame: The PySpark DataFrame with the new column containing percent deviation.
        """
        # Calculate the average of the specified column, grouped by the 'by' variable
        avg_values = data.groupBy(self._by).agg(F.avg(self._column).alias("avg_value"))

        # Join the average values back to the original DataFrame
        data = data.join(avg_values, on=self._by, how="left")

        # Compute the percent deviation and add it as a new column
        data = data.withColumn(
            self._new_column,
            ((F.col(self._column) - F.col("avg_value")) / F.col("avg_value")) * 100,
        )

        # Drop the temporary 'avg_value' column
        data = data.drop("avg_value")

        return data
