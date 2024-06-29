#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/data_prep/text_prep.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday May 19th 2024 09:08:01 pm                                                    #
# Modified   : Friday June 28th 2024 07:52:27 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Stage 2: Data Preparation Module"""
import os
import warnings
from dataclasses import dataclass
from typing import Union

import nltk
import pandas as pd
from dotenv import load_dotenv
from nltk.corpus import stopwords
from pyspark.ml import Pipeline, Transformer
from pyspark.sql import DataFrame, SparkSession
from sparknlp.annotator import (
    LemmatizerModel,
    NGramGenerator,
    Normalizer,
    NorvigSweetingApproach,
    StopWordsCleaner,
    Tokenizer,
)
from sparknlp.base import DocumentAssembler, Finisher

from appinsight.data_prep.base import Preprocessor
from appinsight.data_prep.io import ConvertTask, ReadTask, WriteTask
from appinsight.infrastructure.logging import log_exceptions
from appinsight.infrastructure.profiling.decorator import task_profiler
from appinsight.utils.base import Converter, Reader, Writer
from appinsight.utils.convert import ToSpark
from appinsight.utils.io import FileReader, PySparkReader, PySparkWriter
from appinsight.utils.repo import DatasetRepo
from appinsight.workflow.config import StageConfig
from appinsight.workflow.pipeline import Pipeline as Pipe
from appinsight.workflow.task import Task

# ------------------------------------------------------------------------------------------------ #
nltk.download("stopwords")
warnings.filterwarnings("ignore")
load_dotenv()

# ------------------------------------------------------------------------------------------------ #
TRANSFORMED_COLUMNS = [
    "tokenized",
    "normalized",
    "stopwords",
    "spellchecked",
    "output",
    "bigrams",
    "trigrams",
]


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TextPrepConfig(StageConfig):
    name: str = "TextPreprocessor"
    source_directory: str = "04_features/reviews"
    source_filename: str = None
    target_directory: str = "05_nlp/reviews"
    target_filename: str = None
    partition_cols: str = "category"
    force: bool = False
    text_col: str = "content"
    wordlist: str = None

    def __post_init__(self) -> None:
        self.wordlist = os.getenv("REFERENCE_WORDLIST")
        if self.wordlist is None:
            msg = "No wordlist reference filepath designated in the .env file."
            raise RuntimeError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                 TEXT PREPROCESSOR                                                #
# ------------------------------------------------------------------------------------------------ #
class TextPreprocessor(Preprocessor):
    """Encapsulates the text preprocessing pipeline

    Attributes:
        data (pd.DataFrame): The cleaned dataset

    Args:
        config (TextPrepConfig): Configuration for text preprocessor.
        spark (SparkSession): Spark session
        pipeline_cls type[Pipeline]: Pipeline class to instantiate
        dsm_cls (type[DatasetRepo]): Manages dataset IO
        source_reader_cls (type[Reader]): Class for reading the source data.
        target_writer_cls (type[Writer]): Class for writing the target data
        target_reader_cls (type[Reader]): Class for reading the target data.
        converter_cls (type[Converter]): Converts a DataFrame to Pandas or Spark

    """

    def __init__(
        self,
        config: TextPrepConfig,
        spark: SparkSession,
        source_reader_cls: type[Reader] = FileReader,
        target_writer_cls: type[Writer] = PySparkWriter,
        target_reader_cls: type[Reader] = PySparkReader,
        pipeline_cls: type[Pipe] = Pipe,
        dsm_cls: type[DatasetRepo] = DatasetRepo,
        converter_cls: type[Converter] = ToSpark,
    ) -> None:
        """Initializes the DataQualityPipeline with data."""
        super().__init__(
            config=config,
            source_reader_cls=source_reader_cls,
            target_writer_cls=target_writer_cls,
            target_reader_cls=target_reader_cls,
            pipeline_cls=pipeline_cls,
            dsm_cls=dsm_cls,
        )
        self._spark = spark
        self._converter_cls = converter_cls

    def create_pipeline(self) -> Pipeline:
        """Creates the pipeline with all the tasks for data quality analysis.

        Returns:
            Pipeline: The configured pipeline with tasks.
        """
        # Instantiate pipeline
        pipe = self.pipeline_cls(name=self.config.name)

        # Instantiate Tasks
        load = ReadTask(
            directory=self.config.source_directory,
            filename=self.config.source_filename,
            reader_cls=self.source_reader_cls,
        )

        # Convert the pandas DataFrame to a Spark DataFrame
        conv = ConvertTask(converter_cls=self._converter_cls, spark=self._spark)

        # Text processing pipeline
        text = TextPreprocessorTask(
            wordlist=self.config.wordlist, text_col=self.config.text_col
        )

        # Persist the data
        save = WriteTask(
            directory=self.config.target_directory,
            filename=self.config.target_filename,
            writer_cls=self.target_writer_cls,
            partition_cols=self.config.partition_cols,
        )

        # Add tasks to pipeline...
        pipe.add_task(load)
        pipe.add_task(conv)
        pipe.add_task(text)
        pipe.add_task(save)
        return pipe

    def read_endpoint(self) -> Union[pd.DataFrame, DataFrame]:
        """Reads and returns the target data."""
        filepath = self.dsm.get_filepath(
            directory=self.config.target_directory, filename=self.config.target_filename
        )
        try:
            data = self.target_reader_cls(spark=self._spark).read(filepath=filepath)
            msg = (
                f"{self.config.name} endpoint already exists. Returning prior results."
            )
            self.logger.debug(msg)
            return data
        except Exception as e:
            msg = f"Exception occurred while reading endpoint at {filepath}.\n{e}"
            self.logger.exception(msg)
            raise


# ------------------------------------------------------------------------------------------------ #


class TextPreprocessorTask(Task):
    """
    A class to preprocess text data for natural language processing tasks.

    Attributes:
        WORDLIST (str): Path to the text file containing the wordlist for spell checking.
        TEXT_COL (str): The name of the column containing the text data.
    """

    WORDLIST = "data/04_ref/wordlist.txt"
    TEXT_COL = "content"

    def __init__(
        self,
        wordlist: str = WORDLIST,
        text_col: str = TEXT_COL,
    ) -> None:
        """
        Initializes the Preprocess class with a wordlist and column name

        Args:
            wordlist (str): Path to the wordlist file. Defaults to WORDLIST.
            text_col (str): The name of the text column. Defaults to TEXT_COL.
        """
        super().__init__()
        self.WORDLIST = wordlist
        self.TEXT_COL = text_col

        self._pipeline = None

    def document(self) -> Transformer:
        """
        Creates a DocumentAssembler transformer.

        Returns:
            Transformer: The DocumentAssembler transformer.
        """
        return DocumentAssembler().setInputCol(self.TEXT_COL).setOutputCol("document")

    def tokenizer(self) -> Transformer:
        """
        Creates a Tokenizer transformer.

        Returns:
            Transformer: The Tokenizer transformer.
        """
        return Tokenizer().setInputCols(["document"]).setOutputCol("tokenized")

    def normalizer(self) -> Transformer:
        """
        Creates a Normalizer transformer.

        Returns:
            Transformer: The Normalizer transformer.
        """
        return (
            Normalizer()
            .setInputCols(["tokenized"])
            .setOutputCol("normalized")
            .setLowercase(True)
        )

    def stop_words(self) -> Transformer:
        """
        Creates a StopWordsCleaner transformer with English stopwords.

        Returns:
            Transformer: The StopWordsCleaner transformer.
        """
        eng_stopwords = stopwords.words("english")
        return (
            StopWordsCleaner()
            .setInputCols(["normalized"])
            .setOutputCol("stopwords")
            .setStopWords(eng_stopwords)
        )

    def spell_check(self) -> Transformer:
        """
        Creates a NorvigSweetingApproach transformer for spell checking.

        Returns:
            Transformer: The NorvigSweetingApproach transformer.
        """
        return (
            NorvigSweetingApproach()
            .setInputCols(["stopwords"])
            .setOutputCol("spellchecked")
            .setDictionary(self.WORDLIST)
        )

    def lemmatizer(self) -> Transformer:
        """
        Creates a LemmatizerModel transformer.

        Returns:
            Transformer: The LemmatizerModel transformer.
        """
        return (
            LemmatizerModel.pretrained()
            .setInputCols(["spellchecked"])
            .setOutputCol("output")
        )

    def bigrammer(self) -> Transformer:
        """
        Creates an NGramGenerator transformer for bigrams.

        Returns:
            Transformer: The NGramGenerator transformer for bigrams.
        """
        return (
            NGramGenerator()
            .setInputCols(["output"])
            .setOutputCol("bigrams")
            .setN(2)
            .setEnableCumulative(False)
            .setDelimiter("_")
        )

    def trigrammer(self) -> Transformer:
        """
        Creates an NGramGenerator transformer for trigrams.

        Returns:
            Transformer: The NGramGenerator transformer for trigrams.
        """
        return (
            NGramGenerator()
            .setInputCols(["output"])
            .setOutputCol("trigrams")
            .setN(3)
            .setEnableCumulative(False)
            .setDelimiter("_")
        )

    def build_pipeline(self) -> None:
        """
        Builds the preprocessing pipeline with the specified stages.
        """
        stages = [
            self.document(),
            self.tokenizer(),
            self.normalizer(),
            self.stop_words(),
            self.spell_check(),
            self.lemmatizer(),
            self.bigrammer(),
            self.trigrammer(),
        ]
        self._pipeline = Pipeline().setStages(stages)

    def fit_transform(self, data: DataFrame) -> DataFrame:
        """
        Fits the pipeline to the original DataFrame and transforms it.
        """
        finisher = (
            Finisher()
            .setInputCols(TRANSFORMED_COLUMNS)
            .setOutputCols(TRANSFORMED_COLUMNS)
        )
        data = self._pipeline.fit(data).transform(data)
        return finisher.transform(data)

    @task_profiler()
    @log_exceptions()
    def execute_task(self, data: DataFrame) -> DataFrame:
        """
        Executes the text preprocessing pipeline.

        Args:
            data (DataFrame): The data to be transformed.

        """

        self.build_pipeline()
        data = self.fit_transform(data=data)

        return data
