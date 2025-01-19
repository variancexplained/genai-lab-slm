#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/feature/tqa/syntactic/stage.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday January 19th 2025 11:26:44 am                                                #
# Modified   : Sunday January 19th 2025 05:24:43 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #


from typing import List, Optional

from pyspark.sql import SparkSession
from sparknlp.annotator import (
    Chunker,
    DocumentAssembler,
    PerceptronModel,
    SentenceDetector,
    Tokenizer,
)
from sparknlp.base import Pipeline

from discover.asset.dataset.builder import DatasetBuilder
from discover.asset.dataset.dataset import Dataset
from discover.asset.dataset.identity import DatasetConfig
from discover.core.dtypes import DFType
from discover.core.flow import PhaseDef, StageDef
from discover.flow.base.stage import Stage
from discover.flow.base.task import Task
from discover.infra.persist.object.flowstate import FlowState
from discover.infra.persist.repo.dataset import DatasetRepo


# ------------------------------------------------------------------------------------------------ #
class TQASyntacticStage(Stage):
    """A stage for generating syntactic text quality measures in a data processing pipeline.

    This class processes datasets using an NLP pipeline to extract syntactic features
    and applies specified tasks to enhance or transform the data.

    Attributes:
        source_config (DatasetConfig): Configuration for the source dataset.
        target_config (DatasetConfig): Configuration for the target dataset.
        tasks (List[Task]): A list of tasks to execute during the stage.
        state (FlowState): The state of the data processing flow.
        repo (DatasetRepo): Repository for accessing dataset versions.
        dataset_builder (DatasetBuilder): Builder for constructing datasets.
        spark (Optional[SparkSession]): Spark session for distributed data processing, if required.

    Properties:
        phase (PhaseDef): The current phase of the pipeline.
        stage (StageDef): The specific stage within the phase.
        dftype (DFType): The type of data frame processed in this stage.
        nlp_pipeline (Pipeline): The NLP pipeline used for syntactic analysis.

    Methods:
        _run() -> Dataset:
            Executes the stage by applying the NLP pipeline and tasks to the source dataset.
            Returns the processed dataset.

        _get_nlp_pipeline() -> Pipeline:
            Constructs and returns the NLP pipeline for syntactic analysis.
    """

    __PHASE = PhaseDef.FEATURE
    __STAGE = StageDef.TQA_SYNTACTIC
    __DFTYPE = DFType.SPARKNLP

    def __init__(
        self,
        source_config: DatasetConfig,
        target_config: DatasetConfig,
        tasks: List[Task],
        state: FlowState,
        repo: DatasetRepo,
        dataset_builder: DatasetBuilder,
        spark: Optional[SparkSession] = None,
    ) -> None:
        super().__init__(
            source_config=source_config,
            target_config=target_config,
            tasks=tasks,
            state=state,
            repo=repo,
            dataset_builder=dataset_builder,
            spark=spark,
        )
        self._nlp_pipeline = None

    @property
    def phase(self) -> PhaseDef:
        """Returns the phase of the pipeline."""
        return self.__PHASE

    @property
    def stage(self) -> StageDef:
        """Returns the stage of the pipeline."""
        return self.__STAGE

    @property
    def dftype(self) -> DFType:
        """Returns the data frame type used in this stage."""
        return self.__DFTYPE

    @property
    def nlp_pipeline(self) -> Pipeline:
        """Lazily initializes and returns the NLP pipeline for syntactic analysis."""
        if not self._nlp_pipeline:
            self._nlp_pipeline = self._get_nlp_pipeline()
        return self._nlp_pipeline

    def _run(self) -> Dataset:
        """Processes the source dataset using the NLP pipeline and tasks.

        Returns:
            Dataset: The processed target dataset.
        """
        source = self.get_source_dataset()
        dataframe = source.dataframe
        data = self.nlp_pipeline.fit(dataframe).transform(dataframe)
        for task in self._tasks:
            try:
                data = task.run(data)
            except Exception as e:
                msg = f"Error in task {task.__class__.__name__}: {e}"
                self._logger.error(msg)
                raise RuntimeError(msg)

        self._target = self.save_target_dataset(source=source, dataframe=data)
        return self._target

    def _get_nlp_pipeline(self) -> Pipeline:
        """Constructs and returns the NLP pipeline for syntactic analysis.

        The pipeline includes document assembly, sentence detection, tokenization,
        POS tagging, and dependency parsing.

        Returns:
            Pipeline: The constructed NLP pipeline.
        """
        documentAssembler = (
            DocumentAssembler().setInputCol("content").setOutputCol("tqa_document")
        )

        sentenceDetector = (
            SentenceDetector()
            .setInputCols(["tqa_document"])
            .setOutputCol("tqa_sentence")
        )

        tokenizer = Tokenizer().setInputCols(["tqa_sentence"]).setOutputCol("tqa_token")

        posTagger = (
            PerceptronModel.pretrained()
            .setInputCols(
                [
                    "tqa_sentence",
                    "tqa_token",
                ]
            )
            .setOutputCol("tqa_pos")
        )

        noun_phrase_chunker = (
            Chunker()
            .setInputCols(["tqa_sentence", "tqa_pos"])
            .setOutputCol("tqa_syntactic_noun_phrases")
            .setRegexParsers(["<DT>?<JJ>*<NN.*>"])
        )
        adj_noun_chunker = (
            Chunker()
            .setInputCols(["tqa_sentence", "tqa_pos"])
            .setOutputCol("tqa_syntactic_adjective_noun_pairs")
            .setRegexParsers(["<JJ><NN.*>"])
        )
        aspect_verb_chunker = (
            Chunker()
            .setInputCols(["tqa_sentence", "tqa_pos"])
            .setOutputCol("tqa_syntactic_aspect_verb_pairs")
            .setRegexParsers(["<VB(D|G|N|P|Z)?> +<NN.*>|<NN.*> +<VB(D|G|N|P|Z)?>"])
        )
        adverb_phrase_chunker = (
            Chunker()
            .setInputCols(["tqa_sentence", "tqa_pos"])
            .setOutputCol("tqa_syntactic_adverb_phrases")
            .setRegexParsers(["<RB.*>+"])
        )

        nlp_pipeline = Pipeline(
            stages=[
                documentAssembler,
                sentenceDetector,
                tokenizer,
                posTagger,
                noun_phrase_chunker,
                adj_noun_chunker,
                aspect_verb_chunker,
                adverb_phrase_chunker,
            ]
        )
        return nlp_pipeline
