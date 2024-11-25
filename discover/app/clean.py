#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/app/clean.py                                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday November 24th 2024 07:58:45 pm                                               #
# Modified   : Sunday November 24th 2024 09:10:09 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #


from dependency_injector.wiring import Provide, inject
from explorify.eda.visualize.visualizer import Visualizer

from discover.app.dqa import DQA
from discover.assets.idgen import AssetIDGen
from discover.container import DiscoverContainer
from discover.core.flow import PhaseDef, StageDef
from discover.infra.persistence.repo.dataset import DatasetRepo
from discover.infra.utils.data.compare import compare_dataframes
from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
viz = Visualizer()


# ------------------------------------------------------------------------------------------------ #
#                         DATA CLEANING ANALYSIS SERVICE                                            #
# ------------------------------------------------------------------------------------------------ #
class DataCleaningAnalysis(DQA):
    @inject
    def __init__(
        self, repo: DatasetRepo = Provide[DiscoverContainer.repo.dataset_repo]
    ) -> None:
        source_asset_id = AssetIDGen.get_asset_id(
            asset_type="dataset",
            phase=PhaseDef.DATAPREP,
            stage=StageDef.DQD,
            name="review",
        )
        clean_asset_id = AssetIDGen.get_asset_id(
            asset_type="dataset",
            phase=PhaseDef.DATAPREP,
            stage=StageDef.CLEAN,
            name="review",
        )
        self._source = repo.get(asset_id=source_asset_id, distributed=False, nlp=False)
        self._clean = repo.get(asset_id=clean_asset_id, distributed=False, nlp=False)
        super().__init__(df=self._clean.content)

    def compare(self) -> None:
        comparison = compare_dataframes(
            self_df=self._source.content, other_df=self._clean.content
        )
        Printer().print_dict(
            title="Data Cleaning Dataset Comparison", data=comparison.as_dict()
        )
