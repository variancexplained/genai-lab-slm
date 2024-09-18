#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/domain/service/sentiment_analysis/config.py                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 7th 2024 02:26:49 am                                                    #
# Modified   : Tuesday September 10th 2024 07:36:53 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from collections import defaultdict
from dataclasses import dataclass, field

from peft import PeftType, TaskType

from discover.utils.data import DataClass


# ------------------------------------------------------------------------------------------------ #
@dataclass
class SentimentClassifierTrainingConfig(DataClass):
    """Encapsulates the configuration for a HuggingFace model trainer"""

    output_dir: str = None
    adam_beta1: float = 0.9
    adam_beta2: float = 0.999
    adam_epsilon: float = 1e-8
    auto_find_batch_size: bool = True
    disable_tqdm: bool = False
    do_train: bool = True
    do_eval: bool = True
    evaluation_strategy: str = "epoch"
    greater_is_better: bool = True
    hub_always_push: bool = True
    hub_model_id: str = None
    hub_private_repo: bool = False
    hub_strategy: str = "end"
    learning_rate: float = 1e-3
    load_best_model_at_end: bool = True
    lr_scheduler_kwargs: defaultdict[dict] = field(
        default_factory=lambda: defaultdict(dict)
    )
    lr_scheduler_type: str = "linear"
    metric_for_best_model: str = "eval_accuracy"
    num_train_epochs: int = 3
    optim: str = "adamw_torch"
    overwrite_output_dir: bool = False
    per_device_eval_batch_size: int = 32
    per_device_train_batch_size: int = 32
    prediction_loss_only: bool = False
    push_to_hub: bool = False
    remove_unused_columns: bool = False
    report_to: str = "wandb"
    resume_from_checkpoint: str = None
    run_name: str = None
    save_only_model: bool = False
    save_strategy: str = "epoch"
    save_total_limit: int = 5
    seed: int = 55
    tpu_num_cores: int = 8
    use_cpu: bool = True
    weight_decay: float = 0.01


# ------------------------------------------------------------------------------------------------ #
@dataclass
class SentimentClassifierFineTuningConfig(DataClass):
    """Encapsulates the configuration for fine tuning a model."""

    peft_type: str = PeftType.LORA
    task_type: str = TaskType.SEQ_CLS
    r: int = 8
    inference_mode: bool = False
    lora_alpha: int = 32
    lora_dropout: float = 0.1


# ------------------------------------------------------------------------------------------------ #
@dataclass
class SentimentClassifierDataManagerConfig(DataClass):
    dataset_size: float = 0.0005
    batch_size: float = 0.1
    train_size: float = 0.8
    shuffle_data: bool = True
    max_length: int = 128
    review_col: str = "content"
    random_state: int = None
