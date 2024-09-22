#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/element/base/build.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 21st 2024 10:21:05 pm                                            #
# Modified   : Sunday September 22nd 2024 01:34:48 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base Module for the Element Dimension"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from discover.core.invariants.flow import PhaseDef, StageDef
from discover.element.base.define import Element


# ------------------------------------------------------------------------------------------------ #
class ElementBuilder(ABC):
    """
    Abstract base class for building an Element instance.

    The builder provides methods for configuring various properties of an Element
    before constructing it via the build() method.
    """

    @abstractmethod
    def phase(self, phase: PhaseDef) -> ElementBuilder:
        """Sets the phase in which the element is created.

        Args:
            phase (PhaseDef): The phase associated with the element.

        Returns:
            ElementBuilder: The builder instance, for chaining.
        """

    @abstractmethod
    def stage(self, stage: StageDef) -> ElementBuilder:
        """Sets the stage in which the element is created.

        Args:
            stage (StageDef): The stage associated with the element.

        Returns:
            ElementBuilder: The builder instance, for chaining.
        """

    @abstractmethod
    def content(self, content: Any) -> ElementBuilder:
        """Sets the content of the element.

        Args:
            content (Any): The data or content to be associated with the element.

        Returns:
            ElementBuilder: The builder instance, for chaining.
        """

    @abstractmethod
    def build(self) -> Element:
        """Constructs and returns the requested element.

        Returns:
            Element: The constructed element instance.
        """
