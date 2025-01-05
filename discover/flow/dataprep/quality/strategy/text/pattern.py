#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/dataprep/quality/strategy/text/pattern.py                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 01:58:22 am                                             #
# Modified   : Saturday January 4th 2025 08:36:26 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from typing import Callable, Dict


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Regex:
    """
    Represents a regex pattern and its corresponding replacement.

    Attributes:
        pattern (str): The regex pattern to match.
        replacement (str): The string to replace matches with, or None if no replacement is required.
    """

    pattern: str
    replacement: str


# ------------------------------------------------------------------------------------------------ #
class RegexFactory:
    """
    Factory for retrieving pre-defined and dynamically generated regex patterns and replacements.

    Static Patterns:
        - Predefined regex patterns for common tasks (e.g., email, URL, punctuation).
        - Each static pattern is associated with an optional replacement string.

    Dynamic Patterns:
        - Generated at runtime based on user-defined parameters.
        - Includes patterns for elongation, repetition, and sequences.

    Methods:
        get_regex(pattern: str, **kwargs): Retrieves a regex pattern and replacement.
    """

    __STATIC_PATTERNS = {
        # Static patterns for detecting specific structures
        "email": {
            "pattern": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
            "replacement": " ",
        },
        "url": {
            "pattern": r"(https?:\/\/)?(www\.)?[\w\-_]+(\.[\w\-_]+)+([\/\w\-_\.]*)*",
            "replacement": " ",
        },
        "phone": {
            "pattern": r"(\+?\d{1,3})?[\s.-]?\(?\d{2,4}\)?[\s.-]?\d{3,4}[\s.-]?\d{4}",
            "replacement": " ",
        },
        # Linguistic punctuation marks
        "punctuation": {
            "pattern": r"[.,!?\"'()\-:;]",
            "replacement": None,
        },
        # Non-punctuation special characters
        "special_chars": {
            "pattern": r"[^a-zA-Z0-9\s.,!\"''()\\:;-]",
            "replacement": " ",
        },
        # Non-ASCII characters
        "non_ascii": {
            "pattern": r"[^\x00-\x7F]",
            "replacement": None,
        },
        # Control characters
        "control_chars": {
            "pattern": r"[\x00-\x1F\x7F]",
            "replacement": " ",
        },
        # HTML entities
        "html": {
            "pattern": r"&[#A-Za-z0-9]+;",
            "replacement": "",
        },
        # Excessive whitespace
        "whitespace": {
            "pattern": r"\s{2,}",
            "replacement": " ",
        },
        # Accented characters
        "accents": {
            "pattern": r"[\u00C0-\u024F]",
            "replacement": None,
        },
        # New Lines
        "newline": {
            "pattern": r"\n",
            "replacement": " ",
        },
    }

    def __init__(self) -> None:
        """
        Initializes the RegexFactory with dynamic pattern generators.

        Attributes:
            __DYNAMIC_PATTERN_REPLACEMENT (dict): Maps pattern names to callable methods
            for generating dynamic patterns and replacements.
        """
        self.__DYNAMIC_PATTERN_REPLACEMENT: Dict[str, Callable[..., Regex]] = {
            "elongation": self._elongation,
            "character_repetition": self._character_repetition,
            "sequence_repetition": self._sequence_repetition,
            "phrase_repetition": self._phrase_repetition,
            "word_repetition": self._word_repetition,
        }

    def get_regex(self, pattern: str, **kwargs) -> Regex:
        """
        Retrieves a regex pattern and its replacement.

        Args:
            pattern (str): The name of the pattern to retrieve (static or dynamic).
            **kwargs: Additional arguments for dynamic pattern generation.

        Returns:
            Pattern: An object containing the regex pattern and its replacement.

        Raises:
            ValueError: If the requested pattern is not supported.
        """
        if pattern in self.__STATIC_PATTERNS.keys():
            return Regex(**self.__STATIC_PATTERNS[pattern])

        elif pattern in self.__DYNAMIC_PATTERN_REPLACEMENT.keys():
            return self.__DYNAMIC_PATTERN_REPLACEMENT[pattern](**kwargs)

        else:
            available_patterns = list(self.__STATIC_PATTERNS.keys()) + list(
                self.__DYNAMIC_PATTERN_REPLACEMENT.keys()
            )
            raise ValueError(
                f"Pattern '{pattern}' is not supported. Available patterns: {available_patterns}"
            )

    # ------------------------------------------------------------------------------------------------ #
    # Dynamic pattern generators
    # ------------------------------------------------------------------------------------------------ #

    def _elongation(
        self, threshold: int = 4, max_elongation: int = 3, **kwargs
    ) -> Regex:
        """
        Generates a pattern for detecting elongated characters.

        Args:
            threshold (int): Minimum number of consecutive repeated characters to consider elongation.
            max_elongation (int): Maximum number of consecutive repeated characters to normalize to.

        Returns:
            Pattern: Regex pattern and replacement logic.
        """
        pattern = rf"(.)\1{{{threshold - 1},}}"
        replacement = r"\1" * max_elongation
        return Regex(pattern=pattern, replacement=replacement)

    def _character_repetition(self, min_repetitions: int = 4, **kwargs) -> Regex:
        """
        Generates a pattern for detecting repeated sequences of characters.

        Args:
            min_repetitions (int): Minimum number of repetitions to consider a match.

        Returns:
            Pattern: Regex pattern and replacement logic.
        """
        pattern = f"(.+?)\\1{{{min_repetitions - 1}}}"
        replacement = r"\1"
        return Regex(pattern=pattern, replacement=replacement)

    def _sequence_repetition(
        self, length_of_sequence: int = 3, min_repetitions: int = 3, **kwargs
    ) -> Regex:
        """
        Generates a pattern for detecting repeated sequences of characters.

        Args:
            length_of_sequence (int): Minimum length of the repeating sequence.
            min_repetitions (int): Minimum number of repetitions to consider a match.

        Returns:
            Pattern: Regex pattern and replacement logic.
        """
        pattern = f"(.{{{length_of_sequence},}})\\1{{{min_repetitions - 1}}}"
        replacement = r"\1"
        return Regex(pattern=pattern, replacement=replacement)

    def _phrase_repetition(self, min_repetitions: int = 3, **kwargs) -> Regex:
        """
        Generates a pattern for detecting repeated phrases.

        Args:
            min_repetitions (int): Minimum number of phrase repetitions to consider a match.

        Returns:
            Pattern: Regex pattern and replacement logic.
        """
        pattern = rf"(\b\w+\b(?: \b\w+\b)*)\s*(?:\1\s*){{{min_repetitions - 1},}}"
        replacement = r"\1"
        return Regex(pattern=pattern, replacement=replacement)

    def _word_repetition(self, min_repetitions: int = 3, **kwargs) -> Regex:
        """
        Generates a pattern for detecting repeated words.

        Args:
            min_repetitions (int): Minimum number of word repetitions to consider a match.

        Returns:
            Pattern: Regex pattern and replacement logic.
        """
        pattern = rf"(\b\w+\b)\s*(?:\1\s*){{{min_repetitions - 1},}}"
        replacement = r"\1"
        return Regex(pattern=pattern, replacement=replacement)
