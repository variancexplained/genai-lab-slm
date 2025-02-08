#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/quality/strategy/text/pattern.py                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 01:58:22 am                                             #
# Modified   : Saturday February 8th 2025 01:38:53 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import re
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

    def __post_init__(self) -> None:
        self.pattern = re.compile(self.pattern)


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
            "replacement": "EMAIL",
        },
        "url": {
            "pattern":  r"(https?:\/\/)?(?:www\.)?([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,6}(\/[^\s]*)?(\?[^=\s]+=[^\s&]+(&[^=\s]+=[^\s&]+)*)?",
            "replacement": "URL",
        },
        "phone": {
            "pattern": r"(\+?\d{1,3})?[\s.-]?\(?\d{2,4}\)?[\s.-]?\d{3,4}[\s.-]?\d{4}",
            "replacement": " PHONE",
        },
        # Non-Punctuation Special Characters
        "special_chars": {
            "pattern": r"[^a-zA-Z0-9\s.,!\"''()\\:;-]",
            "replacement": None,
        },
        # Special chars to remove
        "special_chars_to_remove": {
            "pattern": r'["\'#%&@*+\\/^~`_|]',
            "replacement": "",
        },
        # Linguistic punctuation marks
        "punctuation": {
            "pattern": r"(\.|\!|\?){2,}",
            "replacement": r"\1",
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
            "pattern": r"[ \t\u00A0]{2,}",
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

    # -------------------------------------------------------------------------------------------- #
    #                              DYNAMIC REGEX PATTERNS                                          #
    # -------------------------------------------------------------------------------------------- #
    def _elongation(
        self, threshold: int = 3, max_elongation: int = 2, **kwargs
    ) -> Regex:
        """
        Generates a pattern for detecting elongated characters.

        Args:
            threshold (int): Minimum number of consecutive repeated characters to consider elongation.
            max_elongation (int): Maximum number of consecutive repeated characters allowed.

        Returns:
            Pattern: Regex pattern and replacement logic.
        """
        if threshold < 2 or max_elongation < 1:
            raise ValueError("threshold must be >= 2 and max_elongation must be >= 1")

        pattern = rf"([a-zA-Z0-9])\1{{{threshold - 1},}}"
        replacement = r"\1" * min(max_elongation, threshold)
        return Regex(pattern=pattern, replacement=replacement)

    # -------------------------------------------------------------------------------------------- #
    def _word_repetition(
        self, threshold: int = 3, max_repetitions: int = 1, **kwargs
    ) -> Regex:
        """
        Generates a pattern for detecting repeated words.

        Args:
            threshold (int): Minimum number of word repetitions to consider a match. Defaults to 3
            max_repetitions (Optional[int]): Maximum number of word repetitions to keep. Defaults to 1.

        Returns:
            Pattern: Regex pattern and replacement logic.
        """
        if threshold < 2:
            raise ValueError("min_repetitions must be >= 2")
        if max_repetitions < 1:
            raise ValueError("max_repetitions must be >= 1")


        pattern = rf"\b(\w+)\b(?:\s+\1\b){{{threshold - 1},}}"
        replacement = r"\1" * min(max_repetitions, threshold)
        return Regex(pattern=pattern, replacement=replacement)

    # -------------------------------------------------------------------------------------------- #
    def _phrase_repetition(
        self,
        length_of_phrase: int = 2,
        threshold: int = 2,
        max_repetitions: int = 1,
        **kwargs,
    ) -> Regex:
        """
        Generates a pattern for detecting repeated phrases.

        Args:
            length_of_phrase (int): Minimum length of a phrase (in words) to be considered.. Default = 2
            threshold (int): Minimum number of phrase repetitions to consider a match. Default = 2
            max_repetitions (int): Number of allowed repetitions after cleaning. Default = 1.

        Returns:
            Pattern: Regex pattern and replacement logic.
        """
        if length_of_phrase < 1:
            raise ValueError("length_of_phrase must be >= 1")
        if threshold < 2:
            raise ValueError("threshold must be >= 2")
        if max_repetitions < 1:
            raise ValueError("max_repetitions must be >= 1")

        pattern = rf"(\b\w+\b(?: \b\w+\b)*)\s*(?:\s*\1){{{threshold - 1},}}"
        replacement = r"\1" * min(max_repetitions, threshold)
        return Regex(pattern=pattern, replacement=replacement)

    # -------------------------------------------------------------------------------------------- #
    def _sequence_repetition(
        self,
        length_of_sequence: int = 3,
        threshold: int = 3,
        max_repetitions: int = 1,
        **kwargs,
    ) -> Regex:
        """
        Generates a pattern for detecting repeated sequences of characters.

        Args:
            length_of_sequence (int): Minimum length of a sequence (in characters) to be considered.. Default = 3
            threshold (int): Minimum number of sequence repetitions to consider a match. Default = 3
            max_repetitions (int): Number of allowed repetitions after cleaning. Default = 1.
        Returns:
            Pattern: Regex pattern and replacement logic.
        """
        if length_of_sequence < 1:
            raise ValueError("length_of_sequence must be >= 1")
        if threshold < 2:
            raise ValueError("threshold must be >= 2")
        if max_repetitions < 1:
            raise ValueError("max_repetitions must be >= 1")

        pattern = rf"(?i)(.{length_of_sequence})\s*(?:\s*\1){{{max(0, threshold - 1)},}}"
        replacement = r"\1" * min(max_repetitions, threshold)
        return Regex(pattern=pattern, replacement=replacement)
