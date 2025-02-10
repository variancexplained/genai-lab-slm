#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /genailab/infra/utils/visual/print.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday May 6th 2024 11:07:56 pm                                                     #
# Modified   : Saturday February 8th 2025 10:43:31 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import textwrap
from datetime import date, datetime
from typing import Tuple, Union

import numpy as np
import pandas as pd

# ------------------------------------------------------------------------------------------------ #
IMMUTABLE_TYPES: tuple = (
    str,
    int,
    float,
    bool,
    np.int16,
    np.int32,
    np.int64,
    np.int8,
    np.uint8,
    np.uint16,
    np.float16,
    np.float32,
    np.float64,
    np.float128,
    np.bool_,
    datetime,
    date,
)


# ------------------------------------------------------------------------------------------------ #
#                                         PRINTER                                                  #
# ------------------------------------------------------------------------------------------------ #
class Printer:
    """
    A utility class for formatted printing of titles, subtitles, key-value pairs, dictionaries, and dataframes.

    This class provides methods to print content in a visually appealing format, including support for structured data
    like dictionaries and pandas DataFrames.

    Args:
        width (int, optional): The width of the printed content, in characters. Defaults to 80.
    """

    def __init__(self, width: int = 80) -> None:
        self._width = width

    def print_title(self, title: str) -> None:
        """
        Prints a formatted title enclosed in a decorative header.

        Args:
            title (str): The title text to be printed.
        """
        breadth = self._width - 2
        header = f"\n\n# {breadth * '='} #\n"
        header += f"#{title.center(self._width, ' ')}#\n"
        header += f"# {breadth * '='} #\n"
        print(header)

    def print_subtitle(self, subtitle: str, linestyle: str = "-") -> None:
        """
        Prints a formatted subtitle with an underline using the specified line style.

        Args:
            subtitle (str): The subtitle text to be printed.
            linestyle (str, optional): The character used for underlining the subtitle. Defaults to "-".
        """
        s = f"\n\n{subtitle.center(self._width, ' ')}"
        s += f"\n{(linestyle * len(subtitle)).center(self._width, ' ')}"
        print(s)

    def print_kv(self, k: str, v: Union[str, int, float]) -> None:
        """
        Prints a key-value pair in a formatted layout.

        Args:
            k (str): The key to be printed.
            v (Union[str, int, float]): The value associated with the key. If numeric, it will be formatted with commas.
        """
        breadth = int(self._width / 2)
        if isinstance(v, IMMUTABLE_TYPES):
            if isinstance(v, (float, int)):
                v = f"{v:,}"
            s = f"{k.rjust(breadth, ' ')} | {v}"
        print(s)

    def print_trailer(self) -> None:
        """
        Prints a decorative trailer to conclude a section.
        """
        breadth = self._width - 4
        trailer = f"\n\n# {breadth * '='} #\n"
        print(trailer)

    def print_string(self, string: str, centered: bool = True) -> None:
        """Prints a text string.

        Args:
            string (str): A string of text.
            centered (bool): Whether to center the string.

        """
        if centered:
            string = string.center(self._width, " ")
        print(string)

    def print_dict(self, title: str, data: dict, text_col: str = None) -> None:
        """
        Prints a dictionary in a structured, formatted layout.

        Args:
            title (str): The title of the section to be printed.
            data (dict): The dictionary containing key-value pairs to print.
            text_col (str, optional): A specific key in the dictionary whose value will be printed as a text block.
        """
        text = None
        breadth = int(self._width / 2)
        title_lines = title.split("\n")
        title_centered = '\n'.join(title_line.center(self._width, ' ') for title_line in title_lines)
        s = f"\n\n{title_centered}"
        for k, v in data.items():
            if text_col == k:
                text = v
            else:
                if isinstance(v, IMMUTABLE_TYPES):
                    if isinstance(v, (float, int)):
                        v = f"{v:,}"
                    s += f"\n{k.rjust(breadth, ' ')} | {v}"
        print(s)
        if text:
            print(textwrap.fill(text, self._width))

    def print_dataframe_as_dict(
        self,
        df: pd.DataFrame,
        title: str,
        list_index: int = 0,
        text_col: str = None,
    ) -> None:
        """
        Prints a specific row of a pandas DataFrame as a formatted dictionary.

        Args:
            df (pd.DataFrame): The DataFrame to print.
            title (str): The title of the section to be printed.
            list_index (int, optional): The index of the row in the DataFrame to be printed. Defaults to 0.
            text_col (str, optional): A specific column name whose value will be printed as a text block.
        """
        d = df.to_dict("records")[list_index]
        self.print_dict(title=title, data=d, text_col=text_col)


# ------------------------------------------------------------------------------------------------ #
#                                    DATAFRAME PRINTER                                             #
# ------------------------------------------------------------------------------------------------ #
class TablePrinter:
    """
    A utility class for printing tabular data in a structured format.

    This class provides methods to print headers, rows, and separators for tabular data,
    with customizable column names, widths, and layout.

    Args:
        ncols (int, optional): The number of columns in the table. Defaults to 4.
        columns (tuple, optional): A tuple containing the names of the columns. Defaults to ("Task", "Started", "Ended", "Runtime").
        colwidths (tuple, optional): A tuple specifying the widths of each column. Defaults to (40, 8, 8, 12).
    """

    def __init__(
        self,
        ncols: int = 5,
        columns: tuple = (
            "Task",
            "Started",
            "Ended",
            "Runtime",
            "Note",
        ),
        colwidths: tuple = (40, 8, 8, 14, 30),
    ) -> None:
        self.ncols = ncols
        self.columns = columns
        self.colwidths = colwidths
        self._validate()

        self._header_printed = False

    def print_header(
        self, sep: str = "-", top_linespace: bool = True, bottom_linespace: bool = False
    ) -> None:
        """
        Prints the table header based on the configured column names and widths.
        """
        self._validate()

        if top_linespace:
            print()

        header = self._format_header()

        print(header)
        self.print_separator(sep=sep)

        if bottom_linespace:
            print()

        self._header_printed = True

    def print_separator(self, sep: str = "-") -> None:
        print(sep * sum(self.colwidths))

    def _format_header(self) -> str:
        """
        Formats the table header as a string.

        Returns:
            str: The formatted table header.
        """
        header = ""
        for i in range(self.ncols):
            header += self.columns[i].ljust(self.colwidths[i])
        return header

    def print_line(self, data: Tuple[str]) -> None:
        """
        Prints a single row of data in the table.

        Args:
            data (Tuple[str]): A tuple containing the data to be printed in the row. The length of the tuple must match `ncols`.

        Raises:
            ValueError: If the length of the `data` tuple does not match `ncols`.
        """
        self._validate_line(data=data)
        if not self._header_printed:
            self.print_header()
        line = ""
        for i, datum in enumerate(data):
            line += datum.ljust(self.colwidths[i])
        print(line)

    def print_total_line(self, data: Tuple[str]) -> None:
        """
        Prints a total row for the data.

        Args:
            data (Tuple[str]): A tuple containing the data to be printed in the row. The length of the tuple must match `ncols`.

        Raises:
            ValueError: If the length of the `data` tuple does not match `ncols`.
        """
        self._validate_line(data=data)
        self.print_separator(sep="_")

        line = ""
        for i, datum in enumerate(data):
            line += datum.ljust(self.colwidths[i])
        print(line)

    def _validate(self) -> None:
        """
        Validates the configuration of the table, ensuring that the number of columns,
        column names, and column widths are consistent and do not exceed the line width.

        Raises:
            ValueError: If there is a mismatch in the number of columns, column names, or column widths.
            Warning: If the total width of the columns exceeds 100 characters.
        """
        if len(self.columns) == self.ncols == len(self.colwidths):
            pass
        else:
            msg = f"Column length mismatch. Ensure that ncols = {self.ncols} matches length of columns and colwidths."
            raise ValueError(msg)
        if sum(self.colwidths) > 100:
            msg = f"Column widths of {sum(self.colwidths)} exceed 100 characters per line. Consider shortening."
            raise Warning(msg)

    def _validate_line(self, data: tuple = ()) -> None:
        """
        Validates a single row of data to ensure it matches the expected number of columns.

        Args:
            data (tuple): A tuple containing the data for a single row.

        Raises:
            ValueError: If the length of the `data` tuple does not match `ncols`.
        """
        if len(data) != self.ncols:
            msg = f"The data argument must be a tuple of length {self.ncols} elements."
            raise ValueError(msg)
