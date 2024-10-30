#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/infra/utils/visual/print.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday May 6th 2024 11:07:56 pm                                                     #
# Modified   : Tuesday October 29th 2024 12:23:08 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import textwrap
from datetime import date, datetime
from typing import Union

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


class Printer:
    """Class for printing in various formats.

    Attributes
    ----------
    _width : int
        The width of the printed output.
    """

    def __init__(self, width: int = 80) -> None:
        """
        Initializes the Printer with a specified width.

        Args:
            width (int): The width of the printed output. Default is 80.
        """
        self._width = width

    def print_header(self, title: str) -> None:
        """
        Prints a formatted header.

        Args:
            title (str): The title to be printed in the header.
        """
        breadth = self._width - 2
        header = f"\n\n# {breadth * '='} #\n"
        header += f"#{title.center(self._width, ' ')}#\n"
        header += f"# {breadth * '='} #\n"
        print(header)

    def print_subheader(self, subtitle: str, linestyle: str = "-") -> None:
        """
        Prints a centered sub header with an underline

        Args:
            subtitle (str): The subtitle for the subheader.
        """
        s = f"\n\n{subtitle.center(self._width, ' ')}"
        s += f"\n{(linestyle*len(subtitle)).center(self._width, ' ')}"
        print(s)

    def print_kv(self, k: str, v: Union[str, int, float]) -> None:
        """
        Prints a key/value pair justified around a center bar
        """
        breadth = int(self._width / 2)
        if isinstance(v, IMMUTABLE_TYPES):
            if isinstance(v, float) or isinstance(v, int):
                v = f"{v:,}"
            s = f"{k.rjust(breadth, ' ')} | {v}"
        print(s)

    def print_trailer(self) -> None:
        """
        Prints a formatted trailer.
        """
        breadth = self._width - 4
        trailer = f"\n\n# {breadth * '='} #\n"
        print(trailer)

    def print_dict(self, title: str, data: dict, text_col: str = None) -> None:
        """
        Prints a dictionary in a formatted manner.

        Args:
            title (str): The title to be printed above the dictionary.
            data (dict): The dictionary to be printed.
            text_col (str): The column containing text to print below the dictionary.
        """
        text = None
        breadth = int(self._width / 2)
        s = f"\n\n{title.center(self._width, ' ')}"
        for k, v in data.items():
            if text_col == k:
                text = v
            else:
                if isinstance(v, IMMUTABLE_TYPES):
                    if isinstance(v, float) or isinstance(v, int):
                        v = f"{v:,}"
                    s += f"\n{k.rjust(breadth, ' ')} | {v}"
        print(s)
        if text:
            print(textwrap.fill(text, 80))

    def print_dataframe_as_dict(
        self,
        df: pd.DataFrame,
        title: str,
        list_index: int = 0,
        text_col: str = None,
    ) -> None:
        """
        Prints one row of a DataFrame, indexed by list_index, as a dictionary.

        Converts a DataFrame to a list of dictionaries, and prints the designated dictionary
        from the list index.

        Args:
            df (pd.DataFrame): A pandas DataFrame object.
            title (str): The title to be printed above the dictionary.
            text_col (str): A column of text to print.
            list_index (int): The index within the list of dictionaries (rows) to print. Default is 0.
        """
        d = df.to_dict("records")[list_index]
        self.print_dict(title=title, data=d, text_col=text_col)
