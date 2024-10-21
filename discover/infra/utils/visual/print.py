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
# Modified   : Monday October 21st 2024 01:04:31 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from datetime import date, datetime

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
        breadth = self._width - 4
        header = f"\n\n# {breadth * '='} #\n"
        header += f"#{title.center(self._width - 2, ' ')}#\n"
        header += f"# {breadth * '='} #\n"
        print(header)

    def print_trailer(self) -> None:
        """
        Prints a formatted trailer.
        """
        breadth = self._width - 4
        trailer = f"\n\n# {breadth * '='} #\n"
        print(trailer)

    def print_dict(self, title: str, data: dict, text: str = None) -> None:
        """
        Prints a dictionary in a formatted manner.

        Args:
            title (str): The title to be printed above the dictionary.
            data (dict): The dictionary to be printed.
            text (str): Text to print below the dictionary.
        """
        breadth = int(self._width / 2)
        s = f"\n\n{title.center(self._width, ' ')}"
        for k, v in data.items():
            if isinstance(v, IMMUTABLE_TYPES):
                if isinstance(v, float) or isinstance(v, int):
                    v = f"{v:,}"
                s += f"\n{k.rjust(breadth, ' ')} | {v}"
        if text:
            s += f"\n{text}"
        s += "\n\n"

        print(s)

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
        text = None
        d = df.to_dict("records")[list_index]
        if text_col:
            text = df[text_col].values[0]
            del d[text_col]
        self.print_dict(title=title, data=d, text=text)
