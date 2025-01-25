#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailabslm/infra/utils/file/io.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 11th 2024 12:21:35 am                                           #
# Modified   : Saturday January 25th 2025 04:44:55 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import codecs
import json
import logging
import os
import pickle
from abc import ABC, abstractmethod
from typing import Any, List, Union

import pandas as pd
import yaml


# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, unused-argument
# ------------------------------------------------------------------------------------------------ #
#                                       IO BASE                                                    #
# ------------------------------------------------------------------------------------------------ #
class IO(ABC):  # pragma: no cover
    _logger = logging.getLogger(
        f"{__module__}.{__name__}",
    )

    @classmethod
    def read(cls, filepath: str, *args, **kwargs) -> Any:
        data = cls._read(filepath, **kwargs)
        return data

    @classmethod
    @abstractmethod
    def _read(cls, filepath: str, **kwargs) -> Any:
        pass

    @classmethod
    def write(cls, filepath: str, data: Any, *args, **kwargs) -> None:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        cls._write(filepath, data, **kwargs)

    @classmethod
    @abstractmethod
    def _write(cls, filepath: str, data: Any, **kwargs) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #
#                                         EXCEL IO                                                 #
# ------------------------------------------------------------------------------------------------ #


class ExcelIO(IO):  # pragma: no cover
    @classmethod
    def _read(
        cls,
        filepath: str,
        sheet_name: Union[str, int, list, None] = 0,
        header: Union[int, None] = 0,
        index_col: Union[int, str] = None,
        usecols: List[str] = None,
        **kwargs,
    ) -> pd.DataFrame:
        data = pd.read_excel(
            filepath,
            sheet_name=sheet_name,
            header=header,
            index_col=index_col,
            usecols=usecols,
            **kwargs,
        )
        logging.debug(
            f"Read file using {cls.__name__} and returning a {type(data).__name__}"
        )
        return data

    @classmethod
    def _write(
        cls,
        filepath: str,
        data: pd.DataFrame,
        sheet_name: str = "Sheet1",
        columns: Union[str, list] = None,
        header: Union[bool, list] = True,
        index: bool = False,
        **kwargs,
    ) -> None:
        logging.debug(f"Writing file of {type(data).__name__} using {cls.__name__}.")
        data.to_excel(
            excel_writer=filepath,
            sheet_name=sheet_name,
            columns=columns,
            header=header,
            index=index,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
#                                        CSV IO                                                    #
# ------------------------------------------------------------------------------------------------ #


class CSVIO(IO):  # pragma: no cover
    @classmethod
    def _read(
        cls,
        filepath: str,
        sep: str = ",",
        header: Union[int, None] = 0,
        index_col: Union[int, str] = None,
        usecols: List[str] = None,
        escapechar: str = None,
        low_memory: bool = False,
        encoding: str = "utf-8",
        **kwargs,
    ) -> pd.DataFrame:
        data = pd.read_csv(
            filepath,
            sep=sep,
            header=header,
            index_col=index_col,
            usecols=usecols,
            escapechar=escapechar,
            low_memory=low_memory,
            encoding=encoding,
            **kwargs,
        )
        logging.debug(
            f"Read file using {cls.__name__} and returning a {type(data).__name__}"
        )
        return data

    @classmethod
    def _write(
        cls,
        filepath: str,
        data: pd.DataFrame,
        sep: str = ",",
        index: bool = False,
        index_label: bool = None,
        encoding: str = "utf-8",
        **kwargs,
    ) -> None:
        logging.debug(f"Writing file of {type(data).__name__} using {cls.__name__}.")
        data.to_csv(
            filepath,
            sep=sep,
            index=index,
            index_label=index_label,
            encoding=encoding,
            escapechar="\\",
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
#                                        TSV IO                                                    #
# ------------------------------------------------------------------------------------------------ #


class TSVIO(IO):  # pragma: no cover
    @classmethod
    def _read(
        cls,
        filepath: str,
        sep: str = "\t",
        header: Union[int, None] = 0,
        index_col: Union[int, str] = None,
        usecols: List[str] = None,
        low_memory: bool = False,
        encoding: str = "utf-8",
        **kwargs,
    ) -> pd.DataFrame:
        data = pd.read_csv(
            filepath,
            sep=sep,
            header=header,
            index_col=index_col,
            usecols=usecols,
            low_memory=low_memory,
            encoding=encoding,
        )
        logging.debug(
            f"Read file using {cls.__name__} and returning a {type(data).__name__}"
        )
        return data

    @classmethod
    def _write(
        cls,
        filepath: str,
        data: pd.DataFrame,
        sep: str = "\t",
        index: bool = False,
        index_label: bool = None,
        encoding: str = "utf-8",
        **kwargs,
    ) -> None:
        logging.debug(f"Writing file of {type(data).__name__} using {cls.__name__}.")
        data.to_csv(
            filepath,
            sep=sep,
            index=index,
            index_label=index_label,
            encoding=encoding,
            escapechar="\\",
        )


# ------------------------------------------------------------------------------------------------ #
#                                        YAML IO                                                   #
# ------------------------------------------------------------------------------------------------ #


class YamlIO(IO):  # pragma: no cover
    @classmethod
    def _read(cls, filepath: str, **kwargs) -> dict:
        logging.debug(f"Reading file using {cls.__name__}")
        with open(filepath, "r", encoding="utf-8") as f:
            try:
                data = yaml.load(f, Loader=yaml.FullLoader)
            except yaml.YAMLError as e:  # pragma: no cover
                logging.exception(e)
                raise IOError(e) from e
            finally:
                f.close()

        logging.debug(
            f"Read file using {cls.__name__} and returning a {type(data).__name__}"
        )
        return data

    @classmethod
    def _write(cls, filepath: str, data: Any, **kwargs) -> None:
        logging.debug(f"Writing file of {type(data).__name__} using {cls.__name__}.")
        with open(filepath, "w", encoding="utf-8") as f:
            try:
                yaml.dump(data, f)
            except yaml.YAMLError as e:  # pragma: no cover
                logging.exception(e)
                raise IOError(e) from e
            finally:
                f.close()


# ------------------------------------------------------------------------------------------------ #
#                                         PICKLE                                                   #
# ------------------------------------------------------------------------------------------------ #


class PickleIO(IO):  # pragma: no cover
    @classmethod
    def _read(cls, filepath: str, **kwargs) -> Any:
        with open(filepath, "rb") as f:
            try:
                data = pickle.load(f)
            except pickle.PickleError() as e:  # pragma: no cover
                logging.exception(e)
                raise IOError(e) from e
            finally:
                f.close()
            logging.debug(
                f"Read file using {cls.__name__} and returning a {type(data).__name__}"
            )
            return data

    @classmethod
    def _write(cls, filepath: str, data: Any, write_mode: str = "wb", **kwargs) -> None:
        # Note, "a+" write_mode for append. If <TypeError: write() argument must be str, not bytes>
        # use "ab+"
        logging.debug(f"Writing file of {type(data).__name__} using {cls.__name__}.")
        with open(filepath, write_mode) as f:
            try:
                pickle.dump(data, f)
            except pickle.PickleError() as e:  # pragma: no cover
                logging.exception(e)
                raise (e)
            finally:
                f.close()


# ------------------------------------------------------------------------------------------------ #
#                                         PARQUET                                                  #
# ------------------------------------------------------------------------------------------------ #


class ParquetIO(IO):  # pragma: no cover
    @classmethod
    def _read(cls, filepath: str, **kwargs) -> pd.DataFrame:
        """Reads using pyarrow API.

        Args:
            filepath (str): Can be a file or directory path.

        """
        data = pd.read_parquet(path=filepath, **kwargs)
        logging.debug(
            f"Read file using {cls.__name__} and returning a {type(data).__name__}"
        )
        return data

    @classmethod
    def _write(
        cls,
        filepath: str,
        data: pd.DataFrame,
        **kwargs,
    ) -> None:
        """Writes a parquet file using pyarrow API.

        Args:
            filepath (str): Can be a directory or file path.
            data (pd.DataFrame): Pandas DataFrame.

        """
        logging.debug(f"Writing file of {type(data).__name__} using {cls.__name__}.")
        data.to_parquet(path=filepath, **kwargs)


# ------------------------------------------------------------------------------------------------ #
#                                           HTML                                                   #
# ------------------------------------------------------------------------------------------------ #


class HtmlIO(IO):  # pragma: no cover
    @classmethod
    def _read(cls, filepath: str, **kwargs) -> Any:
        """Read the raw html."""
        file = codecs.open(filename=filepath, encoding="utf-8")
        data = file.read()
        logging.debug(
            f"Read file using {cls.__name__} and returning a {type(data).__name__}"
        )
        return data

    @classmethod
    def _write(cls, filepath: str, data: pd.DataFrame, **kwargs) -> None:
        """Converts Pandas DataFrame to a pyarrow table, then persists."""
        raise NotImplementedError


# ------------------------------------------------------------------------------------------------ #
#                                          JSON                                                    #
# ------------------------------------------------------------------------------------------------ #


class JsonIO(IO):  # pragma: no cover
    @classmethod
    def _read(cls, filepath: str, **kwargs) -> Any:
        """Read the parsed dictionary from a json file."""
        with open(filepath, encoding="utf-8") as json_file:
            data = json.load(json_file)
        logging.debug(
            f"Read file using {cls.__name__} and returning a {type(data).__name__}"
        )
        return data

    @classmethod
    def _write(cls, filepath: str, data: dict, **kwargs) -> None:
        """Writes a dictionary to a json file."""
        logging.debug(f"Writing file of {type(data).__name__} using {cls.__name__}.")
        with open(filepath, "w", encoding="utf-8") as json_file:
            if isinstance(data, list):
                for datum in data:
                    if isinstance(datum, dict):
                        json.dump(datum, json_file, indent=2)
                    else:
                        msg = "JsonIO supports dictionaries and lists of dictionaries only."
                        logging.exception(msg)
                        raise ValueError(msg)
            else:
                try:
                    json.dump(data, json_file, indent=2)
                except json.JSONDecodeError as e:
                    logging.exception(f"Exception of type {type(e)} occurred.\n{e}")
                    raise


# ------------------------------------------------------------------------------------------------ #
#                                       IO SERVICE                                                 #
# ------------------------------------------------------------------------------------------------ #
class IOService:  # pragma: no cover
    __io = {
        "html": HtmlIO,
        "dat": CSVIO,
        "csv": CSVIO,
        "tsv": TSVIO,
        "yaml": YamlIO,
        "yml": YamlIO,
        "json": JsonIO,
        "pkl": PickleIO,
        "pickle": PickleIO,
        "xlsx": ExcelIO,
        "xls": ExcelIO,
        "parquet": ParquetIO,
        "": ParquetIO,
    }
    _logger = logging.getLogger(
        f"{__module__}.{__name__}",
    )

    @classmethod
    def read(cls, filepath: str, **kwargs) -> Any:
        io = cls._get_io(filepath, **kwargs)
        return io.read(filepath, **kwargs)

    @classmethod
    def write(cls, filepath: str, data: Any, **kwargs) -> None:
        io = cls._get_io(filepath, **kwargs)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        io.write(filepath=filepath, data=data, **kwargs)

    @classmethod
    def _get_io(cls, filepath: str, **kwargs) -> IO:
        try:
            # Attempt to obtain the io class based on the file format
            file_format = os.path.splitext(filepath)[-1].replace(".", "")
            return IOService.__io[file_format]
        except TypeError as exc:
            if filepath is None:
                msg = "Filepath is None"
                logging.exception(msg)
                raise ValueError(msg) from exc
        except KeyError as exc:
            msg = "File type {} is not supported.".format(file_format)
            logging.exception(msg)
            raise ValueError(msg) from exc
