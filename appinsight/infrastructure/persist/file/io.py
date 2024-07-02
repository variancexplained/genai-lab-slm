#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/persist/file/io.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday April 28th 2024 12:15:31 am                                                  #
# Modified   : Sunday June 30th 2024 09:56:50 pm                                                   #
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
import tarfile
from abc import ABC, abstractmethod
from typing import Any, List, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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
        return pd.read_excel(
            filepath,
            sheet_name=sheet_name,
            header=header,
            index_col=index_col,
            usecols=usecols,
            **kwargs,
        )

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
        return pd.read_csv(
            filepath,
            sep=sep,
            header=header,
            index_col=index_col,
            usecols=usecols,
            escapechar=escapechar,
            low_memory=low_memory,
            encoding=encoding,
        )

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
        data.to_csv(
            filepath,
            sep=sep,
            index=index,
            index_label=index_label,
            encoding=encoding,
            escapechar="\\",
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
        return pd.read_csv(
            filepath,
            sep=sep,
            header=header,
            index_col=index_col,
            usecols=usecols,
            low_memory=low_memory,
            encoding=encoding,
        )

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
        with open(filepath, "r", encoding="utf-8") as f:
            try:
                return yaml.safe_load(f)
            except yaml.YAMLError as e:  # pragma: no cover
                cls._logger.exception(e)
                raise IOError(e) from e
            finally:
                f.close()

    @classmethod
    def _write(cls, filepath: str, data: Any, **kwargs) -> None:
        with open(filepath, "w", encoding="utf-8") as f:
            try:
                yaml.dump(data, f)
            except yaml.YAMLError as e:  # pragma: no cover
                cls._logger.exception(e)
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
                return pickle.load(f)
            except pickle.PickleError() as e:  # pragma: no cover
                cls._logger.exception(e)
                raise IOError(e) from e
            finally:
                f.close()

    @classmethod
    def _write(cls, filepath: str, data: Any, write_mode: str = "wb", **kwargs) -> None:
        # Note, "a+" write_mode for append. If <TypeError: write() argument must be str, not bytes>
        # use "ab+"
        with open(filepath, write_mode) as f:
            try:
                pickle.dump(data, f)
            except pickle.PickleError() as e:  # pragma: no cover
                cls._logger.exception(e)
                raise (e)
            finally:
                f.close()


# ------------------------------------------------------------------------------------------------ #
#                                         PARQUET                                                  #
# ------------------------------------------------------------------------------------------------ #


class ParquetIO(IO):  # pragma: no cover
    @classmethod
    def _read(cls, filepath: str, **kwargs) -> Any:
        """Reads using pyarrow API.

        Args:
            filepath (str): Can be a file or directory path.

        """
        return pq.read_table(source=filepath).to_pandas()

    @classmethod
    def _write(
        cls,
        filepath: str,
        data: pd.DataFrame,
        partition_cols: Union[str, list] = None,
        row_group_size: int = 1073741824,
        existing_data_behavior: str = "delete_matching",
        **kwargs,
    ) -> None:
        """Writes a parquet file using pyarrow API.

        Args:
            filepath (str): Can be a directory or file path.
            data (pd.DataFrame): Pandas DataFrame
            partition_cols (Union[str,list]): String or iterable specifying the column(s) by
                which to partition the dataset. If not None, filepath is interpreted as the root directory for the dataset.
            row_group_size (int): The size of each parquet row group in bytes. Apache
                recommends a row group size of 1GB.
            existing_data_behavior (str): Controls how the dataset will handle data that already
                exists in the destination. The default behaviour is 'delete_matching'.        'overwrite_or_ignore' will ignore any existing data and will overwrite files with the same name as an output file. Other existing files will be ignored. This behavior, in combination with a unique basename_template for each write, will allow for an append workflow. 'error' will raise an error if any data exists in the destination.         'delete_matching' is useful when you are writing a partitioned dataset. The first time each partition directory is encountered the entire directory will be deleted. This allows you to overwrite old partitions completely.

        """
        table = pa.Table.from_pandas(data)
        partition_cols = (
            partition_cols if isinstance(partition_cols, list) else [partition_cols]
        )

        if partition_cols is None:
            pq.write_table(
                table=table,
                where=filepath,
                row_group_size=row_group_size,
                existing_data_behavior=existing_data_behavior,
            )
        else:
            pq.write_to_dataset(
                table=table,
                root_path=filepath,
                partition_cols=partition_cols,
                row_group_size=row_group_size,
                existing_data_behavior=existing_data_behavior,
            )


# ------------------------------------------------------------------------------------------------ #
#                                           HTML                                                   #
# ------------------------------------------------------------------------------------------------ #


class HtmlIO(IO):  # pragma: no cover
    @classmethod
    def _read(cls, filepath: str, **kwargs) -> Any:
        """Read the raw html."""
        file = codecs.open(filename=filepath, encoding="utf-8")
        return file.read()

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
            return json.load(json_file)

    @classmethod
    def _write(cls, filepath: str, data: dict, **kwargs) -> None:
        """Writes a dictionary to a json file."""
        with open(filepath, "w", encoding="utf-8") as json_file:
            if isinstance(data, list):
                for datum in data:
                    if isinstance(datum, dict):
                        json.dump(datum, json_file, indent=2)
                    else:
                        msg = "JsonIO supports dictionaries and lists of dictionaries only."
                        cls._logger.exception(msg)
                        raise ValueError(msg)
            else:
                try:
                    json.dump(data, json_file, indent=2)
                except json.JSONDecodeError as e:
                    cls._logger.exception(f"Exception of type {type(e)} occurred.\n{e}")
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
        "": ParquetIO,  # If no filepath, assumed to be Parquet IO that supports reading and writing from and to directories.
    }
    _logger = logging.getLogger(
        f"{__module__}.{__name__}",
    )

    @classmethod
    def read(cls, filepath: str, **kwargs) -> Any:
        io = cls._get_io(filepath)
        return io.read(filepath, **kwargs)

    @classmethod
    def write(cls, filepath: str, data: Any, **kwargs) -> None:
        io = cls._get_io(filepath)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        io.write(filepath=filepath, data=data, **kwargs)

    @classmethod
    def _get_io(cls, filepath: str) -> IO:
        try:
            file_format = os.path.splitext(filepath)[-1].replace(".", "")
            return IOService.__io[file_format]
        except TypeError as exc:
            if filepath is None:
                msg = "Filepath is None"
                cls._logger.exception(msg)
                raise ValueError(msg) from exc
        except KeyError as exc:
            msg = "File type {} is not supported.".format(file_format)
            cls._logger.exception(msg)
            raise ValueError(msg) from exc


# ------------------------------------------------------------------------------------------------ #
#                                  TAR GZ HANDLER                                                  #
# ------------------------------------------------------------------------------------------------ #
class TarGzHandler:
    """
    A class to handle .tar.gz file operations such as extracting and compressing directories or single files.

    Example:
        # To extract a .tar.gz file:
        handler = TarGzHandler()
        handler.extract('/path/to/file.tar.gz', '/path/to/extract/directory')

        # To compress a directory into a .tar.gz file:
        handler.compress_directory('/path/to/directory', '/path/to/file.tar.gz')

        # To compress a single file into a .tar.gz file:
        handler.compress_file('/path/to/file', '/path/to/file.tar.gz')
    """

    def extract(self, tar_gz_path, extract_dir):
        """
        Extracts the contents of a .tar.gz file to a specified directory.

        Args:
            tar_gz_path (str): The path to the .tar.gz file.
            extract_dir (str): The directory where the contents should be extracted.

        Raises:
            tarfile.TarError: If there is an error during extraction.
        """
        try:
            with tarfile.open(tar_gz_path, "r:gz") as tar:
                tar.extractall(path=extract_dir)
                print(f"Extracted {tar_gz_path} to {extract_dir}")
        except FileNotFoundError as e:
            print(f"Error extracting {tar_gz_path}: {e}")
            raise
        except tarfile.TarError as e:
            print(f"Error extracting {tar_gz_path}: {e}")
            raise

    def compress_directory(self, directory_path, tar_gz_path):
        """
        Compresses a directory into a .tar.gz file.

        Args:
            directory_path (str): The directory to be compressed.
            tar_gz_path (str): The path where the .tar.gz file will be created.

        Raises:
            tarfile.TarError: If there is an error during compression.
        """
        try:
            with tarfile.open(tar_gz_path, "w:gz") as tar:
                tar.add(directory_path, arcname=os.path.basename(directory_path))
                print(f"Compressed {directory_path} into {tar_gz_path}")
        except FileNotFoundError as e:
            print(f"Error compressing {directory_path} into {tar_gz_path}: {e}")
            raise
        except tarfile.TarError as e:
            print(f"Error compressing {directory_path} into {tar_gz_path}: {e}")
            raise

    def compress_file(self, file_path, tar_gz_path):
        """
        Compresses a single file into a .tar.gz file.

        Args:
            file_path (str): The file to be compressed.
            tar_gz_path (str): The path where the .tar.gz file will be created.

        Raises:
            tarfile.TarError: If there is an error during compression.
        """
        try:
            with tarfile.open(tar_gz_path, "w:gz") as tar:
                tar.add(file_path, arcname=os.path.basename(file_path))
                print(f"Compressed {file_path} into {tar_gz_path}")
        except FileNotFoundError as e:
            print(f"Error compressing {file_path} into {tar_gz_path}: {e}")
            raise
        except tarfile.TarError as e:
            print(f"Error compressing {file_path} into {tar_gz_path}: {e}")
            raise
