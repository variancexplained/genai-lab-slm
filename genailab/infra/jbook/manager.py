#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /genailab/infra/jbook/manager.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday June 3rd 2024 02:33:11 pm                                                    #
# Modified   : Sunday January 26th 2025 10:41:24 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""JupyterBook Utilities Module"""
# ------------------------------------------------------------------------------------------------ #
import os
import shutil
import subprocess
from glob import glob

import nbformat as nbf

DEFAULT_TAG_MAPPING = {
    "import": "hide-input",  # Hide the input w/ a button to show
    "SparkSessionPool": "remove-output",
    "jbook": "remove-cell",
}
JBOOK_CWD = "../.."
DEFAULT_SOURCE_FOLDER = "notebooks/content"
DEFAULT_DESTINATION_FOLDER = "jbook/genailab/genailab"


# ------------------------------------------------------------------------------------------------ #
class JupyterBookManager:
    """
    A class to manage the building, rebuilding, and publishing of Jupyter Books.

    Attributes:
    source_folder (str): The path to the source folder containing Jupyter notebooks.
    destination_folder (str): The path to the destination folder where Jupyter Book will be published.
    default_tag_mapping (dict): A default mapping of text patterns to cell tags for tagging notebooks.
    """

    def __init__(
        self,
        source_folder: str = DEFAULT_SOURCE_FOLDER,
        destination_folder: str = DEFAULT_DESTINATION_FOLDER,
        default_tag_mapping: dict = DEFAULT_TAG_MAPPING,
    ) -> None:
        """
        Initializes the JupyterBookManager instance.

        Args:
        source_folder (str): The path to the source folder containing Jupyter notebooks.
        destination_folder (str): The path to the destination folder where Jupyter Book will be published.
        default_tag_mapping (dict, optional): A default mapping of text patterns to cell tags for tagging notebooks.
            Defaults to an empty dictionary.
        """
        self._source_folder = source_folder
        self._destination_folder = destination_folder
        self._default_tag_mapping = default_tag_mapping

    def build(self, script_filepath: str = "scripts/jbook/build.sh") -> None:
        """
        Executes the build script for building the Jupyter Book.

        Args:
        script_filepath (str, optional): The filepath of the build script. Defaults to "scripts/jbook/build.sh".
        """
        absolute_path = os.path.abspath(script_filepath)
        self._run_script(absolute_path)

    def rebuild(self, script_filepath: str = "scripts/jbook/rebuild.sh") -> None:
        """
        Executes the rebuild script for rebuilding the Jupyter Book.

        Args:
        script_filepath (str, optional): The filepath of the rebuild script. Defaults to "scripts/jbook/rebuild.sh".
        """
        absolute_path = os.path.abspath(script_filepath)
        self._run_script(absolute_path)

    def publish(self, subdirectory: str = None) -> None:
        """
        Publishes the Jupyter Book by copying recent files to the destination folder.

        Args:
        subdirectory (str, optional): The subdirectory within the source folder to publish. Defaults to None.
        """
        if subdirectory:
            source_subfolder = os.path.join(self._source_folder, subdirectory)
            destination_subfolder = os.path.join(self._destination_folder, subdirectory)
        else:
            source_subfolder = self._source_folder
            destination_subfolder = self._destination_folder

        self._copy_recent_files(source_subfolder, destination_subfolder)

    def tag(self, tag_mapping: dict = None) -> None:
        """
        Adds cells tags to Jupyter notebooks based on text patterns.

        Args:
        tag_mapping (dict, optional): A mapping of text patterns to cell tags. Defaults to None.
        """
        # Collect a list of all notebooks in the content folder
        notebooks = glob(f"{self._source_folder}/*.ipynb", recursive=True)

        # Text to look for in adding tags
        tag_mapping = tag_mapping or self._default_tag_mapping

        # Search through each notebook and look for the text, add a tag if necessary
        for ipath in notebooks:
            ntbk = nbf.read(ipath, nbf.NO_CONVERT)

            for cell in ntbk.cells:
                cell_tags = cell.get("metadata", {}).get("tags", [])
                for key, val in tag_mapping.items():
                    if key in cell["source"]:
                        if val not in cell_tags:
                            cell_tags.append(val)
                if len(cell_tags) > 0:
                    cell["metadata"]["tags"] = cell_tags

            nbf.write(ntbk, ipath)

    def _copy_recent_files(self, source_folder: str, destination_folder: str) -> None:
        """
        Copies recent files from the source folder to the destination folder.

        Args:
        source_folder (str): The path to the source folder.
        destination_folder (str): The path to the destination folder.
        """
        os.makedirs(destination_folder, exist_ok=True)

        files_replaced = 0

        for root, dirs, files in os.walk(source_folder):
            for file in files:
                source_file = os.path.join(root, file)
                rel_path = os.path.relpath(root, source_folder)
                dest_file = os.path.join(destination_folder, rel_path, file)
                dest_dir = os.path.dirname(dest_file)

                if not os.path.exists(dest_dir):
                    os.makedirs(dest_dir)

                try:
                    if os.path.exists(dest_file):
                        source_mod_time = os.path.getmtime(source_file)
                        dest_mod_time = os.path.getmtime(dest_file)

                        if source_mod_time > dest_mod_time:
                            shutil.copy2(source_file, dest_file)
                            files_replaced += 1
                    else:
                        shutil.copy2(source_file, dest_file)
                except Exception as e:
                    print(f"Error copying {source_file} to {dest_file}: {e}")

        print(f"Publish replaced {files_replaced} files.")

    def _run_script(self, script_filepath):
        try:
            github_token = os.getenv(
                "GITHUB_TOKEN"
            )  # Fetch the GitHub token from the environment

            result = subprocess.run(
                ["bash", script_filepath],
                capture_output=True,
                text=True,
                timeout=300,  # Timeout after 5 minutes
                env={
                    **os.environ,
                    "GITHUB_TOKEN": github_token,
                },  # Add other environment variables as needed
            )
            print("Output:\n", result.stdout)
            print("Error:\n", result.stderr)
        except subprocess.TimeoutExpired:
            print(f"The script {script_filepath} timed out.")
        except Exception as e:
            print(f"An error occurred while running the script {script_filepath}: {e}")
