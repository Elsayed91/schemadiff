"""
Provides functionality for extracting and comparing schemas of Parquet files.
"""

from collections import defaultdict
from typing import Union

import pyarrow.parquet as pq

from schemadiff.filesystem import FileSystem


class SchemaExtractor:
    """A class for extracting schema from Parquet files."""

    @staticmethod
    def get_schema_from_parquet(parquet_file: pq.ParquetFile) -> list[tuple[str, str]]:
        """Returns a sorted list of tuples, where each tuple represents a field in the
        schema.

        Args:
            parquet_file (pq.ParquetFile): The Parquet file to extract the schema from.

        Returns:
            list[Tuple[str, str]]: A sorted list of tuples, where each tuple represents a
            field in the schema.
        """
        arrow_schema = parquet_file.schema_arrow
        return sorted((field.name, str(field.type)) for field in arrow_schema)


class SchemaComparer:
    """A class for comparing schemas of Parquet files."""

    @staticmethod
    def group_files_by_schema(
        file_handler: FileSystem, dir_path: str, return_type: str = "as_dict"
    ) -> Union[dict[str, list[str]], list[list[str]]]:
        """Returns a dictionary or list that groups files by their schema.

        Args:
            file_handler (FileSystem): The file system handler.
            dir_path (str): The directory path.
            return_type (str, optional): The return type. Can be 'as_dict' or 'as_list'.
            Defaults to 'as_dict'.

        Returns:
            Union[dict[str, list[str]], list[list[str]]]: A dictionary or list that groups
            files by their schema.
        """
        files = file_handler.list_files(dir_path)
        schema_to_files = defaultdict(list)

        for file in files:
            parquet_file = file_handler.get_parquet_file(file)
            schema = SchemaExtractor.get_schema_from_parquet(parquet_file)
            schema_to_files[str(schema)].append(file)

        if return_type == "as_list":
            return list(schema_to_files.values())
        else:
            return dict(schema_to_files)
