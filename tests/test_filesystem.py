import os
from unittest import mock

import pyarrow.parquet as pq
import pytest

from schemadiff.filesystem import (FileSystem, FileSystemError,
                                   FileSystemFactory, LocalFileSystem)


def test_create_filesystem_local():
    """Test that create_filesystem correctly creates a LocalFileSystem."""
    fs = FileSystemFactory.create_filesystem("local")
    assert isinstance(fs, LocalFileSystem)


def test_create_filesystem_unsupported():
    """Test that create_filesystem raises a ValueError for an unsupported filesystem type."""
    with pytest.raises(ValueError):
        FileSystemFactory.create_filesystem("unsupported")


import pandas as pd
import pyarrow as pa


def test_list_files_success(tmp_path):
    """Test that list_files returns the correct list of files when the directory exists."""
    # Create two Parquet files and one text file in the temporary directory
    for i in range(2):
        df = pd.DataFrame({"field": [i]})
        table = pa.Table.from_pandas(df)
        pq.write_table(table, tmp_path / f"file{i}.parquet")
    with open(tmp_path / "file.txt", "w") as f:
        f.write("text file")

    fs = LocalFileSystem()
    files = fs.list_files(str(tmp_path))
    assert len(files) == 2
    assert all(file.endswith(".parquet") for file in files)


def test_get_parquet_file_success(tmp_path):
    """Test that get_parquet_file returns a ParquetFile object when the file exists."""
    # Create a Parquet file in the temporary directory
    df = pd.DataFrame({"field": [1]})
    table = pa.Table.from_pandas(df)
    pq.write_table(table, tmp_path / "file.parquet")

    fs = LocalFileSystem()
    file = fs.get_parquet_file(str(tmp_path / "file.parquet"))
    assert isinstance(file, pq.ParquetFile)


def test_list_files_failure():
    """Test that list_files raises a FileSystemError when the directory does not exist."""
    with mock.patch("os.path.isdir", return_value=False):
        fs = LocalFileSystem()
        with pytest.raises(FileSystemError):
            fs.list_files("/path/to/nonexistent/dir")


def test_get_parquet_file_failure():
    """Test that get_parquet_file raises a FileSystemError when the file does not exist."""
    with mock.patch("os.path.isfile", return_value=False):
        fs = LocalFileSystem()
        with pytest.raises(FileSystemError):
            fs.get_parquet_file("/path/to/nonexistent/file.parquet")


def test_list_files_returns_empty_list(temp_dir):
    """Test that list_files returns an empty list when the directory contains no Parquet files."""
    fs = LocalFileSystem()
    # Write non-Parquet files into the temporary directory.
    for i in range(3):
        with open(os.path.join(temp_dir, f"file{i}.txt"), "w") as f:
            f.write("This is a text file.")
    files = fs.list_files(temp_dir)
    assert files == []


def test_list_files_returns_sorted_list(temp_dir, parquet_files_multiple):
    """Test that list_files returns a sorted list of file paths."""
    fs = LocalFileSystem()
    files = fs.list_files(temp_dir)
    assert files == sorted(parquet_files_multiple)


def test_get_parquet_file_raises_error_for_non_parquet_file(temp_dir):
    """Test that get_parquet_file raises a FileSystemError when given a non-Parquet file."""
    fs = LocalFileSystem()
    non_parquet_file = os.path.join(temp_dir, "file.txt")
    with open(non_parquet_file, "w") as f:
        f.write("This is a text file.")
    with pytest.raises(
        FileSystemError, match=f"{non_parquet_file} is not a Parquet file."
    ):
        fs.get_parquet_file(non_parquet_file)


def test_get_parquet_file_correct_schema(parquet_file_single):
    """Test that get_parquet_file returns a ParquetFile object with the correct schema."""
    fs = LocalFileSystem()
    file = fs.get_parquet_file(parquet_file_single)
    schema = file.schema_arrow
    expected_schema = [("field", "int64")]
    assert [(field.name, str(field.type)) for field in schema] == expected_schema


def test_get_parquet_file_can_handle_relative_paths(parquet_file_single):
    fs = LocalFileSystem()
    relative_path = os.path.relpath(parquet_file_single)
    assert fs.get_parquet_file(relative_path) is not None


def test_get_parquet_file_can_handle_absolute_paths(parquet_file_single):
    fs = LocalFileSystem()
    absolute_path = os.path.abspath(parquet_file_single)
    assert fs.get_parquet_file(absolute_path) is not None


def test_get_parquet_file_can_handle_paths_with_spaces(tmp_path):
    fs = LocalFileSystem()
    file_path = tmp_path / "file with spaces.parquet"
    df = pd.DataFrame({"field": [1, 2, 3]})
    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path)
    assert fs.get_parquet_file(str(file_path)) is not None


def test_create_filesystem_raises_error_for_unsupported_filesystem_type():
    with pytest.raises(ValueError):
        FileSystemFactory.create_filesystem("unsupported")


def test_create_filesystem_raises_error_for_empty_filesystem_type():
    with pytest.raises(ValueError):
        FileSystemFactory.create_filesystem("")


def test_create_multiple_local_filesystem_objects():
    fs1 = FileSystemFactory.create_filesystem("local")
    fs2 = FileSystemFactory.create_filesystem("local")
    assert fs1 is not None
    assert fs2 is not None
    assert fs1 is not fs2


def test_cannot_instantiate_abstract_base_class():
    """Test that an error is raised when trying to create an instance of the abstract base class."""
    with pytest.raises(TypeError):
        fs = FileSystem()  # This should raise a TypeError


def test_subclass_must_implement_abstract_methods():
    """Test that a TypeError is raised when a subclass doesn't implement all abstract methods."""

    class IncompleteFileSystem(FileSystem):
        def list_files(self, dir_path: str) -> list[str]:
            pass  # This subclass doesn't implement get_parquet_file

    with pytest.raises(TypeError):
        fs = IncompleteFileSystem()  # This should raise a TypeError
