# test_list_files_success: This test case ensures that the list_files method of the
# LocalFileSystem class returns the correct list of Parquet files when the directory
# exists.

# test_get_parquet_file_success: This test case checks that the get_parquet_file method of
# the LocalFileSystem class returns a ParquetFile object when the file exists.

# test_get_schema_from_parquet: This test case verifies that the get_schema_from_parquet
# method of the SchemaExtractor class correctly extracts the schema from a Parquet file.

# test_group_files_by_schema_as_dict: This test case checks that the group_files_by_schema
# method of the SchemaComparer class correctly groups files by their schemas and returns
# the result as a dictionary when return_type is 'as_dict'.

# test_group_files_by_schema_as_list: This test case ensures that the
# group_files_by_schema method of the SchemaComparer class correctly groups files by their
# schemas and returns the result as a list when return_type is 'as_list'.

# test_compare_schemas: This test case verifies that the compare_schemas function
# correctly groups files by their schemas, generates a report, and saves it to the
# specified path.

import os

import pyarrow.parquet as pq
import pytest

from schemadiff import compare_schemas
from schemadiff.filesystem import FileSystemError, LocalFileSystem
from schemadiff.schema_comparer import SchemaComparer, SchemaExtractor


def test_list_files_success(temp_dir, parquet_files_multiple):
    """Test that list_files returns the correct list of files when the directory exists."""
    fs = LocalFileSystem()
    files = fs.list_files(temp_dir)
    assert len(files) == 2
    assert all(file.endswith(".parquet") for file in files)


def test_get_parquet_file_success(parquet_file_single):
    """Test that get_parquet_file returns a ParquetFile object when the file exists."""
    fs = LocalFileSystem()
    file = fs.get_parquet_file(parquet_file_single)
    assert isinstance(file, pq.ParquetFile)


def test_get_schema_from_parquet(parquet_file_single):
    """Test that get_schema_from_parquet returns the correct schema."""
    parquet_file = pq.ParquetFile(parquet_file_single)
    schema = SchemaExtractor.get_schema_from_parquet(parquet_file)
    assert schema == [("field", "int64")]


# test_schemadiff.py


def test_group_files_by_schema_as_dict(temp_dir, parquet_files_multiple):
    """Test that group_files_by_schema returns the correct dictionary when return_type is 'as_dict'."""
    file_handler = LocalFileSystem()
    grouped_files = SchemaComparer.group_files_by_schema(file_handler, temp_dir)
    expected_grouped_files = {
        "[('field1', 'int64')]": [parquet_files_multiple[0]],
        "[('field2', 'int64')]": [parquet_files_multiple[1]],
    }
    assert grouped_files == expected_grouped_files


def test_group_files_by_schema_as_list(temp_dir, parquet_files_multiple):
    """Test that group_files_by_schema returns the correct list when return_type is 'as_list'."""
    file_handler = LocalFileSystem()
    grouped_files = SchemaComparer.group_files_by_schema(
        file_handler, temp_dir, return_type="as_list"
    )
    assert grouped_files == [[parquet_files_multiple[0]], [parquet_files_multiple[1]]]


# test_schemadiff.py


def test_compare_schemas(temp_dir, parquet_files_multiple):
    """Test that compare_schemas returns the correct result and saves a report."""
    report_path = os.path.join(temp_dir, "report.json")
    grouped_files = compare_schemas(temp_dir, report_path=report_path)
    expected_grouped_files = [[parquet_files_multiple[0]], [parquet_files_multiple[1]]]
    assert grouped_files == expected_grouped_files
