# conftest.py

import os
import tempfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def temp_dir():
    """Create a temporary directory and return its path. The directory is deleted when the fixture goes out of scope."""
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


@pytest.fixture
def parquet_file_single(temp_dir):
    """Create a Parquet file in the temporary directory and return its path. The file is deleted when the fixture goes out of scope."""
    df = pd.DataFrame({"field": [1, 2, 3]})
    table = pa.Table.from_pandas(df)
    file_path = os.path.join(temp_dir, "file.parquet")
    pq.write_table(table, file_path)
    return file_path


@pytest.fixture
def parquet_files_multiple(temp_dir):
    """Create multiple Parquet files in the temporary directory and return their paths. The files are deleted when the fixture goes out of scope."""
    df1 = pd.DataFrame({"field1": [1, 2, 3]})
    df2 = pd.DataFrame({"field2": [4, 5, 6]})
    table1 = pa.Table.from_pandas(df1)
    table2 = pa.Table.from_pandas(df2)
    file1 = os.path.join(temp_dir, "file1.parquet")
    file2 = os.path.join(temp_dir, "file2.parquet")
    pq.write_table(table1, file1)
    pq.write_table(table2, file2)
    return file1, file2
