import json
import os
import tempfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from schemadiff import compare_schemas


def test_integration(parquet_files_multiple):
    """Integration test for the compare_schemas function."""
    file1, file2 = parquet_files_multiple

    # Call the compare_schemas function.
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmpfile:
        report_path = tmpfile.name
    grouped_files = compare_schemas(os.path.dirname(file1), report_path=report_path)

    # Check that the function returned the correct result.
    table1 = pq.ParquetFile(file1).schema_arrow
    table2 = pq.ParquetFile(file2).schema_arrow
    expected_grouped_files = [[file1], [file2]]
    assert grouped_files == expected_grouped_files
