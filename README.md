# schemadiff

schemadiff is a niche package designed for situations where a — large — number of files on a filesystem are expected to have identical schemas, but they don't. This can present a challenge when working with distributed computing systems like `Apache Spark` or `Google BigQuery`, as unexpected schema differences can disrupt data loading and processing.

Consider a scenario where you are processing thousands of files, and a subset of them have schemas that are almost identical but not completely matching. This can lead to errors such as:

- BigQuery: `Error while reading data, error message: Parquet column '<COLUMN_NAME>' has type INT32 which does not match the target cpp_type DOUBLE File: gs://bucket/file.parquet`
- Spark: `Error: java.lang.UnsupportedOperationException: org.apache.parquet.column.values.dictionary.PlainValuesDictionary$PlainDoubleDictionary`

schemadiff addresses these issues by efficiently identifying the files with schema inconsistencies through reading file metadata.

## Installation

Install the package with pip:

```bash
pip install schemadiffed # schemadiff taken :p
```

## Usage

The package can be used as a Python library or as a command-line tool.

### Python Library

Here's an example of using schemadiff to group files by their schema:

```python
import os
from schemadiff import compare_schemas

os.environ['GOOGLE_CLOUD_CREDENTIALS'] = 'key.json'
grouped_files = compare_schemas('path/to/parquet_files', report_path='/desired/path/to/report.json')
```

In this example, `compare_schemas` groups the Parquet files in the directory `path/to/parquet_files` by their schema. It saves the results to `report.json` and also returns the grouped files as a list for potential downstream use.

### Command-Line Interface

schemadiff can also be used as a command-line tool. After installation, the command `compare-schemas` is available in your shell:

```bash
python schemadiff  --dir_path 'gs://<bucket>/yellow/*_2020*.parquet' --fs_type 'gcs' --report_path 'report.json' --return_type 'as_list'
```

## Features

- Efficient processing by reading the metadata of Parquet files.
- Supports local, GCS, S3 filesystems (you must be authenticated to your cloud service first).
- Supports wildcard characters for flexible file selection.