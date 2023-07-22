# Parquet Schema Comparer

Parquet Schema Comparer is a Python package that provides functionalities for comparing the schemas of Parquet files in a directory. It is especially useful for checking schema consistency in large datasets stored in a distributed file system.

It is designed with the following features:
- Supports multiple file systems including Local, Google Cloud Storage (GCS), Amazon S3, and Azure Blob Storage.
- Groups Parquet files by their schema, which is useful to identify files with identical schema in a directory.
- Generates a report of the schema groups in a directory, which can be returned as a dictionary or list or saved to a JSON file.

## Installation

You can install the package via pip:

```bash
pip install parquet-schema-comparer
```
python cli.py --dir_path 'gs://raw-0b6ed91423/yellow/*_2020*.parquet' --fs_type 'gcs' --report_path 'report.json' --return_type 'as_list'



## Usage

You can use the package in two ways: as a Python library or as a command-line tool.

### Python API

Here's a basic example of how to use the Python API:

```python
from parquet_schema_comparer import compare_schemas

# Compare schemas in a directory
compare_schemas('path/to/parquet_files', fs_type='local', report_path='report.json', return_type='as_list')
```

In this example, `compare_schemas` will group the Parquet files in the directory `path/to/parquet_files` by their schema. It will save a report to `report.json` and also return the grouped files as a list.

### Command-Line Interface

You can also use the package as a command-line tool. After installation, the command `compare-schemas` will be available in your shell:

```bash
compare-schemas path/to/parquet_files --fs_type local --report_path report.json --return_type as_list
```

## Contributing

Contributions to Parquet Schema Comparer are welcome. Please open an issue or submit a pull request on the GitHub repository.

## License

Parquet Schema Comparer is licensed under the MIT license.

## Contact

For any queries or support, please open an issue on the GitHub repository.


# SchemaDiff

SchemaDiff is a Python package specifically designed to aid in the processing of large volumes of data files by identifying and grouping files with similar schemas. This is particularly useful when working with distributed computing systems such as Apache Spark or Google BigQuery, where schema differences can cause issues during data loading and processing.

For example, if you're processing thousands of files and a subset of them have schemas that are almost, but not completely, identical, you might encounter errors like:

- BigQuery: `Error while reading data, error message: Parquet column 'airport_fee' has type INT32 which does not match the target cpp_type DOUBLE File: gs://bucket/file.parquet`
- Spark: `Error: java.lang.UnsupportedOperationException: org.apache.parquet.column.values.dictionary.PlainValuesDictionary$PlainDoubleDictionary`

These errors prevent you from loading all the data together, thereby preventing you from leveraging the processing efficiency of these systems. SchemaDiff addresses this issue by analyzing and grouping your files based on their schema, allowing you to cast and process files with the same schema together, bypassing these errors.

## Features

- Supports Local, Google Cloud Storage (GCS), and Amazon S3 file systems.
- Allows you to list files and retrieve Parquet files from the supported file systems.
- Provides a factory class to create FileSystem instances based on the specified type.
- Extracts and compares schemas from Parquet files.
- Generates reports of schema comparisons and saves them as JSON.

## Installation

To install SchemaDiff, you can clone the repository and install it using pip:

```bash
git clone https://github.com/username/schemadiff.git
cd schemadiff
pip install .
```

## Usage

Here is a simple example of how to use SchemaDiff to group files by their schema and generate a report:

```python
import os
from schemadiff import compare_schemas

os.environ['GOOGLE_CLOUD_CREDENTIALS'] = 'key.json'
grouped_files = compare_schemas('gs://bucket/*_2020*.parquet', report_path='report.json')
print(grouped_files)
```

The output will be a list of lists, where each inner list contains the paths of files with the same schema. A JSON report of the schema comparisons will also be saved to 'report.json'.

## Disclaimer

Please note that this is a use case specific package and is not intended for broad use. It was designed to solve a specific problem and may not cater to all general use cases or requirements.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details.