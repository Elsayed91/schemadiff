"""
Provides a command line interface for comparing schemas of Parquet files and generating
reports.
Usage: 
    python -m schemadiff.cli --dir_path DIR_PATH [--fs_type {local,gcs,s3}] \
        [--report_path REPORT_PATH]
"""

import argparse

from schemadiff import compare_schemas


def main():
    parser = argparse.ArgumentParser(description="Group Parquet files by schema.")
    parser.add_argument("--dir_path", type=str, help="The directory path.")
    parser.add_argument(
        "--fs_type",
        type=str,
        default=None,
        help='The type of filesystem. Can be "local", "gcs", or "s3". Defaults to "local".',
    )
    parser.add_argument(
        "--report_path",
        type=str,
        default=None,
        help="The file path where the report will be saved. If None, no report is generated. Defaults to None.",
    )
    parser.add_argument(
        "--return_type",
        type=str,
        default="as_list",
        help='The return type. Can be "as_dict" or "as_list". Defaults to "as_list".',
    )

    args = parser.parse_args()
    grouped_files = compare_schemas(
        args.dir_path, args.fs_type, args.report_path, args.return_type
    )

    print(grouped_files)


if __name__ == "__main__":
    main()
