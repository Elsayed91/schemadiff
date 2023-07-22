from typing import Optional, Union

from schemadiff.filesystem import FileSystemFactory
from schemadiff.report_generator import ReportGenerator
from schemadiff.schema_comparer import SchemaComparer, SchemaExtractor


def compare_schemas(
    dir_path: str,
    fs_type: Optional[str] = None,
    report_path: Union[str, None] = None,
    return_type: str = "as_list",
) -> Union[dict[str, list[str]], list[list[str]]]:
    """Compares schemas of Parquet files in a directory and optionally generates a report.

    Args:
        dir_path (str): The directory path.
        fs_type (str, optional): The type of filesystem. Can be 'local', 'gcs', or 's3'.
        Defaults to 'local'.
        report_path (Union[str, None], optional): The file path where the report will be
        saved. If None, no report is generated. Defaults to None.
        return_type (str, optional): The return type. Can be 'as_dict' or 'as_list'.
        Defaults to 'as_list'.

    Returns:
        Union[dict[str, list[str]], list[list[str]]]: A dictionary or list that groups
        files by their schema.
    """

    fs = FileSystemFactory.create_filesystem(fs_type, dir_path)
    grouped_files = SchemaComparer.group_files_by_schema(fs, dir_path, return_type)

    if report_path is not None:
        all_schemas = [
            SchemaExtractor.get_schema_from_parquet(fs.get_parquet_file(file_group[0]))
            for file_group in grouped_files
        ]

        # Finding differences between all schemas
        common_schema = set(all_schemas[0])
        for schema in all_schemas[1:]:
            common_schema.intersection_update(schema)
        differences = [list(set(schema) - common_schema) for schema in all_schemas]

        schema_to_files = {
            str(difference): file_group
            for difference, file_group in zip(differences, grouped_files)
        }

        report = ReportGenerator.generate_report(schema_to_files)  # type: ignore
        ReportGenerator.save_report(report, report_path)

    return grouped_files
