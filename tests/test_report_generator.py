# test_report_generator.py
import json
import os
from unittest import mock

from schemadiff.report_generator import ReportGenerator


def test_generate_report():
    """Test that generate_report correctly generates a report."""
    grouped_files = {"[('field', 'int64')]": ["/path/to/file1.parquet"]}
    report = ReportGenerator.generate_report(grouped_files)
    assert report == grouped_files


def test_save_report(temp_dir):
    """Test that save_report correctly saves a report to a file."""
    report = {"grouped_files": {"[('field', 'int64')]": ["/path/to/file1.parquet"]}}
    report_path = os.path.join(temp_dir, "report.json")
    ReportGenerator.save_report(report, report_path)
    with open(report_path, "r") as f:
        saved_report = json.load(f)
    assert saved_report == report
