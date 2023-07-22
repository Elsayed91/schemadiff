"""
Contains functionality for generating and saving reports of schema comparisons.
"""

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class ReportGenerator:
    """A class for generating and saving reports of schema comparisons.

    Methods:
        generate_report(grouped_files: Dict[str, List[str]]) -> Dict[str, Any]:
            Returns a dictionary that represents the report.
        save_report(report: Dict[str, Any], file_path: str) -> None:
            Saves the report as a JSON file at the specified file path.
    """

    @staticmethod
    def generate_report(grouped_files: dict[str, list[str]]) -> dict[str, Any]:
        """Returns a dictionary that represents the report.

        Args:
            grouped_files (Dict[str, List[str]]): A dictionary that groups files by their schema.

        Returns:
            Dict[str, Any]: A dictionary that represents the report.
        """
        report = grouped_files

        return report

    @staticmethod
    def save_report(report: dict[str, Any], file_path: str) -> None:
        """Saves the report as a JSON file at the specified file path.

        Args:
            report (Dict[str, Any]): The report to be saved.
            file_path (str): The file path where the report will be saved.

        Returns:
            None
        """
        try:
            with open(file_path, "w") as f:
                json.dump(report, f, indent=4)
            logger.info(f"Report saved to {file_path}.")
        except Exception as e:
            logger.error(f"Error while saving report: {str(e)}")
            raise
