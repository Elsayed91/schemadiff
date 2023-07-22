"""
Handles interactions with different file systems including local, Google Cloud Storage,
and Amazon S3.
"""

import importlib.util
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Union

import pyarrow.parquet as pq

from schemadiff.m_exceptions import FileSystemError


class FileSystem(ABC):
    """An abstract base class for file system interactions.

    This class defines the interface for a file system, including methods to
    list files and retrieve Parquet files.
    """

    @abstractmethod
    def list_files(self, dir_path: Union[str, Path]) -> list[str]:
        """Abstract method that should return a list of file paths."""
        pass

    @abstractmethod
    def get_parquet_file(self, file_path: str) -> pq.ParquetFile:
        """Abstract method that should return a ParquetFile object."""
        pass


class LocalFileSystem(FileSystem):
    """A class to interact with a local filesystem. Inherits from the abstract base class
    FileSystem.

    Methods:
        list_files(dir_path: str) -> list[str]:
            Returns a list of Parquet file paths in the directory.
        get_parquet_file(file_path: str) -> pq.ParquetFile:
            Returns a ParquetFile object.
    """

    def list_files(self, dir_path: Path) -> list[str]:
        """Lists all Parquet files in the provided directory."""
        dir_path = Path(dir_path)
        if "*" in dir_path.name:  # If the last part of the path contains a wildcard
            file_pattern = dir_path.name
            dir_path = dir_path.parent
        else:
            file_pattern = "*.parquet"

        if not dir_path.is_dir():
            raise FileSystemError(f"{dir_path} is not a directory.")
        return sorted(str(path) for path in dir_path.glob(file_pattern))

    def get_parquet_file(self, file_path: str) -> pq.ParquetFile:
        """Loads a Parquet file from the local filesystem."""
        file_path_obj = Path(file_path)
        if not file_path_obj.is_file():
            raise FileSystemError(f"{file_path} is not a file.")
        if file_path_obj.suffix != ".parquet":
            raise FileSystemError(f"{file_path} is not a Parquet file.")
        try:
            return pq.ParquetFile(file_path)
        except Exception as e:
            raise FileSystemError(
                f"Error opening {file_path} as a Parquet file: {str(e)}"
            )


class S3FileSystem(FileSystem):
    """A class to interact with Amazon S3. Inherits from the abstract base class
    FileSystem."""

    def __init__(self, **kwargs):
        if importlib.util.find_spec("s3fs") is None:
            raise ImportError(
                "The s3fs library is required to use the S3FileSystem class."
            )
        import s3fs

        self.fs = s3fs.S3Filesystem()

    def list_files(self, dir_path: str) -> list[str]:
        """Lists all files in the provided S3 directory."""
        return ["s3://" + path for path in sorted(self.fs.glob(dir_path))]

    def get_parquet_file(self, file_path: str) -> pq.ParquetFile:
        """Loads a Parquet file from Amazon S3."""
        try:
            with self.fs.open(file_path) as f:
                return pq.ParquetFile(f)
        except Exception as e:
            raise FileSystemError(
                f"Error opening {file_path} as a Parquet file: {str(e)}"
            )


class GCSFileSystem(FileSystem):
    """A class to interact with Google Cloud Storage. Inherits from the abstract base
    class FileSystem."""

    def __init__(self):
        if importlib.util.find_spec("gcsfs") is None:
            raise ImportError(
                "The gcsfs library is required to use the GCSFileSystem class."
            )
        import gcsfs

        self.fs = gcsfs.GCSFileSystem()

    def list_files(self, dir_path: str) -> list[str]:
        """Lists all files in the provided GCS directory."""
        return ["gs://" + path for path in sorted(self.fs.glob(dir_path))]  # type: ignore

    def get_parquet_file(self, file_path: str) -> pq.ParquetFile:
        """Loads a Parquet file from Google Cloud Storage."""
        try:
            with self.fs.open(file_path) as f:
                return pq.ParquetFile(f)
        except Exception as e:
            raise FileSystemError(
                f"Error opening {file_path} as a Parquet file: {str(e)}"
            )


class FileSystemFactory:
    """A factory class for creating FileSystem instances.

    Methods:
        create_filesystem(type: str) -> Union[LocalFileSystem, None]:
            Returns a FileSystem object of the specified type.
    """

    @staticmethod
    def create_filesystem(
        type: Optional[str] = None, path: Optional[str] = None
    ) -> Union[LocalFileSystem, GCSFileSystem, S3FileSystem]:
        """
        Returns a FileSystem object of the specified type.

        Args:
            type (str, optional): The type of filesystem. Can be 'local', 'gcs', or 's3'.
            path (str, optional): The path from which to infer the filesystem type if no type is provided.

        Returns:
            Union[LocalFileSystem, GCSFileSystem, S3FileSystem]: A FileSystem object of the specified type.

        Raises:
            ValueError: If an unsupported filesystem type is provided.
        """
        if type is None:
            if path.startswith("gs://"):  # type: ignore
                type = "gcs"
            elif path.startswith("s3://"):  # type: ignore
                type = "s3"
            else:
                type = "local"

        if type == "local":
            return LocalFileSystem()
        elif type == "gcs":
            return GCSFileSystem()
        elif type == "s3":
            return S3FileSystem()
        else:
            raise ValueError(f"Unsupported filesystem type: {type}")
