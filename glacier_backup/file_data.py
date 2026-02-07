import logging
import os
import posixpath as os_path
from dataclasses import dataclass

logger = logging.getLogger(__name__)

UPLOAD_TIME_ONCE = "ONCE"
UPLOAD_TIME_EVERY_BACKUP = "EVERY_BACKUP"


@dataclass(kw_only=True)
class FileData:
    SUPPORTED_UPLOAD_TIMES = [UPLOAD_TIME_ONCE, UPLOAD_TIME_EVERY_BACKUP]

    file_path: str
    work_dir: str
    output_file_path: str = ""
    upload_time: str = UPLOAD_TIME_EVERY_BACKUP

    def __post_init__(self):

        if self.upload_time.upper() not in self.SUPPORTED_UPLOAD_TIMES:
            raise ValueError(
                f"Path: {self.file_path}, {self.upload_time=} not supported"
            )

        os.makedirs(self.work_dir, exist_ok=True)

    @property
    def folder_name(self):
        """
        returns the last part of the path with spaces replaces
        :return:
        """
        return os_path.basename(self.file_path).replace(" ", "_")

    @property
    def compressed_file_name(self):
        if self.is_compressed:
            # keep the compressed file as is
            return self.folder_name
        if self.output_file_path:
            return ".".join([self.output_file_path, "tar.gz"])

        return ".".join([self.folder_name, "tar.gz"])

    @property
    def is_compressed(self):
        return self.file_path.endswith("bz2") or self.file_path.endswith("gz")

    @property
    def encrypted_file_name(self):
        return ".".join([self.compressed_file_name, "gpg"])

    @property
    def dest_tar_file_path(self):
        dest_tar_file_path = os_path.join(self.work_dir, self.compressed_file_name)
        return dest_tar_file_path
