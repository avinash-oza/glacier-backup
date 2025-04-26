import csv
import logging
import os
import posixpath as os_path
import tarfile

from glacier_backup.file_data import FileData, UPLOAD_TIME_EVERY_BACKUP
from glacier_backup.gpg_util import GpgUtil

logger = logging.getLogger(__name__)


class BackupRunner:
    def __init__(self, temp_dir=None):
        """

        :param temp_dir:
        """

        self._work_dir = temp_dir
        self._listings_dir = os.path.join(self._work_dir, "listings")
        os.makedirs(self._listings_dir, exist_ok=True)

    def _load_input_file(self, input_file_path) -> list[FileData]:
        input_file_list = []
        with open(input_file_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                upload_time = row.get("upload_time")
                kwargs = {
                    "file_path": row["file_path"],
                    "work_dir": self._work_dir,
                    "listings_root_path": self._listings_dir,
                    "output_file_path": row.get("output_file_path"),
                    "upload_time": upload_time,
                }
                logger.info(
                    f"Added path {kwargs['file_path']} with {upload_time=} to paths to process"
                )
                input_file_list.append(FileData(**kwargs))
        logger.info(f"Finished loading {len(input_file_list)} paths to process")
        return input_file_list

    def _should_upload_file(self, file_data):
        if file_data.upload_time == UPLOAD_TIME_EVERY_BACKUP:
            return True

        return False

    def run(self, input_file_path, key):
        """
        Creates encrypted backup files
        :param input_file_path: location of the input file
        :return:
        """

        # make sure the key passed in is valid
        key_info = GpgUtil.get_key(key)
        fingerprint = key_info["fingerprint"]

        input_file_list = self._load_input_file(input_file_path)

        for row in input_file_list:
            if not self._should_upload_file(row):
                continue

            compressed_path = self.compress(row)
            row.encrypt(compressed_path, fingerprint)

            row.create_dir_listing(self._listings_dir)

            row.cleanup()

        logger.info("ALL DONE")

    def compress(self, file_data: FileData):
        """
        Compresses the file in work_dir and returns the path to compressed file

        :return: path to the compressed file
        """
        if file_data.is_compressed():
            logger.warning(
                f"{file_data.file_path} is already compressed. Not compressing again"
            )
            return file_data.file_path

        # only tar when it is a directory and it doesnt exist
        dest_tar_file_path = file_data.get_dest_tar_file_path()
        if not os_path.exists(dest_tar_file_path):
            logger.info(
                f"Start compressing path: {file_data.file_path}. Output path: {dest_tar_file_path}"
            )
            with tarfile.open(dest_tar_file_path, "w:gz") as tar:
                tar.add(file_data.file_path)
            logger.info(
                "Finished path: {}. Output path: {}".format(
                    file_data.file_path, dest_tar_file_path
                )
            )
        else:
            logger.warning(
                f"Compressed path: {dest_tar_file_path} already exists. Not compressing again"
            )

        return dest_tar_file_path
