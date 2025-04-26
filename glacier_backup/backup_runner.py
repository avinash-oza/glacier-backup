import csv
import logging
import os

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

    def _load_input_file(self, input_file_path):
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

            compressed_path = row.compress()
            row.encrypt(compressed_path, fingerprint)

            row.create_dir_listing(self._listings_dir)

            row.cleanup()

        logger.info("ALL DONE")
