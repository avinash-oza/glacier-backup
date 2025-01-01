import csv
import logging
import os

from glacier_backup.file_data import FileData
from glacier_backup.gpg_util import GpgUtil

logger = logging.getLogger(__name__)


class BackupRunner:
    def __init__(self, temp_dir=None, apprise_obj=None):
        """

        :param temp_dir:
        :param apprise_obj: Apprise instance for sending notifications during the process
        """

        self._work_dir = temp_dir
        self._listings_dir = os.path.join(self._work_dir, "listings")
        os.makedirs(self._listings_dir, exist_ok=True)
        self._apprise_obj = apprise_obj

    def _load_input_file(self, input_file_path):
        input_file_list = []
        with open(input_file_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                kwargs = {
                    "folder_or_file_path": row["file_path"],
                    "storage_class": row["storage_class"],
                    "work_dir": self._work_dir,
                    "listings_root_path": self._listings_dir,
                    "apprise_obj": self._apprise_obj,
                    "output_file_path": row.get("output_file_path"),
                }
                logger.info(
                    f"Added path {kwargs['folder_or_file_path']} with level:{kwargs['storage_class']} to paths to process"
                )
                input_file_list.append(FileData(**kwargs))
        logger.info(f"Finished loading {len(input_file_list)} paths to process")
        return input_file_list

    def _should_upload_file(self, file_data):
        if file_data.storage_class != "DEEP_ARCHIVE":
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
