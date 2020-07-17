import csv
import logging
import os

from glacier_backup.file_data import FileData
from glacier_backup.s3_uploader import S3Client

logger = logging.getLogger(__name__)


class BackupRunner:
    def __init__(self, bucket_name='glacier-backups-651d8f3', temp_dir=None, uploader=None):
        self._client = uploader
        if uploader is None:
            self._client = S3Client(bucket_name)

        self._work_dir = temp_dir
        self._listings_dir = os.path.join(self._work_dir, 'listings')
        os.makedirs(self._listings_dir, exist_ok=True)

    def _load_input_file(self, input_file_path):
        input_file_list = []
        with open(input_file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                kwargs = {
                    'folder_or_file_path': row['file_path'],
                    'storage_class': row['storage_class'],
                    'work_dir': self._work_dir,
                    'listings_root_path': self._listings_dir
                }
                logger.info(f"Added path {kwargs['folder_or_file_path']} with level:{kwargs['storage_class']} to paths to process")
                input_file_list.append(FileData(**kwargs))
        logger.info(f"Finished loading {len(input_file_list)} paths to process")
        return input_file_list

    def _should_upload_file(self, file_data):
        if file_data.storage_class != 'DEEP_ARCHIVE':
            return True

        expected_file_name = file_data.encrypted_file_name
        file_metadata = self._client.get_file_metadata(expected_file_name)

        if not file_metadata:
            logger.info(f"Did not find file existing already, will upload={file_data.file_path}")
            return True

        # object exists so print out the datetime
        logger.info(
            f"Found archive {expected_file_name} that was last uploaded on {file_metadata['LastModified'].isoformat()} with class: {file_metadata['StorageClass']}. It will not be uploaded again"
        )
        return False

    def run(self, input_file_path, key):
        """
        Uploads backups to the S3 based glacier that allows us to keep track of filenames
        :param input_file_path: location of the input file
        :return:
        """

        input_file_list = self._load_input_file(input_file_path)

        for row in input_file_list:
            if not self._should_upload_file(row):
                continue

            compressed_path = row.compress()
            encrypted_path = row.encrypt(compressed_path, key)

            dir_listing = row.create_dir_listing(self._listings_dir)
            if dir_listing is not None:
                logger.info("Uploading listing information")
                listing_file_key = '/'.join(['listings', '.'.join([row.folder_name, 'gz'])])
                self._client.upload_file(listing_file_key, dir_listing)
                logger.info(f"Finished uploading listing information as key={listing_file_key}")

            self._client.upload_file(row.encrypted_file_name,
                                     encrypted_path,
                                     extra_args={'StorageClass': row.storage_class}
                                     )

        logger.info("ALL DONE")
