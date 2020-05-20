import csv
import os
import logging

import boto3
from boto3.s3.transfer import TransferConfig, MB
from botocore.exceptions import ClientError

from glacier_backup.file_data import FileData

logger = logging.getLogger(__name__)

class GlacierUploader:
    # input_field_names = ['file_path','type']

    def __init__(self, bucket_name='glacier-backups-651d8f3', temp_dir=None):
        self.s3 = boto3.client('s3')
        self._transfer_config = TransferConfig(max_concurrency=2, multipart_chunksize=64*MB, max_io_queue=2, num_download_attempts=1)
        self._bucket_name = bucket_name
        self._work_dir = temp_dir
        self._listings_dir = os.path.join(self._work_dir, 'listings')
        os.makedirs(self._listings_dir)

    def upload_file_to_s3(self, bucket_name, bucket_key, upload_file_path, extra_args=None):
        if extra_args is None:
            extra_args = {}

        with open(upload_file_path, 'rb') as f:
            logger.info(f"Start uploading {upload_file_path} to S3 as key={bucket_key}")
            self.s3.upload_fileobj(f, Bucket=bucket_name, Key=bucket_key, ExtraArgs=extra_args, Config=self._transfer_config)
            logger.info(f"Finish uploading {upload_file_path} to S3 as key={bucket_key}")

    def _load_input_file(self, input_file_path):
        input_file_list = []
        with open(input_file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                file_path = row['file_path']

                if row['type'].upper() not in ['GLACIER', 'DEEP_ARCHIVE']:
                    raise ValueError(f"Invalid storage class passed in for {file_path}")
                file_type = row['type'].upper()
                input_file_list.append(
                    FileData(folder_or_file_path=file_path, file_type=file_type, work_dir=self._work_dir,
                             listings_root_path=self._listings_dir))
        return input_file_list

    def _should_upload_file(self, file_data):
        if file_data.type != 'DEEP_ARCHIVE':
            return True

        expected_file_name = file_data.encrypted_file_name
        try:
            metadata = self.s3.head_object(Bucket=self._bucket_name, Key=expected_file_name)
        except ClientError:
            logger.info(f"Did not find file existing already, will upload={file_data.file_path}")
            return True
        else:
            # object exists so print out the datetime
            logger.info(
                "Found archive {} that was last uploaded on {} with class: {}. It will not be uploaded again".format(
                    expected_file_name,
                    metadata['LastModified'].isoformat(),
                    metadata['StorageClass']))
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

            logger.info(
                f"Calling encrypt and compress with {row.file_path}. Dest file name will be {row.compressed_file_name}")
            compressed_path = row.compress()
            encrypted_path = row.encrypt(compressed_path, key)

            dir_listing = row.create_dir_listing(self._listings_dir)
            if dir_listing is not None:
                logger.info("Uploading listing information")
                listing_file_key = '/'.join(['listings', '.'.join([row.folder_name, 'gz'])])
                self.upload_file_to_s3(self._bucket_name, listing_file_key, dir_listing)
                logger.info(f"Finished uploading listing information as key={listing_file_key}")

            self.upload_file_to_s3(self._bucket_name,
                                   row.encrypted_file_name,
                                   encrypted_path,
                                   extra_args={'StorageClass': row.storage_class}
                                   )

        logger.info("ALL DONE")
