import csv
import io
import logging

import boto3
from boto3.s3.transfer import TransferConfig, MB
from botocore.exceptions import ClientError

from glacier_backup.file_data import FileData

logger = logging.getLogger(__name__)

class GlacierUploader:
    # input_field_names = ['file_path','vault_name','type']

    def __init__(self, bucket_name='glacier-backups-651d8f3', temp_dir=None):
        self.s3 = boto3.client('s3')
        self._transfer_config = TransferConfig(max_concurrency=2, multipart_chunksize=64*MB, max_io_queue=2, num_download_attempts=1)
        self._bucket_name = bucket_name
        self._work_dir = temp_dir
        self._listings_dir = 'listings'

    def upload_csv_to_s3(self, bucket_name, file_name, file_obj):
        file_obj.seek(io.SEEK_SET)  # reset to beginning
        encoded_data = file_obj
        if isinstance(file_obj, io.StringIO):
            # convert StringIO to BytesIO for upload
            encoded_data = io.BytesIO(file_obj.read().encode())
        self.s3.upload_fileobj(encoded_data, bucket_name, file_name)
        logger.info("Successfully wrote {} to bucket {}".format(file_name, bucket_name))

    def upload_file_to_s3(self, gpg_file_name, bucket_name, extra_args=None, file_data=None):
        if extra_args is None:
            extra_args = {}

        gpg_file_path = file_data.encrypted_file_full_path
        file_name_key = file_data.encrypted_file_name

        with open(gpg_file_path, 'rb') as f:
            logger.info("Start uploading {} to S3 as key={}".format(gpg_file_name, file_name_key))
            self.s3.upload_fileobj(f, Bucket=bucket_name, Key=file_name_key, ExtraArgs=extra_args, Config=self._transfer_config)
            logger.info("Finished uploading {} to S3".format(gpg_file_name))

    def _create_input_file_list(self, input_file_path):
        input_file_list = []
        with open(input_file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                file_path = row['file_path']

                file_type = row['type']
                # TODO: fix args here
                input_file_list.append(
                    FileData(folder_or_file_path=file_path, file_type=file_type, work_dir=self._work_dir,
                             listings_root_path=self._listings_dir))
        return input_file_list

    def run(self, input_file_path):
        """
        Uploads backups to the S3 based glacier that allows us to keep track of filenames
        :param input_file_path: location of the input file
        :return:
        """

        input_file_list = self._create_input_file_list(input_file_path)

        for row in input_file_list:
            if row.type == 'photos':
                expected_file_name = row.encrypted_file_name
                try:
                    metadata = self.s3.head_object(Bucket=self._bucket_name, Key=expected_file_name)
                except ClientError:
                    logger.info("Detected a photo folder/file for upload {}. Setting storage class to DEEP_ARCHIVE".format(row.file_path))
                else:
                    # object exists so print out the datetime
                    logger.info(
                        "Found archive {} that was last uploaded on {} with class: {}. It will not be uploaded again".format(
                            expected_file_name,
                            metadata['LastModified'].isoformat(),
                            metadata['StorageClass']))
                    continue

            logger.info("Calling encrypt and compress with {}. Dest file name will be {}".format(row.file_path,
                                                                                                 row.compressed_file_name))
            compressed_path = row.compress()
            # TODO: pass in KEY
            key = None
            encrypted_path = row.encrypt(compressed_path, key)

            dir_listing = row.create_dir_listing(self._listings_dir)
            if dir_listing is not None:
                # TODO: upload listing here
                pass

            self.upload_file_to_s3(encrypted_path,
                                   self._bucket_name,
                                   extra_args={'StorageClass': row.storage_class},
                                   file_data=row)

        logger.info("ALL DONE")