import logging

import boto3
from boto3.s3.transfer import TransferConfig, MB
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class S3Client:
    def __init__(self, bucket_name):
        self.s3 = boto3.client('s3')
        self._transfer_config = TransferConfig(max_concurrency=2, multipart_chunksize=64 * MB, max_io_queue=2,
                                               num_download_attempts=1)
        self._bucket_name = bucket_name

    def upload_file(self, bucket_key, upload_file_path, extra_args=None):
        if extra_args is None:
            extra_args = {}

        with open(upload_file_path, 'rb') as f:
            logger.info(f"Start uploading {upload_file_path} to S3 as key={bucket_key}")
            self.s3.upload_fileobj(f, Bucket=self._bucket_name, Key=bucket_key, ExtraArgs=extra_args,
                                   Config=self._transfer_config)
            logger.info(f"Finish uploading {upload_file_path} to S3 as key={bucket_key}")

    def get_file_metadata(self, key):
        """
        Returns None or dict
        :param key: key to get
        :return:
        """
        try:
            metadata = self.s3.head_object(Bucket=self._bucket_name, Key=key)
        except ClientError:
            return
        else:
            # object exists so print out the datetime
            return metadata
