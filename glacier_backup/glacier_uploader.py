import argparse
import logging
import csv
import gzip
import io
import logging
import os
import subprocess
import tarfile

import boto3
import gnupg
from boto3.s3.transfer import TransferConfig, MB
from botocore.exceptions import ClientError

from glacier_backup.file_data import FileData

logger = logging.getLogger(__name__)

class GlacierUploader:
    # input_field_names = ['file_path','vault_name','type']

    def __init__(self, bucket_name='glacier-backups-651d8f3', archive_file_name='glacier_archive_list.csv', temp_dir=None):
        self.s3 = boto3.client('s3')
        self._transfer_config = TransferConfig(max_concurrency=2, multipart_chunksize=64*MB, max_io_queue=2, num_download_attempts=1)
        self._archive_file_name = archive_file_name
        self._bucket_name = bucket_name
        # self._work_dir = os.path.join(temp_dir, datetime.date.today().strftime('%Y-%m-%d'))
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

    def write_directory_list_to_file(self, file_data: FileData):
        """writes the directory listing out to a buffer for upload to S3"""
        listing_file_name = file_data.listing_file_name
        listing_file_obj = io.BytesIO()

        logger.info("Creating file containing the list of files {}".format(listing_file_name))

        with gzip.GzipFile(filename=listing_file_name, mode='w', fileobj=listing_file_obj) as gzipped_listing:
            # with gzip.open(listing_file_path, 'wt') as f:
            result = subprocess.run('du -ah {}'.format(file_data.file_path), shell=True, capture_output=True)
            gzipped_listing.write(result.stdout)

        logger.info("Done creating file {}".format(listing_file_name))
        self.upload_csv_to_s3(self._bucket_name, listing_file_name, listing_file_obj)
        return listing_file_obj

    def _compress_path(self, row_file_data):
        logger.info("Create directory for run: {}".format(self._work_dir))

        try:
            os.mkdir(self._work_dir)
        except FileExistsError:
            pass

        # set up tarred output file
        # TODO: move this into class
        dest_tar_file = row_file_data.compressed_file_full_path
        if os.path.isfile(row_file_data.file_path):
            dest_tar_file = row_file_data.file_path
            logger.info("Not compressing file {} as it is aleady compressed".format(dest_tar_file))

        logger.info("Output tar file is {}".format(dest_tar_file))

        # only tar when it is a directory and it doesnt exist
        if not os.path.exists(dest_tar_file):
            logger.info("Start tarring path: {}. Output path: {}".format(row_file_data.file_path, dest_tar_file))

            with tarfile.open(dest_tar_file, 'w:gz') as tar:
                tar.add(row_file_data.file_path)
            logger.info("Finished path: {}. Output path: {}".format(row_file_data.file_path, dest_tar_file))

    def _encrypt_path(self, row_file_data):
        # Init GPG class
        gpg = gnupg.GPG()
        key_to_use = gpg.list_keys()[0]  # Assumption is the proper key is the only one here
        fingerprint = key_to_use['fingerprint']
        logger.info("Fingerprint of key is {} and uid is {}".format(fingerprint, key_to_use['uids']))

        # set up tarred output file
        dest_tar_file = row_file_data.compressed_file_full_path
        # if os.path.isfile(row_file_data.file_path):
        #     dest_tar_file = row_file_data.file_path
        #     logger.info("Not compressing file {} as it is aleady compressed".format(dest_tar_file))

        # setup encrypted file path
        dest_gpg_encrypted_output = row_file_data.encrypted_file_full_path
        logger.info("Start GPG encrypting path: {} Output path: {}".format(dest_tar_file, dest_gpg_encrypted_output))

        if not os.path.exists(dest_gpg_encrypted_output):
            logger.info(
                "Start GPG encrypting path: {} Output path: {}".format(dest_tar_file, dest_gpg_encrypted_output))
            with open(dest_tar_file, 'rb') as tar_file:
                ret = gpg.encrypt_file(tar_file, output=dest_gpg_encrypted_output, armor=False, recipients=fingerprint)
            logger.info("{} {} {}".format(ret.ok, ret.status, ret.stderr))
        logger.info(
            "Finished GPG encrypting path: {}  Output path: {}".format(dest_tar_file, dest_gpg_encrypted_output))

        return dest_gpg_encrypted_output

    def upload_file_to_s3(self, gpg_file_name, bucket_name, extra_args=None, file_data=None):
        if extra_args is None:
            extra_args = {}

        gpg_file_path = file_data.encrypted_file_full_path
        file_name_key = file_data.encrypted_file_name

        with open(gpg_file_path, 'rb') as f:
            logger.info("Start uploading {} to S3 as key={}".format(gpg_file_name, file_name_key))
            self.s3.upload_fileobj(f, Bucket=bucket_name, Key=file_name_key, ExtraArgs=extra_args, Config=self._transfer_config)
            logger.info("Finished uploading {} to S3".format(gpg_file_name))

    def upload_s3_glacier(self, args):
        """
        Uploads backups to the S3 based glacier that allows us to keep track of filenames
        :param args:
        :return:
        """
        input_file_path = args.input_file_path

        input_file_list = []
        with open(input_file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                file_path = row['file_path']

                file_type = row['type']
                input_file_list.append(FileData(folder_or_file_path=file_path, file_type=file_type, work_dir=self._work_dir, listings_root_path='listings'))

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
            self._compress_path(row)
            self._encrypt_path(row)
            gpg_file_name = row.encrypted_file_full_path
            # gpg_file_name = self.encrypt_and_compress_path(row)
            if os.path.isdir(row.file_path):
                # don't upload the file_path if it happens to be a file (possibly compressed already)
                self.write_directory_list_to_file(row)
            self.upload_file_to_s3(gpg_file_name,
                                   self._bucket_name,
                                   extra_args={'StorageClass': row.storage_class},
                                   file_data=row)

        logger.info("ALL DONE")