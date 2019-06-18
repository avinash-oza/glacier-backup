import argparse
import csv
import datetime
import gzip
import io
import logging
import os
import subprocess
import tarfile

import boto3
import gnupg
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig, MB


class GlacierUploader:
    # input_field_names = ['file_path','vault_name','type']

    def __init__(self, bucket_name='glacier-backups-651d8f3', archive_file_name='glacier_archive_list.csv'):
        self.s3 = boto3.client('s3')
        self._transfer_config = TransferConfig(max_concurrency=2, multipart_chunksize=64*MB, max_io_queue=2, num_download_attempts=1)
        self._archive_file_name = archive_file_name
        self._bucket_name = bucket_name

    def upload_csv_to_s3(self, bucket_name, file_name, file_obj):
        file_obj.seek(io.SEEK_SET)  # reset to beginning
        encoded_data = file_obj
        if isinstance(file_obj, io.StringIO):
            # convert StringIO to BytesIO for upload
            encoded_data = io.BytesIO(file_obj.read().encode())
        self.s3.upload_fileobj(encoded_data, bucket_name, file_name)
        logger.info("Successfully wrote {} to bucket {}".format(file_name, bucket_name))

    def write_directory_list_to_file(self, file_path):
        """writes the directory listing out to a buffer for upload to S3"""
        listing_file_name = os.path.join('listings', '.'.join([os.path.basename(file_path).replace(' ', '_'), 'gz']))
        listing_file_obj = io.BytesIO()

        logger.info("Creating file containing the list of files {}".format(listing_file_name))

        with gzip.GzipFile(filename=listing_file_name, mode='w', fileobj=listing_file_obj) as gzipped_listing:
            # with gzip.open(listing_file_path, 'wt') as f:
            result = subprocess.run('du -ah {}'.format(file_path), shell=True, capture_output=True)
            gzipped_listing.write(result.stdout)

        logger.info("Done creating file {}".format(listing_file_name))
        self.upload_csv_to_s3(self._bucket_name, listing_file_name, listing_file_obj)
        return listing_file_obj

    def encrypt_and_compress_path(self, file_path, temp_dir):
        tar_dest_dir = os.path.join(temp_dir, datetime.date.today().strftime('%Y-%m-%d'))
        logger.info("Create directory for run: {}".format(tar_dest_dir))

        try:
            os.mkdir(tar_dest_dir)
        except FileExistsError:
            pass

        # Init GPG class
        gpg = gnupg.GPG()
        key_to_use = gpg.list_keys()[0] # Assumption is the proper key is the only one here
        fingerprint = key_to_use['fingerprint']
        logger.info("Fingerprint of key is {} and uid is {}".format(fingerprint, key_to_use['uids']))

        # set up tarred output file
        output_file = '.'.join([os.path.basename(file_path).replace(' ', '_'), 'tar.gz'])
        dest_tar_file = os.path.join(tar_dest_dir, output_file)
        logger.info("Output tar file is {}".format(dest_tar_file))

        if not os.path.exists(dest_tar_file):
            logger.info("Start tarring path: {}. Output path: {}".format(file_path, dest_tar_file))

            with tarfile.open(dest_tar_file, 'w:gz') as tar:
                tar.add(file_path)
            logger.info("Finished path: {}. Output path: {}".format(file_path, dest_tar_file))

        # setup encrypted file path
        encrypted_output = '.'.join([output_file, 'gpg'])
        dest_gpg_encrypted_output = os.path.join(tar_dest_dir, encrypted_output)
        logger.info("Start GPG encrypting path: {} Output path: {}".format(dest_tar_file, dest_gpg_encrypted_output))

        if not os.path.exists(dest_gpg_encrypted_output):
            logger.info("Start GPG encrypting path: {} Output path: {}".format(dest_tar_file, dest_gpg_encrypted_output))
            with open(dest_tar_file, 'rb') as tar_file:
                ret = gpg.encrypt_file(tar_file, output=dest_gpg_encrypted_output, armor=False, recipients=fingerprint)
            logger.info("{} {} {}".format(ret.ok, ret.status, ret.stderr))
        logger.info("Finished GPG encrypting path: {}  Output path: {}".format(dest_tar_file, dest_gpg_encrypted_output))

        return dest_gpg_encrypted_output, encrypted_output

    def upload_file_to_s3(self, gpg_file_path, gpg_file_name, bucket_name, extra_args=None):
        if extra_args is None:
            extra_args = {}

        with open(gpg_file_path, 'rb') as f:
            logger.info("Start uploading {} to S3".format(gpg_file_name))
            self.s3.upload_fileobj(f, Bucket=bucket_name, Key=gpg_file_name, ExtraArgs=extra_args, Config=self._transfer_config)
            logger.info("Finished uploading {} to S3".format(gpg_file_name))

    def upload_s3_glacier(self, args):
        """
        Uploads backups to the S3 based glacier that allows us to keep track of filenames
        :param args:
        :return:
        """
        input_file_path = args.input_file_path
        temp_dir = args.temp_dir

        # read the input files and only add those that we don't have already
        input_file_dict = {}
        with open(input_file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                file_path = row['file_path']

                input_file_dict[file_path] = row

        for key, values_dict in input_file_dict.items():
            vault_name = values_dict['vault_name']
            file_path = values_dict['file_path']
            dir_type = row['type']
            storage_class = 'GLACIER'

            if dir_type == 'photos':
                # TODO: refactor this out with encrypt_and_compress_path
                expected_file_name = '.'.join([os.path.basename(file_path).replace(' ', '_'), 'tar.gz.gpg'])
                try:
                    metadata = self.s3.head_object(Bucket=self._bucket_name, Key=expected_file_name)
                except ClientError:
                    logger.info("Detected a photo folder for upload {}. Setting storage class to DEEP_ARCHIVE".format(file_path))
                    storage_class = 'DEEP_ARCHIVE'
                else:
                    # object exists so print out the datetime
                    logger.info(
                        "Found archive {} that was last uploaded on {} with class: {}. It will not be uploaded again".format(
                            expected_file_name,
                            metadata['LastModified'].isoformat(),
                            metadata['StorageClass']))
                    continue

            logger.info("Calling encrypt and compress with {} and vault {}".format(file_path, vault_name))

            gpg_file_path, gpg_file_name = self.encrypt_and_compress_path(file_path, temp_dir)
            self.write_directory_list_to_file(file_path)
            self.upload_file_to_s3(gpg_file_path, gpg_file_name,
                                   self._bucket_name,
                                   extra_args={'StorageClass': storage_class})

        logger.info("ALL DONE")


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('--temp-dir', type=str, required=True, help='dir to use for scratch space')
    parser.add_argument('--input-file-path', type=str, required=True, help='File containing directory list')
    args = parser.parse_args()

    g = GlacierUploader()

    g.upload_s3_glacier(args)
