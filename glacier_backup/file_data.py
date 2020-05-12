import argparse
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

logger = logging.getLogger(__name__)

class FileData:
    def __init__(self, folder_or_file_path, work_dir, listings_root_path):
        self._file_path = folder_or_file_path
        self._work_dir = work_dir
        self._listings_root_path = listings_root_path

    @property
    def file_path(self):
        return self._file_path

    @property
    def folder_name(self):
        """
        returns the last part of the path with spaces replaces
        :return:
        """
        return os.path.basename(self._file_path).replace(' ', '_')

    @property
    def listing_file_name(self):
        return os.path.join(self._listings_root_path, '.'.join([self.folder_name, 'gz']))

    @property
    def compressed_file_name(self):
        if self.compressed():
            # keep the compressed file as is
            return self.folder_name

        return '.'.join([self.folder_name, 'tar.gz'])

    def compressed(self):
        return self._file_path.endswith('bz2') or self._file_path.endswith('gz')

    @property
    def compressed_file_full_path(self):
        return os.path.join(self._work_dir, self.compressed_file_name)

    @property
    def encrypted_file_name(self):
        return '.'.join([self.compressed_file_name, 'gpg'])

    @property
    def encrypted_file_full_path(self):
        return os.path.join(self._work_dir, self.encrypted_file_name)

    @property
    def type(self):
        return self._file_type

    @property
    def storage_class(self):
        return 'DEEP_ARCHIVE' if self._file_type == 'photos' else 'GLACIER'

    def compress(self):
        """
        Compresses the file in work_dir and returns the path to compressed file

        :return: str
        """
        logger.info("Start compressing")

        if self.compressed():
            logger.info("Not compressing as file is already compressed")
            return self._file_path

        # only tar when it is a directory and it doesnt exist
        dest_tar_file_path = os.path.join(self._work_dir, self.compressed_file_name)
        if not os.path.exists(dest_tar_file_path):
            logger.info("Start tarring path: {}. Output path: {}".format(self._file_path, dest_tar_file_path))

            with tarfile.open(dest_tar_file_path, 'w:gz') as tar:
                tar.add(self._file_path)
            logger.info("Finished path: {}. Output path: {}".format(self._file_path, dest_tar_file_path))

        return dest_tar_file_path

    def encrypt(self, file_path, key):
        """
        Encrypts the passed in file with key_id
        :param key:
        :param file_path:
        :param work_dir:
        :return: str of encrypted file
        """

        dest_file_name = '.'.join([file_path, 'gpg'])
        dest_file_path = os.path.join(self._work_dir, dest_file_name)

        if os.path.exists(dest_file_path):
            logger.info("Found encrypted version of current file and will not rerun")
            return dest_file_path

        # Init GPG class
        gpg = gnupg.GPG()
        fingerprint = key['fingerprint']
        logger.info("Fingerprint of key is {} and uid is {}".format(fingerprint, key['uids']))
        logger.info("Start GPG encrypting path: {} Output path: {}".format(file_path, dest_file_path))

        with open(file_path, 'rb') as tar_file:
            ret = gpg.encrypt_file(tar_file, output=dest_file_path, armor=False, recipients=fingerprint)

        logger.info("Encryption status: {} {} {}".format(ret.ok, ret.status, ret.stderr))

        logger.info("Finished GPG encrypting path: {}  Output path: {}".format(file_path, dest_file_path))

        return dest_file_path

    def create_dir_listing(self, listing_dir):
        """
        Returns the directory listing

        :return: file path or None
        """

        if not os.path.isdir(self._file_path):
            logger.warning("Input is not a dir, not creating a dir listing")
            return

        listing_file_name = '.'.join([self.folder_name, 'gz'])
        output_file_path = os.path.join(listing_dir, listing_file_name)

        logger.info("Creating file containing the list of files {}".format(output_file_path))

        with gzip.GzipFile(filename=output_file_path, mode='w') as gzipped_listing:
            # with gzip.open(listing_file_path, 'wt') as f:
            result = subprocess.run('du -ah {}'.format(self._file_path), shell=True, capture_output=True)
            gzipped_listing.write(result.stdout)

        logger.info("Done creating file {}".format(output_file_path))
        return output_file_path

# if __name__ == '__main__':
#     import gnupg
#
#     gpg = gnupg.GPG()
#     key_to_use = gpg.list_keys()[0]
#     logging.basicConfig()
#     logger.setLevel(logging.DEBUG)
#
#     # fd = FileData(r'/mnt/scratch/mta_test', 'a', 'a', 'a')
#     fd = FileData(r'/mnt/scratch/mta_test', '/mnt/vm_drives', '/mnt/vm_drives')
#
#     # res = fd.compress(r'/mnt/vm_drives')
#     # res = fd.encrypt(r'/mnt/vm_drives/mta_test.tar.gz', key_to_use)
#     res = fd.create_dir_listing('/mnt/vm_drives')
#
#     logger.warning(res)
