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

class FileData:
    def __init__(self, folder_or_file_path, file_type, work_dir, listings_root_path):
        self._file_path = folder_or_file_path
        # TODO: check this
        self._file_type = file_type
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
        if self._file_path.endswith('bz2') or self._file_path.endswith('gz'):
            # keep the compressed file as is
            return self.folder_name

        return '.'.join([self.folder_name, 'tar.gz'])

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

    def compress(self, work_dir):
        """
        Compresses the file in work_dir and returns the path to compressed file
        :param work_dir:
        :return: str
        """
        pass

    def encrypt(self, file_path, work_dir, key_id):
        """
        Encrypts the passed in file with key_id
        :param key_id:
        :param file_path:
        :param work_dir:
        :return: str of encrypted file
        """
        pass
