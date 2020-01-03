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
    def __init__(self, file_path, file_type, work_dir):
        self._file_path = file_path
        # TODO: check this
        self._file_type = file_type
        self._work_dir = work_dir

    @property
    def file_path(self):
        return self._file_path

    @property
    def compressed_file_name(self):
        if self._file_path.endswith('bz2') or self._file_path.endswith('gz'):
            # keep the compressed file as is
            return os.path.basename(self._file_path).replace(' ', '_')

        return '.'.join([os.path.basename(self._file_path).replace(' ', '_'), 'tar.gz'])

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