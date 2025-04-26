import gzip
import logging
import os
import posixpath as os_path
import subprocess
from dataclasses import dataclass

from glacier_backup.gpg_util import GpgUtil

logger = logging.getLogger(__name__)

UPLOAD_TIME_ONCE = "ONCE"
UPLOAD_TIME_EVERY_BACKUP = "EVERY_BACKUP"


@dataclass(kw_only=True)
class FileData:
    SUPPORTED_UPLOAD_TIMES = [UPLOAD_TIME_ONCE, UPLOAD_TIME_EVERY_BACKUP]

    file_path: str
    work_dir: str
    listings_root_path: str
    output_file_path: str = ""
    upload_time: str = UPLOAD_TIME_EVERY_BACKUP

    def __post_init__(self):

        if self.upload_time.upper() not in self.SUPPORTED_UPLOAD_TIMES:
            raise ValueError(
                f"Path: {self.file_path}, {self.upload_time=} not supported"
            )

        os.makedirs(self.work_dir, exist_ok=True)

    @property
    def folder_name(self):
        """
        returns the last part of the path with spaces replaces
        :return:
        """
        return os_path.basename(self.file_path).replace(" ", "_")

    @property
    def compressed_file_name(self):
        if self.is_compressed():
            # keep the compressed file as is
            return self.folder_name
        if self.output_file_path:
            return ".".join([self.output_file_path, "tar.gz"])

        return ".".join([self.folder_name, "tar.gz"])

    def is_compressed(self):
        return self.file_path.endswith("bz2") or self.file_path.endswith("gz")

    @property
    def encrypted_file_name(self):
        return ".".join([self.compressed_file_name, "gpg"])

    def get_dest_tar_file_path(self):
        dest_tar_file_path = os_path.join(self.work_dir, self.compressed_file_name)
        return dest_tar_file_path

    def encrypt(self, file_path, fingerprint):
        """
        Encrypts the passed in file with key_id
        :param str fingerprint: key to use for encryption
        :param file_path: the file path to encrypt
        :return: full path of the encrypted file
        """

        dest_file_name = ".".join([os_path.basename(file_path), "gpg"])
        dest_file_path = os_path.join(self.work_dir, dest_file_name)

        if os_path.exists(dest_file_path):
            logger.warning(
                f"Encrypted path: {dest_file_path} already exists. Not encrypting again"
            )
            return dest_file_path

        logger.info(
            f"Start GPG encrypting path: {file_path} Output path: {dest_file_path}"
        )
        GpgUtil().encrypt_file(fingerprint, file_path, dest_file_path)

        message = (
            f"Finished GPG encrypting path: {file_path}  Output path: {dest_file_path}"
        )
        logger.info(message)

        return dest_file_path

    def create_dir_listing(self, listing_dir):
        """
        Returns the directory listing

        :return: file path or None
        """

        if not os_path.isdir(self.file_path):
            logger.warning("Input is not a dir, not creating a dir listing")
            return

        listing_file_name = ".".join([self.folder_name, "gz"])
        output_file_path = os_path.join(listing_dir, listing_file_name)

        logger.info(
            "Creating file containing the list of files {}".format(output_file_path)
        )

        with gzip.GzipFile(filename=output_file_path, mode="w") as gzipped_listing:
            result = subprocess.run(
                "du -ah {}".format(self.file_path), shell=True, capture_output=True
            )
            gzipped_listing.write(result.stdout)

        logger.info("Done creating file {}".format(output_file_path))
        return output_file_path

    def cleanup(self):
        """
        Cleans up any temporary files created
        :return:
        """
        if self.is_compressed():
            # do not remove the original input archive file
            return

        dest_tar_file_path = self.get_dest_tar_file_path()
        logger.info(f"Removing {dest_tar_file_path}")
        os.remove(dest_tar_file_path)
