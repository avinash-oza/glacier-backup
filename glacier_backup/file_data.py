import gzip
import logging
import os
import posixpath as os_path
import subprocess
import tarfile

import gnupg

from gpg_util import GpgUtil

logger = logging.getLogger(__name__)


class FileData:
    SUPPORTED_STORAGE_CLASSES = ["GLACIER", "DEEP_ARCHIVE", "STANDARD"]
    SUPPORTED_STORAGE_PROVIDERS = ["S3", "ONEDRIVE"]

    def __init__(
        self,
        folder_or_file_path,
        storage_class,
        work_dir,
        listings_root_path,
        storage_provider="S3",
    ):
        if storage_class.upper() not in self.SUPPORTED_STORAGE_CLASSES:
            raise ValueError(
                f"Path: {folder_or_file_path}, storage class: {storage_class} not supported"
            )

        if storage_provider.upper() not in self.SUPPORTED_STORAGE_PROVIDERS:
            raise ValueError(
                f"Path: {folder_or_file_path}, storage provider: {storage_provider} not supported"
            )

        self._file_path = folder_or_file_path
        self._storage_class = storage_class.upper()
        self._storage_provider = storage_provider.upper()
        self._listings_root_path = listings_root_path
        self._work_dir = os_path.join(work_dir, self._storage_provider.lower())
        os.makedirs(self._work_dir, exist_ok=True)

    @property
    def file_path(self):
        return self._file_path

    @property
    def folder_name(self):
        """
        returns the last part of the path with spaces replaces
        :return:
        """
        return os_path.basename(self._file_path).replace(" ", "_")

    @property
    def compressed_file_name(self):
        if self.is_compressed():
            # keep the compressed file as is
            return self.folder_name

        return ".".join([self.folder_name, "tar.gz"])

    def is_compressed(self):
        return self._file_path.endswith("bz2") or self._file_path.endswith("gz")

    @property
    def encrypted_file_name(self):
        return ".".join([self.compressed_file_name, "gpg"])

    @property
    def storage_class(self):
        return self._storage_class.upper()

    def compress(self):
        """
        Compresses the file in work_dir and returns the path to compressed file

        :return: path to the compressed file
        """
        if self.is_compressed():
            logger.warning(
                f"{self.file_path} is already compressed. Not compressing again"
            )
            return self._file_path

        # only tar when it is a directory and it doesnt exist
        dest_tar_file_path = self._get_dest_tar_file_path()
        if not os_path.exists(dest_tar_file_path):
            logger.info(
                f"Start compressing path: {self._file_path}. Output path: {dest_tar_file_path}"
            )

            with tarfile.open(dest_tar_file_path, "w:gz") as tar:
                tar.add(self._file_path)
            logger.info(
                "Finished path: {}. Output path: {}".format(
                    self._file_path, dest_tar_file_path
                )
            )
        else:
            logger.warning(
                f"Compressed path: {dest_tar_file_path} already exists. Not compressing again"
            )

        return dest_tar_file_path

    def _get_dest_tar_file_path(self):
        dest_tar_file_path = os_path.join(self._work_dir, self.compressed_file_name)
        return dest_tar_file_path

    def encrypt(self, file_path, key):
        """
        Encrypts the passed in file with key_id
        :param str key: key to use for encryption
        :param file_path: the file path to encrypt
        :return: full path of the encrypted file
        """
        key = key.upper()

        dest_file_name = ".".join([os_path.basename(file_path), "gpg"])
        dest_file_path = os_path.join(self._work_dir, dest_file_name)

        if os_path.exists(dest_file_path):
            logger.warning(
                f"Encrypted path: {dest_file_path} already exists. Not encrypting again"
            )
            return dest_file_path

        GpgUtil().encrypt_file(key, file_path, dest_file_path)

        # # Init GPG class
        # gpg = gnupg.GPG()
        # try:
        #     key_data = gpg.list_keys().key_map[key]
        # except KeyError:
        #     raise ValueError(f"Invalid GPG key id passed in:{key}")
        #
        # fingerprint = key_data["fingerprint"]
        # logger.info(
        #     "Fingerprint of key is {} and uid is {}".format(
        #         fingerprint, key_data["uids"]
        #     )
        # )

        # with open(file_path, "rb") as tar_file:
        #     logger.info(
        #         f"Start GPG encrypting path: {file_path} Output path: {dest_file_path}"
        #     )
        #     ret = gpg.encrypt_file(
        #         tar_file, output=dest_file_path, armor=False, recipients=fingerprint
        #     )
        #
        # if not ret.ok:
        #     raise RuntimeError(f"Error when encrypting: {ret.stderr}")

        # logger.debug(f"Encryption status: {ret.ok} {ret.status} {ret.stderr}")
        # logger.info(
        #     f"Finished GPG encrypting path: {file_path}  Output path: {dest_file_path}"
        # )

        return dest_file_path

    def create_dir_listing(self, listing_dir):
        """
        Returns the directory listing

        :return: file path or None
        """

        if not os_path.isdir(self._file_path):
            logger.warning("Input is not a dir, not creating a dir listing")
            return

        listing_file_name = ".".join([self.folder_name, "gz"])
        output_file_path = os_path.join(listing_dir, listing_file_name)

        logger.info(
            "Creating file containing the list of files {}".format(output_file_path)
        )

        with gzip.GzipFile(filename=output_file_path, mode="w") as gzipped_listing:
            result = subprocess.run(
                "du -ah {}".format(self._file_path), shell=True, capture_output=True
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

        dest_tar_file_path = self._get_dest_tar_file_path()
        logger.info(f"Removing {dest_tar_file_path}")
        os.remove(dest_tar_file_path)
