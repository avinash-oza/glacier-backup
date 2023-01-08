import logging

import gnupg

logger = logging.getLogger(__name__)


class GpgUtil:
    @staticmethod
    def get_key(key):
        # Init GPG class
        gpg = gnupg.GPG()
        try:
            key_data = gpg.list_keys().key_map[key]
        except KeyError:
            raise ValueError(f"Invalid GPG key id passed in:{key}")

        fingerprint = key_data["fingerprint"]
        logger.info(
            "Fingerprint of key is {} and uid is {}".format(
                fingerprint, key_data["uids"]
            )
        )

        return key_data

    @staticmethod
    def encrypt_file(fingerprints, source_path, dest_path):
        with open(dest_path, "rb") as tar_file:
            logger.info(
                f"Start GPG encrypting path: {source_path} Output path: {dest_path}"
            )
            ret = gpg.encrypt_file(
                tar_file, output=dest_path, armor=False, recipients=fingerprints
            )

        if not ret.ok:
            raise RuntimeError(f"Error when encrypting: {ret.stderr}")

        logger.debug(f"Encryption status: {ret.ok} {ret.status} {ret.stderr}")
        logger.info(
            f"Finished GPG encrypting path: {source_path}  Output path: {dest_path}"
        )
