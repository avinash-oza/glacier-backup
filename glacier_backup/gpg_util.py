import logging

import gnupg

logger = logging.getLogger(__name__)

KEYSERVER = "hkp://keyserver.ubuntu.com"


class GpgUtil:
    @staticmethod
    def get_key(key):
        key = key.upper()
        # Init GPG class
        gpg = gnupg.GPG()

        result = gpg.recv_keys(KEYSERVER, key)
        if result.count == 0:
            raise ValueError(f"Could not get any keys for:{key}")

        key_data = result.results[0]

        fingerprint = key_data["fingerprint"]

        gpg.trust_keys(fingerprint, "TRUST_FULLY")

        logger.info(
            "Fingerprint of key is {} and uid is {}".format(
                fingerprint, key_data["uids"]
            )
        )

        return key_data

    @staticmethod
    def encrypt_file(fingerprints, source_path, dest_path):
        gpg = gnupg.GPG()

        with open(source_path, "rb") as tar_file:
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
