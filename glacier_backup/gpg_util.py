import logging

import gnupg

logger = logging.getLogger(__name__)

KEYSERVER = "hkp://keyserver.ubuntu.com"
# Init GPG class
gpg = gnupg.GPG()


class GpgUtil:
    @staticmethod
    def get_key(key):
        key = key.upper()

        result = gpg.recv_keys(KEYSERVER, key)
        if result.count == 0:
            raise ValueError(f"Could not get any keys for:{key}")

        key_data = gpg.list_keys(keys=key).key_map[key]

        fingerprint = key_data["fingerprint"]

        gpg.trust_keys(fingerprint, "TRUST_ULTIMATE")

        logger.info(
            "Fingerprint of key is {} and uid is {}".format(
                fingerprint, key_data["uids"]
            )
        )

        return key_data

    @staticmethod
    def encrypt_file(fingerprints, source_path, dest_path):
        with open(source_path, "rb") as tar_file:
            ret = gpg.encrypt_file(
                tar_file, output=dest_path, armor=False, recipients=fingerprints
            )

        if not ret.ok:
            raise RuntimeError(
                f"Error when encrypting: {ret.stderr}, status={ret.status}"
            )

        logger.debug(f"Encryption status: {ret.ok} {ret.status} {ret.stderr}")
