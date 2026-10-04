import logging
import subprocess

import gnupg

logger = logging.getLogger(__name__)

KEYSERVER = "hkp://keyserver.ubuntu.com"
# Init GPG class
gpg = gnupg.GPG()


class GpgUtil:
    @staticmethod
    def update_key_expiry(key_id, keyserver=KEYSERVER):
        key_id = key_id.strip().upper()
        if not key_id or key_id.startswith("-"):
            raise ValueError("Invalid GPG key ID")
        keyserver = keyserver.strip()
        if not keyserver:
            raise ValueError("Keyserver must not be empty")

        keys = gpg.list_keys(secret=True, keys=key_id)
        if len(keys) != 1:
            raise ValueError(
                f"Expected one secret key matching {key_id}, found {len(keys)}"
            )

        key_data = keys[0]
        subkeys = key_data.get("subkeys", [])
        if not subkeys:
            raise ValueError(f"No subkeys found for GPG key: {key_id}")

        first_subkey_id = subkeys[0][0]
        subkey_fingerprint = (
            key_data.get("subkey_info", {}).get(first_subkey_id, {}).get("fingerprint")
        )
        fingerprint = key_data.get("fingerprint")
        if not fingerprint or not subkey_fingerprint:
            raise ValueError(f"Could not resolve key fingerprints for: {key_id}")

        commands = [
            ["gpg", "--quick-set-expire", fingerprint, "1y"],
            ["gpg", "--quick-set-expire", fingerprint, "1y", subkey_fingerprint],
        ]
        try:
            for command in commands:
                subprocess.run(command, check=True)
        except subprocess.CalledProcessError as error:
            raise RuntimeError(
                f"GPG failed to update key expiration (exit status {error.returncode})"
            ) from error

        result = gpg.send_keys(keyserver, fingerprint)
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to upload updated key to {keyserver} "
                f"(exit status {result.returncode}): {result.stderr.strip()}"
            )

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
