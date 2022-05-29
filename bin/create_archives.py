import argparse
import logging
import os

from glacier_backup.backup_runner import BackupRunner

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s %(name)s.%(funcName)s %(message)s",
    )
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket-name", type=str, help="The bucket to use for backups")
    parser.add_argument(
        "--gpg-key-id", type=str, required=True, help="Fingerprint of the key to use"
    )
    parser.add_argument(
        "--input-file-path",
        type=str,
        required=True,
        help="Local file containing directory list",
    )
    parser.add_argument("--temp-dir", type=str, help="dir to use for scratch space")

    args = parser.parse_args()
    temp_dir = args.temp_dir
    input_path = args.input_file_path

    if not os.path.exists(temp_dir):
        raise ValueError(f"temp dir does not exist, create before running")

    g = BackupRunner(bucket_name=args.bucket_name, temp_dir=temp_dir)

    g.run(input_path, args.gpg_key_id)
