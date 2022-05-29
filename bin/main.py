import argparse
import logging
import boto3
import os

from glacier_backup.dummy_s3_client import DummyS3Client
from glacier_backup.backup_runner import BackupRunner


def get_from_s3_url(s3_url):
    try:
        _, bucket_and_path = s3_url.split("//")
    except:
        logger.exception("Wrong s3 url format detected")
    else:
        bucket, path = bucket_and_path.split("#")
        return bucket, path


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
        help="File containing directory list or s3 in the format of s3://BUCKET_NAME#PATH",
    )
    parser.add_argument("--temp-dir", type=str, help="dir to use for scratch space")
    parser.add_argument(
        "--skip-upload",
        action="store_true",
        help="Do not actually upload files, only compress",
    )

    args = parser.parse_args()
    temp_dir = args.temp_dir
    input_path = args.input_file_path
    skip_upload = args.skip_upload

    upload_client = None  # use default
    if skip_upload:
        logger.info(f"Will not upload file after compressing")
        upload_client = DummyS3Client(None)

    if not os.path.exists(temp_dir):
        raise ValueError(f"temp dir does not exist, create before running")

    if input_path.startswith("s3://"):
        logger.info("Start downloading input file from S3")
        bucket, path = get_from_s3_url(args.input_file_path)
        # download the file
        s3 = boto3.client("s3")
        input_path = os.path.join(temp_dir, "INPUT_LIST.CSV")
        s3.download_file(bucket, path, input_path)
        logger.info("Finished downloading input file from S3")

    g = BackupRunner(bucket_name=args.bucket_name, temp_dir=temp_dir)

    g.run(input_path, args.gpg_key_id)
