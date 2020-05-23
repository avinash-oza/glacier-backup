import argparse
import logging
import tempfile
import boto3
import os

from glacier_backup.archiver import S3Archiver

def get_from_s3_url(s3_url):
    try:
        _, bucket_and_path = s3_url.split('//')
    except:
        logger.exception("Wrong s3 url format detected")
    else:
        bucket, path = bucket_and_path.split('#')
        return bucket, path


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket-name', type=str, default='test-backups-2296778', help='The bucket to use for backups')
    parser.add_argument('--gpg-key-id', type=str, required=True, help='Fingerprint of the key to use')
    parser.add_argument('--input-file-path', type=str, required=True, help='File containing directory list or s3 in the format of s3://BUCKET_NAME#PATH')
    parser.add_argument('--temp-dir', type=str, default=r'/mnt/scratch/', help='dir to use for scratch space (needs to exist)')

    args = parser.parse_args()

    temp_dir = tempfile.TemporaryDirectory(prefix=args.temp_dir)
    logger.info(f"Temp dir is {temp_dir.name}")

    input_path = args.input_file_path

    if input_path.startswith('s3://'):
        logger.info("Start downloading input file from S3")
        bucket, path = get_from_s3_url(args.input_file_path)
        # download the file
        s3 = boto3.client('s3')
        input_path = os.path.join(temp_dir.name, 'INPUT_LIST.CSV')
        s3.download_file(bucket, path, input_path)
        logger.info("Finished downloading input file from S3")


    g = S3Archiver(bucket_name=args.bucket_name, temp_dir=temp_dir.name)

    g.run(input_path, args.gpg_key_id)
