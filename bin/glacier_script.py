import argparse
import logging
import tempfile

from glacier_backup.glacier_uploader import GlacierUploader


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket-name', type=str, default='test-backups-2296778', help='The bucket to use for backups')
    parser.add_argument('--gpg-key-id', type=str, required=True, help='Fingerprint of the key to use')
    parser.add_argument('--input-file-path', type=str, required=True, help='File containing directory list')
    parser.add_argument('--temp-dir', type=str, default=r'/mnt/scratch/', help='dir to use for scratch space (needs to exist)')

    args = parser.parse_args()

    temp_dir = tempfile.TemporaryDirectory(prefix=args.temp_dir)
    logger.info(f"Temp dir is {temp_dir}")
    g = GlacierUploader(bucket_name=args.bucket_name, temp_dir=temp_dir.name)

    g.run(args.input_file_path, args.gpg_key_id)
