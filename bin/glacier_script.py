import argparse
import logging

from glacier_backup.glacier_uploader import GlacierUploader

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('--temp-dir', type=str, required=True, help='dir to use for scratch space')
    parser.add_argument('--input-file-path', type=str, required=True, help='File containing directory list')
    args = parser.parse_args()

    g = GlacierUploader(bucket_name='abcd', temp_dir=args.temp_dir)

    g.upload_s3_glacier(args.input_file_path)
