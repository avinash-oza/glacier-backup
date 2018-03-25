import os
import tarfile
import csv
import logging
import datetime

def main(tar_dest_dir, base_dir, force_recreate):
    tar_dest_dir = os.path.join(tar_dest_dir, datetime.date.today().strftime('%Y-%m-%d'))
    logger.info("Create directory for run: {}".format(tar_dest_dir))

    try:
        os.mkdir(tar_dest_dir)
    except FileExistsError:
        pass

    with open('file_list.csv', 'r') as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            if row['archive_always'].capitalize() == 'True' or force_recreate:
                input_path = os.path.join(base_dir, row['directory_path'])
                # file name based on dir name
                output_file = '.'.join([os.path.basename(input_path).replace(' ', '_'), 'tar.gz'])
                dest_tar_file = os.path.join(tar_dest_dir, output_file)

                if not os.path.exists(dest_tar_file):
                    logger.info("Start tarring path: {}. Output path: {}".format(input_path, dest_tar_file))

                    with tarfile.open(dest_tar_file, 'w:gz') as tar:
                        tar.add(input_path)
                    logger.info("Finished path: {}. Output path: {}".format(input_path, dest_tar_file))


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    base_dir = os.path.join('/mnt', 'raid0')
    tar_dest_dir = os.path.join('/mnt/', 'raid0', 'glacier-tars')
    main(tar_dest_dir, base_dir, force_recreate=False)


