import os
import tarfile
import csv
import logging
import datetime
import gnupg

def main(tar_dest_dir, base_dir, force_recreate):
    tar_dest_dir = os.path.join(tar_dest_dir, datetime.date.today().strftime('%Y-%m-%d'))
    logger.info("Create directory for run: {}".format(tar_dest_dir))

    try:
        os.mkdir(tar_dest_dir)
    except FileExistsError:
        pass

    # Init GPG class
    gpg = gnupg.GPG()
    key_to_use = gpg.list_keys()[0] # Assumption is the proper key is the only one here
    fingerprint = key_to_use['fingerprint']
    logger.info("Fingerprint of key is {} and uid is {}".format(fingerprint, key_to_use['uids']))

    with open('file_list.csv', 'r') as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            if row['archive_always'].capitalize() == 'True' or force_recreate:
                input_path = os.path.join(base_dir, row['directory_path'])
                # file name based on dir name
                output_file = '.'.join([os.path.basename(input_path).replace(' ', '_'), 'tar.gz'])
                dest_tar_file = os.path.join(tar_dest_dir, output_file)

                encrypted_output = '.'.join([output_file, 'gpg'])
                dest_gpg_encrypted_output = os.path.join(tar_dest_dir, encrypted_output)

                if not os.path.exists(dest_tar_file):
                    logger.info("Start tarring path: {}. Output path: {}".format(input_path, dest_tar_file))

                    with tarfile.open(dest_tar_file, 'w:gz') as tar:
                        tar.add(input_path)
                    logger.info("Finished path: {}. Output path: {}".format(input_path, dest_tar_file))

                if not os.path.exists(dest_gpg_encrypted_output):
                    logger.info("Start GPG encrypting path: {} Output path: {}".format(dest_tar_file, dest_gpg_encrypted_output))
                    with open(dest_tar_file, 'rb') as tar_file:
                        ret = gpg.encrypt_file(tar_file, output=dest_gpg_encrypted_output, armor=False, recipients=fingerprint)
                    logger.info("{} {} {}".format(ret.ok, ret.status, ret.stderr))
                    logger.info("Finished GPG encrypting path: {}  Output path: {}".format(dest_tar_file, dest_gpg_encrypted_output))


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    base_dir = os.path.join('/mnt', 'raid0')
    tar_dest_dir = os.path.join('/mnt/', 'raid0', 'glacier-tars')
    main(tar_dest_dir, base_dir, force_recreate=True)


