import argparse
import csv
import datetime
import logging
import math
import os
import tarfile

import boto3
import gnupg
from dateutil.parser import parse
from glacier_upload.upload import GlacierUploadException, upload

# input_file_fields = [file_path,vault_name,type]
field_names = ['file_path','type','dest_file_name','dest_file_path','vault_name','timestamp_uploaded','archive_id']

def write_line_to_archive(archive_file_path, **kwargs):
    """
    kwargs should be field_names
    :param archive_file_path:
    :param kwargs:
    :return:
    """
    kwargs['timestamp_uploaded'] = datetime.datetime.utcnow().isoformat() +'Z'
    with open(archive_file_path, 'a') as f:
        writer = csv.DictWriter(f, field_names, dialect=csv.unix_dialect)
        writer.writerow(kwargs)
        logger.info("Wrote {} to archive log {}".format(kwargs, archive_file_path))



def encrypt_and_compress_path(file_path, temp_dir):
    tar_dest_dir = os.path.join(temp_dir, datetime.date.today().strftime('%Y-%m-%d'))
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

    # set up tarred output file
    output_file = '.'.join([os.path.basename(file_path).replace(' ', '_'), 'tar.gz'])
    dest_tar_file = os.path.join(tar_dest_dir, output_file)
    logger.info("Output tar file is {}".format(dest_tar_file))

    if not os.path.exists(dest_tar_file):
        logger.info("Start tarring path: {}. Output path: {}".format(file_path, dest_tar_file))

        with tarfile.open(dest_tar_file, 'w:gz') as tar:
            tar.add(file_path)
        logger.info("Finished path: {}. Output path: {}".format(file_path, dest_tar_file))

    # setup encrypted file path
    encrypted_output = '.'.join([output_file, 'gpg'])
    dest_gpg_encrypted_output = os.path.join(tar_dest_dir, encrypted_output)
    logger.info("Start GPG encrypting path: {} Output path: {}".format(dest_tar_file, dest_gpg_encrypted_output))

    if not os.path.exists(dest_gpg_encrypted_output):
        logger.info("Start GPG encrypting path: {} Output path: {}".format(dest_tar_file, dest_gpg_encrypted_output))
        with open(dest_tar_file, 'rb') as tar_file:
            ret = gpg.encrypt_file(tar_file, output=dest_gpg_encrypted_output, armor=False, recipients=fingerprint)
        logger.info("{} {} {}".format(ret.ok, ret.status, ret.stderr))
        logger.info("Finished GPG encrypting path: {}  Output path: {}".format(dest_tar_file, dest_gpg_encrypted_output))

    return dest_gpg_encrypted_output, encrypted_output

def get_or_create_multipart_upload(file_path, vault_name, part_size_bytes):
    """
    returns the archive id if it exists already
    :param file_path:
    :param vault_name:
    :param part_size_bytes:
    :return:
    """
    glacier = boto3.client('glacier')

    # check to see the current pending uploads
    pending_uploads = glacier.list_multipart_uploads(vaultName=vault_name)
    if pending_uploads['UploadsList']:
        # check to see if any are the current file we are upload
        for one_upload in pending_uploads['UploadsList']:
            if one_upload['ArchiveDescription'] == file_path:
                upload_id = one_upload['MultipartUploadId']
                logger.info("Found pending upload id {}".format(upload_id))
                return upload_id

    # create new multipart id
    response = glacier.initiate_multipart_upload(
        vaultName=vault_name,
        archiveDescription=file_path,
        partSize=str(part_size_bytes)
    )
    upload_id = response['uploadId']
    logger.info("Multipart upload id {} created for {}".format(upload_id, file_path))

    return upload_id

def upload_file_to_glacier(file_path, vault_name, part_size=1024, threads=1):
    """
    uploads the encrypted file to glacier
    :param file_path: full path to file
    :param vault_name:vault to upload to
    :param threads:
    :param part_size:
    :return:
    """

    logger.info("Start glacier upload for file: {}".format(file_path))
    # following block copied from tbumi
    if not math.log2(part_size).is_integer():
        raise ValueError('part-size must be a power of 2')
    if part_size < 1 or part_size > 4096:
        raise ValueError('part-size must be more than 1 MB '
                         'and less than 4096 MB')

    with open(file_path, 'rb') as file_to_upload:
        file_size = file_to_upload.seek(0, 2)

    part_size_bytes = part_size * 1024 * 1024
    # end copied block

    if file_size < 4096:
        # dont generate upload_id before sending (let the script do it)
        logger.info("File size is less than 4mb. Passing directly to upload script.")
        upload_id = None
    else:
        # use the file name as the archive description and generate here so that we can keep track of it
        upload_id = get_or_create_multipart_upload(file_path, vault_name, part_size_bytes)

    for t in range(5):
        logger.info("Starting glacier upload script for {}".format(file_path))
        try:
            result = upload(vault_name, file_name=[file_path], region=None, arc_desc=file_path, part_size=part_size, num_threads=threads, upload_id=upload_id)
        except GlacierUploadException as e:
            # set the upload id to use for resuming
            logger.error("Retrying upload for {} with upload_id {}".format(file_path, upload_id))
        else:
            logger.info("Successfully uploaded {} and archive_id is {} and location {}".format(file_path, result['archiveId'], result['location']))
            return result

def get_list_of_files_to_upload(input_file_path, archive_log_path):
    """
    Returns a list of files that should be uploaded. type=photo will be ignored if its already in
    the archive (as these only need to be archived once)
    :param str input_file_path:
    :param str archive_log_path:
    :return: list of dicts
    """
    archive_dict = {}
    input_file_dict = {}

    with open(archive_log_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row['timestamp_uploaded'] = parse(row['timestamp_uploaded'])
            #TODO: handle updating dict when multiple entries of same file exists
            archive_dict[row['source_dir']] = row

    logger.info("Loaded {} prior archived entries".format(len(archive_dict)))

    # read the input files and only add those that we don't have already
    with open(input_file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            file_path = row['file_path']
            dir_type = row['type'] # photos/data

            if file_path in archive_dict and dir_type == 'photos':
                # add additional checks for when to add file in
                logger.warning("Path {} already exists with last uploaded date as {}".format(file_path, archive_dict[file_path]['timestamp_uploaded']))
                continue

            input_file_dict[file_path] = row

    logger.info("Loaded {} directories to upload".format(len(input_file_dict)))

    return input_file_dict

def main(args):
    input_file_path = args.input_file_path
    archive_log_path = args.archive_log_path
    temp_dir = args.temp_dir

    directories_to_upload = get_list_of_files_to_upload(input_file_path, archive_log_path)
    for key, values_dict in directories_to_upload.items():
        vault_name = values_dict['vault_name']
        file_path = values_dict['file_path']
        dir_type = values_dict['type']

        logger.info("Calling encrypt and compress with {} and vault {}".format(file_path, vault_name))

        gpg_file_path, gpg_file_name = encrypt_and_compress_path(file_path, temp_dir)
        result = upload_file_to_glacier(gpg_file_path, vault_name)

        write_line_to_archive(archive_log_path, file_path=file_path, type=dir_type,
                              dest_file_name=gpg_file_name, dest_file_path=gpg_file_path,
                              vault_name=vault_name, archive_id=result['archiveId'])
    logger.info("ALL DONE")


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', type=str, help='Dir to tar')
    parser.add_argument('--temp-dir', type=str, help='scratch space')

    parser.add_argument('--archive-log-path', type=str, help='File to output archive ids to ')
    parser.add_argument('--input-file-path', type=str, help='File containing directory list')
    args = parser.parse_args()

    main(args)

    # write_line_to_archive(args.archive_log_path, source_dir='abcd', type='type1', dest_file_path='dest_file_path', dest_file_name='FILENAME',
    #                       vault_name='my_vault', archive_id='MY_BIG_ID')
