import boto3
import logging
import binascii
import hashlib
import os
import math
import collections

logger = logging.getLogger(__name__)

GlacierResult = collections.namedtuple('GlacierResult', 'vault_name multipart successful tree_hash archive_id location upload_id')

class GlacierUploader:
    def __init__(self):
        self.glacier = boto3.client('glacier')
        self.vaults = {one_vault['VaultName'] : one_vault for one_vault in self.glacier.list_vaults()['VaultList']}

    def get_vault_list(self):
        return self.vaults


    def _calculate_tree_hash(self, part, part_size):
        """
        This code is lifted directly from https://github.com/tbumi/glacier-upload/blob/develop/src/glacier_upload/upload.py
        :param part:
        :param part_size:
        :return:
        """
        checksums = []
        upper_bound = min(len(part), part_size)
        step = 1024 * 1024  # 1 MB
        for chunk_pos in range(0, upper_bound, step):
            chunk = part[chunk_pos:chunk_pos+step]
            checksums.append(hashlib.sha256(chunk).hexdigest())
            del chunk
        return self._calculate_total_tree_hash(checksums)


    def _calculate_total_tree_hash(self, list_of_checksums):
        tree = list_of_checksums[:]
        while len(tree) > 1:
            parent = []
            for i in range(0, len(tree), 2):
                if i < len(tree) - 1:
                    part1 = binascii.unhexlify(tree[i])
                    part2 = binascii.unhexlify(tree[i + 1])
                    parent.append(hashlib.sha256(part1 + part2).hexdigest())
                else:
                    parent.append(tree[i])
            tree = parent
        return tree[0]

    def upload(self, vault_name, file_path, archive_description, part_size, num_threads=1, resume_upload_id=None):
        """
        Takes a file path and uploads it to a vault No compression, etc is handled here
        :param vault_name: name of vault to upload to
        :param file_path: path to a file. This cannot be a directory
        :param archive_description: a description to add for the archive
        :param part_size: part size in mb to use
        :param num_threads:
        :param resume_upload_id:
        :return:
        """
        if num_threads != 1:
            raise ValueError("Currently only 1 thread is supported for upload")
        if not os.path.isfile(file_path):
            raise ValueError('Only file type inputs are supported')
        if not math.log2(part_size).is_integer():
            raise ValueError('part-size must be a power of 2')

        logger.info("Starting to process {}")

        # Determine if this needs a multipart upload of not
        part_size = part_size * 1024 * 1024

        file_handle = open(file_path, 'rb')
        file_size = file_handle.seek(0, 2)

        if file_size < 4096:
            logger.info("Starting to upload file in one request")

            return self._upload_single_part(archive_description, file_handle, file_path, vault_name)

        # Handle multipart uploads here

    def _upload_single_part(self, archive_description, file_handle, file_path, vault_name):
        try:
            response = self.glacier.upload_archive(vaultName=vault_name,
                                                   archiveDescription=archive_description,
                                                   body=file_handle)
        except Exception as e:
            logger.exception("Exception occured while uploading file {}".format(file_path))
            return GlacierResult(vault_name, multipart=False,
                                 successful=False, tree_hash=None,
                                 archive_id=None,
                                 location=None, upload_id=None)
        else:
            logger.info(
                "Successfully uploaded single file. Tree hash is {}, location is {} and archive_id is {}".format(
                    response['checksum'],
                    response['location'],
                    response['archiveId']))
            return GlacierResult(vault_name, multipart=False,
                                 successful=True, tree_hash=response['checksum'],
                                 archive_id=response['archiveId'],
                                 location=['location'], upload_id=None)






