import logging

logger = logging.getLogger(__name__)


class DummyS3Client:
    def __init__(self, bucket_name):
        pass

    def upload_file(self, bucket_key, upload_file_path, extra_args=None):
        logger.warning(
            f"DummyS3Client called for bucket_key={bucket_key}. No file will actually be uploaded"
        )

    def get_file_metadata(self, key):
        """
        Returns None or dict
        :param key: key to get
        :return:
        """
        return
