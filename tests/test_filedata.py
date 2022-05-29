import os
from unittest import TestCase, mock

from glacier_backup.file_data import FileData


class FileDataTestCase(TestCase):
    def test_sample_path(self):
        p = r"/mnt/raid0/test_folder"
        work_dir = r"/mnt/raid1/www/work_dir"
        file_type = "DEEP_ARCHIVE"
        fd = FileData(p, file_type, work_dir, "my_listings")
        self.assertEqual(fd.compressed_file_name, "test_folder.tar.gz")
        self.assertEqual(fd.encrypted_file_name, "test_folder.tar.gz.gpg")
        self.assertEqual(fd.storage_class, "DEEP_ARCHIVE")
        self.assertEqual(fd.folder_name, "test_folder")

    def test_sample_path_with_spaces(self):
        p = r"/mnt/raid0/test folder spaces"
        work_dir = r"/mnt/raid1/www/work_dir"
        file_type = "DEEP_ARCHIVE"
        fd = FileData(p, file_type, work_dir, "my_listings")
        self.assertEqual(fd.compressed_file_name, "test_folder_spaces.tar.gz")
        self.assertEqual(fd.encrypted_file_name, "test_folder_spaces.tar.gz.gpg")
        self.assertEqual(fd.storage_class, "DEEP_ARCHIVE")
        self.assertEqual(fd.folder_name, "test_folder_spaces")

    def test_sample_path_bz2(self):
        p = r"/mnt/raid0/test_folder.bz2"
        work_dir = r"/mnt/raid1/www/work_dir"
        file_type = "deep_archive"
        fd = FileData(p, file_type, work_dir, "my_listings")
        self.assertEqual(fd.compressed_file_name, "test_folder.bz2")
        self.assertEqual(fd.encrypted_file_name, "test_folder.bz2.gpg")
        self.assertEqual(fd.storage_class, "DEEP_ARCHIVE")
        self.assertEqual(fd.folder_name, "test_folder.bz2")

    def test_sample_path_bz2_spaces(self):
        p = r"/mnt/raid0/test folder spaces.bz2"
        work_dir = r"/mnt/raid1/www/work_dir"
        file_type = "DEEP_ARCHIVE"
        fd = FileData(p, file_type, work_dir, "my_listings")
        self.assertEqual(fd.compressed_file_name, "test_folder_spaces.bz2")
        self.assertEqual(fd.encrypted_file_name, "test_folder_spaces.bz2.gpg")
        self.assertEqual(fd.storage_class, "DEEP_ARCHIVE")
        self.assertEqual(fd.folder_name, "test_folder_spaces.bz2")

    def test_sample_path_gz(self):
        p = r"/mnt/raid0/test_folder.gz"
        work_dir = r"/mnt/raid1/www/work_dir"
        file_type = "DEEP_ARCHIVE"
        fd = FileData(p, file_type, work_dir, "my_listings")
        self.assertEqual(fd.compressed_file_name, "test_folder.gz")
        self.assertEqual(fd.encrypted_file_name, "test_folder.gz.gpg")
        self.assertEqual(fd.storage_class, "DEEP_ARCHIVE")
        self.assertEqual(fd.folder_name, "test_folder.gz")

    @mock.patch("glacier_backup.file_data.tarfile.open")
    def test_compress(self, _):
        p = r"/mnt/raid0/test_folder"
        work_dir = r"/mnt/raid1/www/work_dir"
        file_type = "DEEP_ARCHIVE"
        fd = FileData(p, file_type, work_dir, "my_listings")

        expected_output_path = "/mnt/raid1/www/work_dir/test_folder.tar.gz"
        res = fd.compress()
        self.assertEqual(res, expected_output_path)

    @mock.patch("glacier_backup.file_data.tarfile.open")
    def test_compress_compressed_file(self, _):
        p = r"/mnt/raid0/test_folder.bz2"
        work_dir = r"/mnt/raid1/www/work_dir"
        file_type = "DEEP_ARCHIVE"
        fd = FileData(p, file_type, work_dir, "my_listings")

        expected_output_path = "/mnt/raid0/test_folder.bz2"
        res = fd.compress()
        self.assertEqual(res, expected_output_path, msg="file should not be copied")

    @mock.patch("glacier_backup.file_data.open")
    @mock.patch("glacier_backup.file_data.gnupg")
    def test_encrypt_sample_gz(self, mock_gnupg, *_):
        p = r"/mnt/raid0/test_folder.gz"
        work_dir = r"/mnt/raid1/www/work_dir"
        file_type = "DEEP_ARCHIVE"
        fd = FileData(p, file_type, work_dir, "my_listings")

        compressed_file_name = "/mnt/raid0/test_folder.bz2"
        _ = fd.encrypt(compressed_file_name, "my_key_abc")
        mock_gnupg.GPG.return_value.encrypt_file.assert_called_once_with(
            mock.ANY,
            armor=False,
            output="/mnt/raid1/www/work_dir/test_folder.bz2.gpg",
            recipients=mock.ANY,
        )
