from unittest import TestCase

from bin.glacier_script import FileData


class FileDataTestCase(TestCase):


    def test_sample_path(self):
        p = r'/mnt/raid0/test_folder'
        work_dir = r'/mnt/raid1/www/work_dir'
        file_type = 'photos'
        fd = FileData(p, file_type, work_dir)
        self.assertEqual(fd.compressed_file_name, 'test_folder.tar.gz')
        self.assertEqual(fd.encrypted_file_name, 'test_folder.tar.gz.gpg')
        self.assertEqual(fd.storage_class, 'DEEP_ARCHIVE')
        self.assertEqual(fd.type, file_type)

    def test_sample_path_bz2(self):
        p = r'/mnt/raid0/test_folder.bz2'
        work_dir = r'/mnt/raid1/www/work_dir'
        file_type = 'photos'
        fd = FileData(p, file_type, work_dir)
        self.assertEqual(fd.compressed_file_name, 'test_folder.bz2')
        self.assertEqual(fd.encrypted_file_name, 'test_folder.bz2.gpg')
        self.assertEqual(fd.storage_class, 'DEEP_ARCHIVE')
        self.assertEqual(fd.type, file_type)

    def test_sample_path_gz(self):
        p = r'/mnt/raid0/test_folder.gz'
        work_dir = r'/mnt/raid1/www/work_dir'
        file_type = 'photos'
        fd = FileData(p, file_type, work_dir)
        self.assertEqual(fd.compressed_file_name, 'test_folder.gz')
        self.assertEqual(fd.encrypted_file_name, 'test_folder.gz.gpg')
        self.assertEqual(fd.storage_class, 'DEEP_ARCHIVE')
        self.assertEqual(fd.type, file_type)