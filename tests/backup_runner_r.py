import os.path
import tempfile
import unittest

from glacier_backup.backup_runner import BackupRunner
from glacier_backup.file_data import FileData


class BackupRunnerTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.temp_dir_path = cls.temp_dir.name
        cls.runner = BackupRunner(temp_dir=cls.temp_dir_path)

        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()
        super().tearDownClass()

    def test__load_input_file(self):
        input_file_list = self.runner._load_input_file("test_input_list.csv")

        listings_dir = os.path.join(self.temp_dir_path, "listings")
        expected_result = [
            FileData(
                file_path="{DIRECTORY_ROOTS}/Folder1",
                storage_class="GLACIER",
                work_dir=self.temp_dir_path,
                listings_root_path=listings_dir,
            ),
            FileData(
                file_path="{DIRECTORY_ROOTS}/Folder2",
                storage_class="GLACIER",
                work_dir=self.temp_dir_path,
                listings_root_path=listings_dir,
            ),
            FileData(
                file_path="{DIRECTORY_ROOTS}/Folder3",
                storage_class="GLACIER",
                work_dir=self.temp_dir_path,
                listings_root_path=listings_dir,
            ),
            FileData(
                file_path="{DIRECTORY_ROOTS}/Folder4",
                storage_class="GLACIER",
                work_dir=self.temp_dir_path,
                listings_root_path=listings_dir,
            ),
        ]

        self.assertEqual(input_file_list, expected_result)
