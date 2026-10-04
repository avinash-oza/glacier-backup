from pathlib import Path

from glacier_backup.backup_runner import BackupRunner
from glacier_backup.file_data import (
    FileData,
    UPLOAD_TIME_EVERY_BACKUP,
    UPLOAD_TIME_ONCE,
)
from glacier_backup.sns_notification_adapter import NoNotificationAdapter


def test_load_input_file(tmp_path):
    runner = BackupRunner(NoNotificationAdapter(), temp_dir=str(tmp_path))

    input_file_list = runner._load_input_file(
        Path(__file__).with_name("test_input_list.csv")
    )

    expected_result = [
        FileData(
            file_path="{DIRECTORY_ROOTS}/Folder1",
            work_dir=str(tmp_path),
            upload_time=UPLOAD_TIME_ONCE,
            listing_file_name="Folder1",
        ),
        FileData(
            file_path="{DIRECTORY_ROOTS}/Folder2",
            work_dir=str(tmp_path),
            upload_time=UPLOAD_TIME_EVERY_BACKUP,
            listing_file_name="Folder2",
        ),
        FileData(
            file_path="{DIRECTORY_ROOTS}/Folder3",
            work_dir=str(tmp_path),
            upload_time=UPLOAD_TIME_EVERY_BACKUP,
            listing_file_name="Folder3",
        ),
        FileData(
            file_path="{DIRECTORY_ROOTS}/Folder4",
            work_dir=str(tmp_path),
            upload_time=UPLOAD_TIME_EVERY_BACKUP,
            listing_file_name="Folder4",
        ),
    ]

    assert input_file_list == expected_result
