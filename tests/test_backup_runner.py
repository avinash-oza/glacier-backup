import gzip
import logging
import tarfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest

from glacier_backup import backup_runner as backup_runner_module
from glacier_backup.backup_runner import BackupRunner
from glacier_backup.file_data import (
    FileData,
    UPLOAD_TIME_EVERY_BACKUP,
    UPLOAD_TIME_ONCE,
)
from glacier_backup.sns_notification_adapter import NoNotificationAdapter


@pytest.fixture
def notification_adapter():
    return Mock(spec=NoNotificationAdapter)


@pytest.fixture
def runner(tmp_path, notification_adapter):
    backup_runner = BackupRunner(notification_adapter, temp_dir=str(tmp_path))
    notification_adapter.reset_mock()
    return backup_runner


def test_init_creates_listings_directory_and_notifies(tmp_path, notification_adapter):
    runner = BackupRunner(notification_adapter, temp_dir=str(tmp_path))

    assert (tmp_path / "listings").is_dir()
    notification_adapter.set_logger.assert_called_once_with(backup_runner_module.logger)
    notification_adapter.send_notification.assert_called_once_with(
        "Starting backup runner"
    )


def test_load_input_file(runner, tmp_path):

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


@pytest.mark.parametrize(
    ("upload_time", "expected"),
    [(UPLOAD_TIME_ONCE, False), (UPLOAD_TIME_EVERY_BACKUP, True)],
)
def test_should_upload_file(runner, tmp_path, upload_time, expected):
    file_data = FileData(
        file_path="/source/archive",
        work_dir=str(tmp_path),
        upload_time=upload_time,
    )

    assert runner._should_upload_file(file_data) is expected


def test_run_processes_only_scheduled_files(runner, tmp_path, monkeypatch):
    once = FileData(
        file_path="/source/once",
        work_dir=str(tmp_path),
        upload_time=UPLOAD_TIME_ONCE,
    )
    every_backup = FileData(
        file_path="/source/every-backup",
        work_dir=str(tmp_path),
        upload_time=UPLOAD_TIME_EVERY_BACKUP,
    )
    get_key = Mock(return_value={"fingerprint": "FINGERPRINT"})
    monkeypatch.setattr(backup_runner_module.GpgUtil, "get_key", get_key)
    monkeypatch.setattr(
        runner, "_load_input_file", Mock(return_value=[once, every_backup])
    )
    compress = Mock(return_value="every-backup.tar.gz")
    encrypt = Mock()
    create_dir_listing = Mock()
    cleanup = Mock()
    monkeypatch.setattr(runner, "compress", compress)
    monkeypatch.setattr(runner, "encrypt", encrypt)
    monkeypatch.setattr(runner, "create_dir_listing", create_dir_listing)
    monkeypatch.setattr(runner, "cleanup", cleanup)

    runner.run("input.csv", "backup-key")

    get_key.assert_called_once_with("backup-key")
    compress.assert_called_once_with(every_backup)
    encrypt.assert_called_once_with("every-backup.tar.gz", "FINGERPRINT")
    create_dir_listing.assert_called_once_with(every_backup)
    cleanup.assert_called_once_with(every_backup)


def test_compress_creates_archive_for_uncompressed_path(
    runner, tmp_path, notification_adapter
):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "document.txt").write_text("backup contents")
    file_data = FileData(file_path=str(source_dir), work_dir=str(tmp_path))

    archive_path = runner.compress(file_data)

    assert archive_path == str(tmp_path / "source.tar.gz")
    with tarfile.open(archive_path, "r:gz") as archive:
        assert archive.getnames()[-1].endswith("source/document.txt")
    assert notification_adapter.send_notification.call_count == 2


def test_compress_returns_already_compressed_path(
    runner, tmp_path, notification_adapter
):
    source_path = tmp_path / "backup.tar.gz"
    source_path.write_bytes(b"existing archive")
    file_data = FileData(file_path=str(source_path), work_dir=str(tmp_path))

    result = runner.compress(file_data)

    assert result == str(source_path)
    notification_adapter.send_notification.assert_called_once_with(
        f"{source_path} is already compressed. Not compressing again",
        log_level=logging.WARNING,
    )


def test_compress_does_not_overwrite_existing_archive(
    runner, tmp_path, notification_adapter
):
    source_dir = tmp_path / "source"
    file_data = FileData(file_path=str(source_dir), work_dir=str(tmp_path))
    archive_path = Path(file_data.dest_tar_file_path)
    archive_path.write_bytes(b"keep existing archive")

    result = runner.compress(file_data)

    assert result == str(archive_path)
    assert archive_path.read_bytes() == b"keep existing archive"
    notification_adapter.send_notification.assert_called_once_with(
        f"dest_tar_file_path='{archive_path}' already exists. Not compressing again",
        log_level=logging.WARNING,
    )


def test_encrypt_creates_destination_and_calls_gpg(
    runner, tmp_path, notification_adapter, monkeypatch
):
    source_path = tmp_path / "backup.tar.gz"
    encrypt_file = Mock()
    monkeypatch.setattr(backup_runner_module.GpgUtil, "encrypt_file", encrypt_file)

    result = runner.encrypt(str(source_path), "FINGERPRINT")

    destination = str(tmp_path / "backup.tar.gz.gpg")
    assert result == destination
    encrypt_file.assert_called_once_with("FINGERPRINT", str(source_path), destination)
    assert notification_adapter.send_notification.call_args_list == [
        call(
            f"Start GPG encrypting compressed_file_path='{source_path}', "
            f"dest_file_path='{destination}'"
        ),
        call(
            f"Finish GPG encrypting compressed_file_path='{source_path}', "
            f"dest_file_path='{destination}'"
        ),
    ]


def test_encrypt_does_not_overwrite_existing_destination(
    runner, tmp_path, notification_adapter, monkeypatch
):
    source_path = tmp_path / "backup.tar.gz"
    destination_path = tmp_path / "backup.tar.gz.gpg"
    destination_path.write_bytes(b"keep existing encryption")
    encrypt_file = Mock()
    monkeypatch.setattr(backup_runner_module.GpgUtil, "encrypt_file", encrypt_file)

    result = runner.encrypt(str(source_path), "FINGERPRINT")

    assert result == str(destination_path)
    assert destination_path.read_bytes() == b"keep existing encryption"
    encrypt_file.assert_not_called()
    notification_adapter.send_notification.assert_called_once_with(
        f"dest_file_path='{destination_path}' already exists. Not encrypting again",
        log_level=logging.WARNING,
    )


def test_create_dir_listing_returns_none_for_non_directory(runner, tmp_path, caplog):
    file_data = FileData(
        file_path=str(tmp_path / "archive.tar.gz"), work_dir=str(tmp_path)
    )

    with caplog.at_level(logging.WARNING, logger=backup_runner_module.__name__):
        result = runner.create_dir_listing(file_data)

    assert result is None
    assert "Input is not a dir, not creating a dir listing" in caplog.text


def test_create_dir_listing_writes_gzipped_subprocess_output(
    runner, tmp_path, monkeypatch
):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "document.txt").write_text("contents")
    file_data = FileData(
        file_path=str(source_dir),
        work_dir=str(tmp_path),
        listing_file_name="source-listing",
    )
    run = Mock(return_value=SimpleNamespace(stdout=b"8\t/source/document.txt\n"))
    monkeypatch.setattr(backup_runner_module.subprocess, "run", run)

    listing_path = runner.create_dir_listing(file_data)

    assert listing_path == str(tmp_path / "listings" / "source-listing.gz")
    with gzip.open(listing_path, "rb") as listing:
        assert listing.read() == b"8\t/source/document.txt\n"
    run.assert_called_once_with(f"du -ah {source_dir}", shell=True, capture_output=True)


def test_cleanup_preserves_compressed_input(runner, tmp_path):
    source_path = tmp_path / "source.tar.gz"
    source_path.write_bytes(b"keep source archive")
    file_data = FileData(file_path=str(source_path), work_dir=str(tmp_path))

    runner.cleanup(file_data)

    assert source_path.read_bytes() == b"keep source archive"


def test_cleanup_removes_temporary_archive(runner, tmp_path):
    file_data = FileData(file_path="/source/directory", work_dir=str(tmp_path))
    archive_path = Path(file_data.dest_tar_file_path)
    archive_path.write_bytes(b"temporary archive")

    runner.cleanup(file_data)

    assert not archive_path.exists()
