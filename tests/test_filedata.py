import pytest

from glacier_backup.file_data import (
    FileData,
    UPLOAD_TIME_EVERY_BACKUP,
    UPLOAD_TIME_ONCE,
)


@pytest.mark.parametrize(
    ("file_path", "compressed_file_name", "folder_name", "is_compressed"),
    [
        (r"/mnt/raid0/test_folder", "test_folder.tar.gz", "test_folder", False),
        (
            r"/mnt/raid0/test folder spaces",
            "test_folder_spaces.tar.gz",
            "test_folder_spaces",
            False,
        ),
        (
            r"/mnt/raid0/test_folder.bz2",
            "test_folder.bz2",
            "test_folder.bz2",
            True,
        ),
        (
            r"/mnt/raid0/test folder spaces.bz2",
            "test_folder_spaces.bz2",
            "test_folder_spaces.bz2",
            True,
        ),
        (
            r"/mnt/raid0/test_folder.gz",
            "test_folder.gz",
            "test_folder.gz",
            True,
        ),
    ],
)
def test_file_names(
    file_path, compressed_file_name, folder_name, is_compressed, tmp_path
):
    file_data = FileData(file_path=file_path, work_dir=str(tmp_path))

    assert file_data.compressed_file_name == compressed_file_name
    assert file_data.encrypted_file_name == f"{compressed_file_name}.gpg"
    assert file_data.folder_name == folder_name
    assert file_data.is_compressed is is_compressed


@pytest.mark.parametrize(
    "upload_time", [UPLOAD_TIME_ONCE, UPLOAD_TIME_EVERY_BACKUP, "once", "every_backup"]
)
def test_upload_time_is_case_insensitive(upload_time, tmp_path):
    file_data = FileData(
        file_path="/input/archive",
        work_dir=str(tmp_path),
        upload_time=upload_time,
    )

    assert file_data.upload_time == upload_time


def test_unsupported_upload_time_raises_value_error(tmp_path):
    with pytest.raises(
        ValueError,
        match=r"Path: /input/archive, self\.upload_time='sometimes' not supported",
    ):
        FileData(
            file_path="/input/archive",
            work_dir=str(tmp_path),
            upload_time="sometimes",
        )


def test_init_creates_work_directory(tmp_path):
    work_dir = tmp_path / "nested" / "work"

    FileData(file_path="/input/archive", work_dir=str(work_dir))

    assert work_dir.is_dir()


def test_output_file_path_sets_compressed_and_encrypted_names(tmp_path):
    file_data = FileData(
        file_path="/input/source folder",
        work_dir=str(tmp_path),
        output_file_path="daily_backup",
    )

    assert file_data.compressed_file_name == "daily_backup.tar.gz"
    assert file_data.encrypted_file_name == "daily_backup.tar.gz.gpg"
    assert file_data.dest_tar_file_path == str(tmp_path / "daily_backup.tar.gz")


@pytest.mark.parametrize(
    ("listing_file_name", "expected_name"),
    [("", "source_folder.gz"), ("monthly_listing", "monthly_listing.gz")],
)
def test_listing_file_full_name(listing_file_name, expected_name, tmp_path):
    file_data = FileData(
        file_path="/input/source folder",
        work_dir=str(tmp_path),
        listing_file_name=listing_file_name,
    )

    assert file_data.listing_file_full_name == expected_name
