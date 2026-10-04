from unittest import mock

import pytest

from glacier_backup.file_data import FileData


@pytest.mark.parametrize(
    ("file_path", "compressed_file_name", "folder_name"),
    [
        (r"/mnt/raid0/test_folder", "test_folder.tar.gz", "test_folder"),
        (
            r"/mnt/raid0/test folder spaces",
            "test_folder_spaces.tar.gz",
            "test_folder_spaces",
        ),
        (r"/mnt/raid0/test_folder.bz2", "test_folder.bz2", "test_folder.bz2"),
        (
            r"/mnt/raid0/test folder spaces.bz2",
            "test_folder_spaces.bz2",
            "test_folder_spaces.bz2",
        ),
        (r"/mnt/raid0/test_folder.gz", "test_folder.gz", "test_folder.gz"),
    ],
)
def test_file_names(file_path, compressed_file_name, folder_name, tmp_path):
    file_data = FileData(file_path=file_path, work_dir=str(tmp_path))

    assert file_data.compressed_file_name == compressed_file_name
    assert file_data.encrypted_file_name == f"{compressed_file_name}.gpg"
    assert file_data.folder_name == folder_name


@pytest.mark.skip(reason="Need to review later")
@mock.patch("glacier_backup.file_data.tarfile.open")
def test_compress(_):
    file_data = FileData(
        file_path=r"/mnt/raid0/test_folder", work_dir="/mnt/raid1/www/work_dir"
    )

    expected_output_path = "/mnt/raid1/www/work_dir/s3/test_folder.tar.gz"
    result = file_data.compress()
    assert result == expected_output_path


@pytest.mark.skip(reason="Need to review later")
@mock.patch("glacier_backup.file_data.tarfile.open")
def test_compress_compressed_file(_):
    file_data = FileData(
        file_path=r"/mnt/raid0/test_folder.bz2", work_dir="/mnt/raid1/www/work_dir"
    )

    expected_output_path = "/mnt/raid0/test_folder.bz2"
    result = file_data.compress()
    assert result == expected_output_path


@pytest.mark.skip(reason="Need to review later")
@mock.patch("glacier_backup.file_data.GpgUtil")
def test_encrypt_sample_gz(mock_gnupg, *_):
    file_data = FileData(
        file_path=r"/mnt/raid0/test_folder.gz", work_dir="/mnt/raid1/www/work_dir"
    )

    compressed_file_name = "/mnt/raid0/test_folder.bz2"
    _ = file_data.encrypt(compressed_file_name, "my_key_abc")
    mock_gnupg.GPG.return_value.encrypt_file.assert_called_once_with(
        mock.ANY,
        armor=False,
        output="/mnt/raid1/www/work_dir/onedrive/test_folder.bz2.gpg",
        recipients=mock.ANY,
    )
