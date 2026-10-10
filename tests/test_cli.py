import datetime
import time
from unittest.mock import Mock, call

from botocore.exceptions import ClientError, NoCredentialsError, ProfileNotFound
from click.testing import CliRunner
import pytest

from glacier_backup import cli as cli_module
from glacier_backup.gpg_util import KEYSERVER


@pytest.mark.parametrize(
    ("options", "expected_keyserver"),
    [
        ([], KEYSERVER),
        (["--keyserver", "hkps://keys.example"], "hkps://keys.example"),
    ],
)
def test_update_gpg_expiry_command_invokes_gpg_util(
    monkeypatch, options, expected_keyserver
):
    update_key_expiry = Mock()
    monkeypatch.setattr(cli_module.GpgUtil, "update_key_expiry", update_key_expiry)

    result = CliRunner().invoke(
        cli_module.cli, ["update-gpg-expiry", "ABC123", *options]
    )

    assert result.exit_code == 0
    update_key_expiry.assert_called_once_with("ABC123", expected_keyserver)
    assert f"uploaded ABC123 to {expected_keyserver}" in result.output


@pytest.fixture
def backup_csv(tmp_path):
    input_file = tmp_path / "backups.csv"
    input_file.write_text(
        "file_path,upload_time,output_file_path\n"
        "/input/source folder,EVERY_BACKUP,\n"
        "/input/other,ONCE,custom\n"
    )
    return input_file


@pytest.fixture
def s3_client(monkeypatch):
    client = Mock()
    client.head_object.return_value = {
        "LastModified": datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    }
    monkeypatch.setattr(cli_module.boto3, "client", Mock(return_value=client))
    session = Mock()
    session.client.return_value = client
    monkeypatch.setattr(cli_module.boto3, "Session", Mock(return_value=session))
    return client


@pytest.mark.parametrize("prefix", ["s3://bucket/backups", "s3://bucket/backups/"])
def test_get_newest_backup_file_reports_timestamps(backup_csv, s3_client, prefix):
    older = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    newer = datetime.datetime(
        2026, 1, 2, 3, tzinfo=datetime.timezone(datetime.timedelta(hours=3))
    )
    s3_client.head_object.side_effect = [
        {"LastModified": older},
        {"LastModified": newer},
    ]

    result = CliRunner().invoke(
        cli_module.cli,
        [
            "get-newest-backup-file",
            "--input-file-path",
            str(backup_csv),
            "--s3-prefix",
            prefix,
        ],
    )

    assert result.exit_code == 0, result.output
    cli_module.boto3.client.assert_called_once_with("s3")
    assert s3_client.head_object.call_args_list == [
        call(Bucket="bucket", Key="backups/source_folder.tar.gz.gpg"),
        call(Bucket="bucket", Key="backups/custom.tar.gz.gpg"),
    ]
    assert (
        f"s3://bucket/backups/source_folder.tar.gz.gpg\t{older.astimezone().isoformat(sep=' ', timespec='seconds')}"
        in result.output
    )
    assert (
        f"Newest: s3://bucket/backups/custom.tar.gz.gpg\t{newer.astimezone().isoformat(sep=' ', timespec='seconds')}"
        in result.output
    )
    assert "days ago)" in result.output
    assert "\x1b[" not in result.output


@pytest.fixture
def local_timezone(monkeypatch):
    if not hasattr(time, "tzset"):
        pytest.skip("Setting the process timezone requires time.tzset")
    try:
        with monkeypatch.context() as environment:
            environment.setenv("TZ", "EST5EDT,M3.2.0,M11.1.0")
            time.tzset()
            yield
    finally:
        time.tzset()


@pytest.mark.parametrize(
    ("updated_at", "now", "expected"),
    [
        (
            datetime.datetime(2026, 1, 3, 4, tzinfo=datetime.timezone.utc),
            datetime.datetime(2026, 1, 3, 5, 30, tzinfo=datetime.timezone.utc),
            "2026-01-02 23:00:00-05:00 (1 day ago)",
        ),
        (
            datetime.datetime(2026, 1, 3, 5, tzinfo=datetime.timezone.utc),
            datetime.datetime(2026, 1, 3, 5, 30, tzinfo=datetime.timezone.utc),
            "2026-01-03 00:00:00-05:00 (0 days ago)",
        ),
        (
            datetime.datetime(2026, 7, 1, 4, tzinfo=datetime.timezone.utc),
            datetime.datetime(2026, 7, 3, 4, 30, tzinfo=datetime.timezone.utc),
            "2026-07-01 00:00:00-04:00 (2 days ago)",
        ),
    ],
)
def test_get_newest_backup_file_reports_local_calendar_age(
    backup_csv, s3_client, local_timezone, monkeypatch, updated_at, now, expected
):
    clock = Mock(wraps=datetime)
    clock.datetime = Mock(wraps=datetime.datetime)
    clock.datetime.now.return_value = now
    monkeypatch.setattr(cli_module, "datetime", clock)
    s3_client.head_object.return_value = {"LastModified": updated_at}

    result = CliRunner().invoke(
        cli_module.cli,
        [
            "get-newest-backup-file",
            "--input-file-path",
            str(backup_csv),
            "--s3-prefix",
            "s3://bucket",
        ],
    )
    assert result.exit_code == 0, result.output
    assert result.output.splitlines() == [
        f"s3://bucket/source_folder.tar.gz.gpg\t{expected}",
        f"s3://bucket/custom.tar.gz.gpg\t{expected}",
        f"Newest: s3://bucket/source_folder.tar.gz.gpg\t{expected}",
    ]


def test_get_newest_backup_file_colors_log_output(backup_csv, s3_client):
    s3_client.head_object.side_effect = [
        s3_client.head_object.return_value,
        ClientError({"Error": {"Code": "404", "Message": "Missing"}}, "HeadObject"),
    ]
    result = CliRunner().invoke(
        cli_module.cli,
        [
            "get-newest-backup-file",
            "--input-file-path",
            str(backup_csv),
            "--s3-prefix",
            "s3://bucket",
        ],
        color=True,
    )
    assert result.exit_code == 0, result.output
    assert "\x1b[36ms3://bucket/source_folder.tar.gz.gpg" in result.stderr
    assert "\x1b[33ms3://bucket/custom.tar.gz.gpg\tMISSING" in result.stderr
    assert "\x1b[32mNewest:" in result.stderr


@pytest.mark.parametrize(
    ("file_path", "output_name", "expected_key"),
    [
        ("/input/source folder", "", "source_folder.tar.gz.gpg"),
        ("/input/source", "nested/custom", "custom.tar.gz.gpg"),
        ("/input/archive file.gz", "ignored", "archive file.gz.gpg"),
        ("/input/archive.bz2", "", "archive.bz2.gpg"),
    ],
)
def test_get_newest_backup_file_matches_encrypted_names(
    backup_csv, s3_client, file_path, output_name, expected_key
):
    backup_csv.write_text(
        f"file_path,upload_time,output_file_path\n{file_path},ONCE,{output_name}\n"
    )
    result = CliRunner().invoke(
        cli_module.cli,
        [
            "get-newest-backup-file",
            "--input-file-path",
            str(backup_csv),
            "--s3-prefix",
            "s3://bucket",
        ],
    )
    assert result.exit_code == 0, result.output
    s3_client.head_object.assert_called_once_with(Bucket="bucket", Key=expected_key)
    assert sorted(path.name for path in backup_csv.parent.iterdir()) == ["backups.csv"]


def test_get_newest_backup_file_uses_named_profile(backup_csv, s3_client):
    result = CliRunner().invoke(
        cli_module.cli,
        [
            "get-newest-backup-file",
            "--input-file-path",
            str(backup_csv),
            "--s3-prefix",
            "s3://bucket",
            "--assume-role",
            "backup-profile",
        ],
    )
    assert result.exit_code == 0, result.output
    cli_module.boto3.Session.assert_called_once_with(profile_name="backup-profile")
    cli_module.boto3.Session.return_value.client.assert_called_once_with("s3")
    cli_module.boto3.client.assert_not_called()
    assert s3_client.head_object.call_count == 2


@pytest.mark.parametrize("code", ["404", "NoSuchKey", "NotFound"])
@pytest.mark.parametrize("all_missing", [False, True])
def test_get_newest_backup_file_reports_missing_objects(
    backup_csv, s3_client, code, all_missing
):
    missing = ClientError({"Error": {"Code": code, "Message": "Missing"}}, "HeadObject")
    s3_client.head_object.side_effect = [
        missing,
        missing if all_missing else s3_client.head_object.return_value,
    ]
    result = CliRunner().invoke(
        cli_module.cli,
        [
            "get-newest-backup-file",
            "--input-file-path",
            str(backup_csv),
            "--s3-prefix",
            "s3://bucket",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "s3://bucket/source_folder.tar.gz.gpg\tMISSING" in result.output
    assert s3_client.head_object.call_count == 2
    if all_missing:
        assert "No backup objects found in S3." in result.output
        assert "Newest:" not in result.output
    else:
        assert "Newest: s3://bucket/custom.tar.gz.gpg" in result.output


@pytest.mark.parametrize("assume_role", [False, True])
@pytest.mark.parametrize(
    "error",
    [
        NoCredentialsError(),
        ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Not allowed"}}, "AWSRequest"
        ),
    ],
)
def test_get_newest_backup_file_surfaces_aws_errors(
    backup_csv, s3_client, assume_role, error
):
    s3_client.head_object.side_effect = error
    options = ["--assume-role", "backup-profile"] if assume_role else []
    result = CliRunner().invoke(
        cli_module.cli,
        [
            "get-newest-backup-file",
            "--input-file-path",
            str(backup_csv),
            "--s3-prefix",
            "s3://bucket",
            *options,
        ],
    )
    assert result.exit_code == 1
    assert f"Error: {error}" in result.output
    assert "MISSING" not in result.output


def test_get_newest_backup_file_reports_missing_profile(backup_csv, s3_client):
    error = ProfileNotFound(profile="missing-profile")
    cli_module.boto3.Session.side_effect = error
    result = CliRunner().invoke(
        cli_module.cli,
        [
            "get-newest-backup-file",
            "--input-file-path",
            str(backup_csv),
            "--s3-prefix",
            "s3://bucket",
            "--assume-role",
            "missing-profile",
        ],
    )
    assert result.exit_code == 1
    assert f"Error: {error}" in result.output
    cli_module.boto3.client.assert_not_called()
    s3_client.head_object.assert_not_called()


@pytest.mark.parametrize(
    "prefix",
    [
        "bucket",
        "https://bucket/path",
        "s3:///path",
        "s3://bucket/path?query=1",
        "s3://bucket/path#fragment",
        "s3://user@bucket",
        "s3://bucket:443",
    ],
)
def test_get_newest_backup_file_rejects_invalid_prefix(backup_csv, s3_client, prefix):
    result = CliRunner().invoke(
        cli_module.cli,
        [
            "get-newest-backup-file",
            "--input-file-path",
            str(backup_csv),
            "--s3-prefix",
            prefix,
        ],
    )
    assert result.exit_code == 1
    assert "--s3-prefix must be" in result.output
    cli_module.boto3.client.assert_not_called()


@pytest.mark.parametrize(
    ("contents", "message"),
    [
        ("file_path\n/input/source\n", "CSV requires"),
        ("file_path,upload_time\n,ONCE\n", "missing file_path"),
        ("file_path,upload_time\n/input/source\n", "missing file_path or upload_time"),
        ("file_path,upload_time\n/input/source,INVALID\n", "unsupported upload_time"),
    ],
)
def test_get_newest_backup_file_rejects_invalid_csv(
    backup_csv, s3_client, contents, message
):
    backup_csv.write_text(contents)
    result = CliRunner().invoke(
        cli_module.cli,
        [
            "get-newest-backup-file",
            "--input-file-path",
            str(backup_csv),
            "--s3-prefix",
            "s3://bucket",
        ],
    )
    assert result.exit_code == 1
    assert message in result.output
    cli_module.boto3.client.assert_not_called()


def test_get_newest_backup_file_handles_empty_csv(backup_csv, s3_client):
    backup_csv.write_text("file_path,upload_time\n")
    result = CliRunner().invoke(
        cli_module.cli,
        [
            "get-newest-backup-file",
            "--input-file-path",
            str(backup_csv),
            "--s3-prefix",
            "s3://bucket",
        ],
    )
    assert result.exit_code == 0
    assert "No backup files in CSV." in result.output
    cli_module.boto3.client.assert_not_called()
