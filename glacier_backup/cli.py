import csv
import dataclasses
import datetime
import logging
import os
import posixpath
from urllib.parse import urlsplit

import boto3
import click
from botocore.exceptions import BotoCoreError, ClientError

from glacier_backup.backup_runner import BackupRunner
from glacier_backup.csv_input_row import CsvInputRow
from glacier_backup.file_data import FileData, UPLOAD_TIME_EVERY_BACKUP
from glacier_backup.gpg_util import GpgUtil, KEYSERVER
from glacier_backup.sns_notification_adapter import (
    NoNotificationAdapter,
    SnsNotificationAdapter,
)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s.%(funcName)s %(message)s",
)
logger = logging.getLogger(__name__)


class _ColoredOutputHandler(logging.Handler):
    def emit(self, record):
        color = getattr(record, "color", None)
        if color is None:
            color = "yellow" if record.levelno >= logging.WARNING else "cyan"
        click.secho(self.format(record), fg=color, err=True)


backup_status_logger = logging.Logger(f"{__name__}.backup_status", level=logging.INFO)
backup_status_logger.addHandler(_ColoredOutputHandler())
backup_status_logger.propagate = False


def _format_backup_timestamp(updated_at, today):
    local_time = updated_at.astimezone()
    days_ago = (today - local_time.date()).days
    unit = "day" if days_ago == 1 else "days"
    return (
        f"{local_time.isoformat(sep=' ', timespec='seconds')} ({days_ago} {unit} ago)"
    )


@click.group()
def cli():
    pass


@cli.command()
@click.argument("key_id")
@click.option(
    "--keyserver",
    default=KEYSERVER,
    show_default=True,
    help="Keyserver to publish the updated key to",
)
def update_gpg_expiry(key_id, keyserver):
    try:
        GpgUtil.update_key_expiry(key_id, keyserver)
    except (ValueError, RuntimeError) as error:
        raise click.ClickException(str(error)) from error

    click.echo(f"Updated expiry to 1 year and uploaded {key_id} to {keyserver}.")


@cli.command()
@click.option(
    "--immich-file-root",
    type=str,
    required=True,
    help="Root of immich file installation",
)
@click.option(
    "--output-file-path",
    type=str,
    required=True,
    help="Path to write the output file",
)
@click.option(
    "--full-backup",
    is_flag=True,
    help="Do a full backup (vs current year only)",
)
def list_immich(immich_file_root, output_file_path, full_backup):
    logger.info(
        f"Starting list_immich: immich_file_root={immich_file_root} "
        f"output_file_path={output_file_path} full_backup={full_backup}"
    )
    # thumbs -> complete directory every time
    # upload -> complete directory every time
    # library/1e958228-47fc-463d-83c7-0bc485a8cbfa/2024
    output_list: list[CsvInputRow] = [
        CsvInputRow(
            os.path.join(immich_file_root, "photos", "thumbs"),
            UPLOAD_TIME_EVERY_BACKUP,
            output_file_path=None,
        ),
        CsvInputRow(
            os.path.join(immich_file_root, "photos", "upload"),
            UPLOAD_TIME_EVERY_BACKUP,
            output_file_path=None,
        ),
    ]

    current_year = str(datetime.datetime.now().year)

    photos_file_path = os.path.join(immich_file_root, "photos", "library")
    for user in os.listdir(photos_file_path):
        user_file_path = os.path.join(photos_file_path, user)

        if not os.path.isdir(user_file_path):
            logger.warning(f"Skipping non-directory path: {user_file_path}")
            continue

        logger.info(f"Processing user={user} path={user_file_path}")

        user_file_path = os.path.join(photos_file_path, user)
        for year in os.listdir(user_file_path):
            year_file_path = os.path.join(user_file_path, year)

            archive_output_file_name = f"{user}__{year}"

            if full_backup:
                logger.info(
                    f"Adding year for full backup: user={user} year={year} path={year_file_path}"
                )
                output_list.append(
                    CsvInputRow(
                        year_file_path,
                        UPLOAD_TIME_EVERY_BACKUP,
                        archive_output_file_name,
                        listing_file_name=f"{archive_output_file_name}.gz",
                    )
                )
                continue

            if year == current_year:
                output_list.append(
                    CsvInputRow(
                        year_file_path,
                        UPLOAD_TIME_EVERY_BACKUP,
                        archive_output_file_name,
                        listing_file_name=f"{archive_output_file_name}.gz",
                    )
                )
                logger.info(
                    f"Adding current year for backup: user={user} year={year} path={year_file_path}"
                )
                continue
            output_list.append(
                CsvInputRow(
                    year_file_path,
                    UPLOAD_TIME_EVERY_BACKUP,
                    archive_output_file_name,
                    listing_file_name=f"{archive_output_file_name}.gz",
                )
            )

    with open(output_file_path, "w") as f:
        writer = csv.writer(f, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(
            ["file_path", "upload_time", "output_file_path", "listing_file_name"]
        )
        for r in output_list:
            writer.writerow(dataclasses.astuple(r))
    logger.info(
        f"Finished list_immich: wrote {len(output_list)} rows to {output_file_path}"
    )


@cli.command()
@click.option("--gpg-key-id", help="Fingerprint of the key to use")
@click.option("--input-file-path", help="Local file containing directory list")
@click.option("--temp-dir", help="dir to use for scratch space")
@click.option("--sns-notification-arn", help="send SNS notifications to this ARN")
@click.option(
    "--assume-role-arn", help="ARN of the role to assume for SNS notifications"
)
def create_archives(
    gpg_key_id, input_file_path, temp_dir, sns_notification_arn, assume_role_arn
):
    logger.info(
        f"Starting create_archives: input_file_path={input_file_path} "
        f"temp_dir={temp_dir} sns_notification_enabled={sns_notification_arn is not None}"
    )
    if not os.path.exists(temp_dir):
        logger.error(f"Temp dir does not exist: {temp_dir}")
        raise ValueError("temp dir does not exist, create before running")

    notifier = NoNotificationAdapter()
    if sns_notification_arn is not None:
        logger.info(f"Using SNS notifier: topic_arn={sns_notification_arn}")
        notifier = SnsNotificationAdapter(
            topic_arn=sns_notification_arn, assume_role_arn=assume_role_arn
        )

    backup_runner = BackupRunner(temp_dir=temp_dir, notifier=notifier)

    try:
        backup_runner.run(input_file_path, gpg_key_id)
    finally:
        logger.info("Sending final notification for backup runner completion")
        notifier.send_notification("Finished backup runner")


@cli.command()
@click.option(
    "--input-file-path",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
    help="Local CSV containing the backup directory list",
)
@click.option(
    "--s3-prefix", required=True, help="S3 destination, such as s3://bucket/backups/"
)
@click.option("--assume-role", help="Name of the AWS profile to use for S3 lookups")
def get_newest_backup_file(input_file_path, s3_prefix, assume_role):
    try:
        destination = urlsplit(s3_prefix)
        if (
            destination.scheme != "s3"
            or not destination.netloc
            or "@" in destination.netloc
            or ":" in destination.netloc
            or destination.query
            or destination.fragment
        ):
            raise ValueError("--s3-prefix must be s3://bucket or s3://bucket/prefix/")

        prefix = destination.path.lstrip("/").rstrip("/")
        objects = []
        with open(input_file_path, newline="") as input_file:
            reader = csv.DictReader(input_file)
            if not {"file_path", "upload_time"}.issubset(reader.fieldnames or []):
                raise ValueError("CSV requires file_path and upload_time columns")
            for row in reader:
                file_path = row["file_path"]
                upload_time = row["upload_time"]
                if not file_path or not upload_time:
                    raise ValueError(
                        f"CSV row {reader.line_num}: missing file_path or upload_time"
                    )
                if upload_time.upper() not in FileData.SUPPORTED_UPLOAD_TIMES:
                    raise ValueError(
                        f"CSV row {reader.line_num}: unsupported upload_time {upload_time!r}"
                    )

                if file_path.endswith("bz2") or file_path.endswith("gz"):
                    archive_name = posixpath.basename(file_path)
                else:
                    archive_name = posixpath.basename(
                        FileData.get_compressed_file_name(
                            file_path, row.get("output_file_path")
                        )
                    )
                key = f"{archive_name}.gpg"
                if prefix:
                    key = f"{prefix}/{key}"
                objects.append(key)

        if not objects:
            backup_status_logger.warning("No backup files in CSV.")
            return

        if assume_role:
            s3_client = boto3.Session(profile_name=assume_role).client("s3")
        else:
            s3_client = boto3.client("s3")

        today = datetime.datetime.now().astimezone().date()
        newest = None
        for key in objects:
            uri = f"s3://{destination.netloc}/{key}"
            try:
                metadata = s3_client.head_object(Bucket=destination.netloc, Key=key)
            except ClientError as error:
                if error.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                    backup_status_logger.warning("%s\tMISSING", uri)
                    continue
                raise
            updated_at = metadata["LastModified"]
            backup_status_logger.info(
                "%s\t%s", uri, _format_backup_timestamp(updated_at, today)
            )
            if newest is None or updated_at > newest[1]:
                newest = (uri, updated_at)

        if newest is None:
            backup_status_logger.warning("No backup objects found in S3.")
        else:
            backup_status_logger.info(
                "Newest: %s\t%s",
                newest[0],
                _format_backup_timestamp(newest[1], today),
                extra={"color": "green"},
            )
    except (BotoCoreError, ClientError, OSError, csv.Error, ValueError) as error:
        raise click.ClickException(str(error)) from error


if __name__ == "__main__":
    cli()
