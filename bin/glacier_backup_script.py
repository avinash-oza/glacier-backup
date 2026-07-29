import csv
import dataclasses
import datetime
import logging
import os

import click

from glacier_backup.backup_runner import BackupRunner
from glacier_backup.file_data import UPLOAD_TIME_EVERY_BACKUP
from glacier_backup.csv_input_row import CsvInputRow
from glacier_backup.sns_notification_adapter import (
    NoNotificationAdapter,
    SnsNotificationAdapter,
)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s.%(funcName)s %(message)s",
)
logger = logging.getLogger(__name__)


@click.group()
def cli():
    pass


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
        writer.writerow(["file_path", "upload_time", "output_file_path", "listing_file_name"])
        for r in output_list:
            writer.writerow(dataclasses.astuple(r))
    logger.info(f"Finished list_immich: wrote {len(output_list)} rows to {output_file_path}")


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


if __name__ == "__main__":
    cli()
