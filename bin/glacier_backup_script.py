import csv
import dataclasses
import datetime
import logging
import os

import click

from glacier_backup.backup_runner import BackupRunner
from glacier_backup.file_data import UPLOAD_TIME_EVERY_BACKUP
from glacier_backup.structures import CsvInputRow

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
    "--full",
    is_flag=True,
    help="Do a full backup (vs current year only)",
)
def list_immich(immich_file_root, output_file_path, full_backup):
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
            logger.warning(f"Skipping {user_file_path=}")
            continue

        logger.info("Username: %s", user)

        user_file_path = os.path.join(photos_file_path, user)
        for year in os.listdir(user_file_path):
            year_file_path = os.path.join(user_file_path, year)

            archive_output_file_name = f"{user}__{year}"

            if full_backup:
                output_list.append(
                    CsvInputRow(
                        year_file_path,
                        UPLOAD_TIME_EVERY_BACKUP,
                        archive_output_file_name,
                    )
                )
                continue

            if year == current_year:
                output_list.append(
                    CsvInputRow(
                        year_file_path,
                        UPLOAD_TIME_EVERY_BACKUP,
                        archive_output_file_name,
                    )
                )
                logger.info("Setting current year to glacier")
                continue
            output_list.append(
                CsvInputRow(
                    year_file_path, UPLOAD_TIME_EVERY_BACKUP, archive_output_file_name
                )
            )

    with open(output_file_path, "w") as f:
        writer = csv.writer(f, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["file_path", "upload_time", "output_file_path"])
        for r in output_list:
            writer.writerow(dataclasses.astuple(r))


@cli.command()
@click.option("--gpg-key-id", help="Fingerprint of the key to use")
@click.option("--input-file-path", help="Local file containing directory list")
@click.option("--temp-dir", help="dir to use for scratch space")
def create_archives(gpg_key_id, input_file_path, temp_dir):
    if not os.path.exists(temp_dir):
        raise ValueError(f"temp dir does not exist, create before running")

    backup_runner = BackupRunner(temp_dir=temp_dir)

    backup_runner.run(input_file_path, gpg_key_id)


if __name__ == "__main__":

    cli()
